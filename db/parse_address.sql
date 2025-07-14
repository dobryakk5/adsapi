-- Полная реализация parse_and_find_house в трех процедурах

-- Для определения id дома по составному имени с корпусами
CREATE OR REPLACE FUNCTION public.parse_and_find_house(
    parentobjids INT[],
    hs_raw        TEXT
) RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    hs       TEXT := lower(trim(hs_raw));
    prefix   TEXT;
    rest     TEXT;
    rem      TEXT;
    result_id INT;
    tmp      RECORD;
    cand     RECORD;
    -- Первый и второй уровни
    a1_id INT; n1 TEXT;
    a2_id INT; n2 TEXT;
BEGIN
    -- 1) Найти первый маркер из addtype (по позиции)
    SELECT name, id, strpos(hs,name) AS pos
      INTO tmp
      FROM lookup_types
     WHERE category = 'addtype' AND strpos(hs,name) > 0
     ORDER BY pos
     LIMIT 1;

    IF FOUND THEN
        prefix := substr(hs, 1, tmp.pos - 1);
        rest   := substr(hs, tmp.pos);
        -- извлечь первый уровень
        a1_id := tmp.id;
        n1    := substring(rest from '^' || tmp.name || '([0-9]+)');
        rem   := substr(rest, length(tmp.name || coalesce(n1,'')) + 1);
    ELSE
        prefix := hs;
        rem    := '';
    END IF;

    -- базовый fallback — первый дом по prefix
    SELECT objectid
      INTO result_id
      FROM public.fias_houses
     WHERE parentobjid = ANY(parentobjids)
       AND upper(housenum) = upper(prefix)
     LIMIT 1;

    -- Уровень 0: нет маркера вовсе
    IF rem = '' AND rest IS NULL THEN
        FOR cand IN
            SELECT objectid, addnum1, addnum2
              FROM public.fias_houses
             WHERE parentobjid = ANY(parentobjids)
               AND upper(housenum) = upper(prefix)
        LOOP
            IF cand.addnum1 IS NULL AND cand.addnum2 IS NULL THEN
                RETURN cand.objectid;
            END IF;
        END LOOP;
        RETURN result_id;
    END IF;

    -- Уровень 1: есть первый уровень, но больше ничего
    IF rem = '' THEN
        FOR cand IN
            SELECT objectid, addtype1, addnum1, addnum2
              FROM public.fias_houses
             WHERE parentobjid = ANY(parentobjids)
               AND upper(housenum) = upper(prefix)
        LOOP
            IF cand.addtype1 = a1_id
               AND coalesce(cand.addnum1,'') = coalesce(n1,'')
               AND cand.addnum2 IS NULL THEN
                RETURN cand.objectid;
            END IF;
        END LOOP;
        RETURN result_id;
    END IF;

    -- Уровень 2: извлечь второй уровень
    SELECT name, id
      INTO tmp.name, tmp.id
      FROM lookup_types
     WHERE category = 'addtype'
       AND strpos(rem, name) = 1
     ORDER BY length(name) DESC
     LIMIT 1;

    IF NOT FOUND THEN
        RETURN result_id;
    END IF;

    a2_id := tmp.id;
    n2    := substring(rem from '^' || tmp.name || '([0-9]+)');

    -- Ищем совпадение по двум уровням
    FOR cand IN
        SELECT objectid, addtype1, addnum1, addtype2, addnum2
          FROM public.fias_houses
         WHERE parentobjid = ANY(parentobjids)
           AND upper(housenum) = upper(prefix)
    LOOP
        IF cand.addtype1 = a1_id
           AND coalesce(cand.addnum1,'') = coalesce(n1,'')
           AND cand.addtype2 = a2_id
           AND coalesce(cand.addnum2,'') = coalesce(n2,'') THEN
            RETURN cand.objectid;
        END IF;
    END LOOP;

    -- Финальный fallback
    RETURN result_id;
END;
$$;

-- Функция 1: Разбор адреса на тип улицы и нормализованное имя ---------------------------
-- 1) Добавляем колонку aliases в справочную таблицу (если ещё не добавлена)
ALTER TABLE public.lookup_types
  ADD COLUMN IF NOT EXISTS aliases text[] NULL;

-- 2) Заполняем все типы улиц вручную пронумерованными id и массивами aliases
INSERT INTO public.lookup_types (id, category, "name", aliases) VALUES
  (1, 'street_type', 'ул',    ARRAY['\bул\b',       '\bулица\b']),
  (2, 'street_type', 'пр-кт', ARRAY['\bпросп\b',    '\bпр-кт\b']),
  (3, 'street_type', 'бул',   ARRAY['\bбульвар\b',  '\bбул\b']),
  (4, 'street_type', 'прзд',  ARRAY['\bпроезд\b']),
  (5, 'street_type', 'пос',   ARRAY['\bпос\b',       '\bпоселок\b']),
  (6, 'street_type', 'наб',   ARRAY['\bнаб\b',       '\bнабережная\b']),
  (7, 'street_type', 'пер',   ARRAY['\bпер\b',       '\bпереулок\b']),
  (8, 'street_type', 'ш',     ARRAY['\bшоссе\b',     '\bш\b'])
ON CONFLICT (id) DO NOTHING;


-- 3) Создаём функцию parse_address, которая:
--    • сначала нормализует строку,
--    • выделяет по запятым сегмент с точным словом‑типом (без лишних вхождений),
--    • разбивает сегмент на слова, убирает у слова точку, ищет exact‑match в lookup_types,
--    • удаляет найденное слово‑тип и всё после номера дома,
--    • возвращает street_type и norm_name.

CREATE OR REPLACE FUNCTION public.parse_address(street_raw text)
  RETURNS TABLE(street_type text, norm_name text)
  LANGUAGE plpgsql
AS $$
DECLARE
  segments    TEXT[];
  seg         TEXT;
  clean       TEXT;
  words       TEXT[];
  w           TEXT;
  base_w      TEXT;
  tpl         RECORD;
  found_kind  TEXT;
BEGIN
  -- 1. Нормализуем исходную строку
  clean := lower(street_raw);
  clean := regexp_replace(clean, '[\u0451]', 'е', 'gi');
  clean := regexp_replace(clean, '\.+', '.', 'g');
  clean := btrim(clean);

  -- 2. Разбиваем на сегменты по запятым
  segments := regexp_split_to_array(clean, '\s*,\s*');

  -- 3. Ищем сегмент, в котором встречается хоть одно слово‑тип
  FOREACH seg IN ARRAY segments LOOP
    -- разбиваем сегмент на слова
    words := regexp_split_to_array(seg, '\s+');
    FOREACH w IN ARRAY words LOOP
      -- удаляем конечную точку
      base_w := regexp_replace(w, '\.$', '', 'g');
      -- пробуем найти exact‑match в lookup_types
      SELECT name
        INTO found_kind
      FROM public.lookup_types
      WHERE category = 'street_type'
        AND lower(name) = base_w
      LIMIT 1;

      IF found_kind IS NOT NULL THEN
        clean := seg;    -- это тот сегмент, в котором нашли тип
        EXIT;            -- выходим из цикла слов
      END IF;
    END LOOP;

    IF found_kind IS NOT NULL THEN
      EXIT;              -- выходим из цикла сегментов
    END IF;
  END LOOP;

  -- 4. Если тип не найден в любом сегменте — возвращаем пустой тип и полный нормализованный текст
  IF found_kind IS NULL THEN
    street_type := NULL;
    norm_name   := clean;
    RETURN NEXT;
  END IF;

  -- 5. Удаляем слово‑тип из сегмента
  clean := regexp_replace(
    clean,
    -- гарантия границ слова и возможной точки
    format('\m%s\.?\M', found_kind),
    ' ',
    'gi'
  );
  clean := btrim(clean);

  -- 6. Отбрасываем номер дома и всё после (цифры и после)
  clean := split_part(clean, ',', 1);
  clean := regexp_replace(clean, '\d.*$', '', 'g');
  clean := btrim(clean);

  -- 7. Возвращаем результат
  street_type := found_kind;
  norm_name   := clean;
  RETURN NEXT;
END;
$$;


-- Функция 2: Поиск parentobjids по разобранному типу и имени. для определения id улиц по по нормализованному имени
CREATE OR REPLACE FUNCTION public.find_parentobjids_by_parsed(
    street_type TEXT,
    norm_name_val TEXT
) RETURNS INT[] LANGUAGE plpgsql AS $$
DECLARE
    result_ids INT[];
BEGIN
    IF street_type IS NOT NULL THEN
        SELECT array_agg(f.objectid) INTO result_ids
        FROM public.fias_objects f
        WHERE f.norm_name LIKE norm_name_val || '%' AND f.typename = street_type;
    ELSE
        SELECT array_agg(f.objectid) INTO result_ids
        FROM public.fias_objects f
        WHERE f.norm_name LIKE norm_name_val || '%';
    END IF;
    RAISE NOTICE 'Found IDs: %', COALESCE(result_ids::TEXT, '{}');
    RETURN COALESCE(result_ids, '{}');
END;
$$;
