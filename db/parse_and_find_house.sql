-- DROP FUNCTION public.parse_and_find_house(_int4, text);

CREATE OR REPLACE FUNCTION public.parse_and_find_house(parentobjids integer[], hs_raw text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- raw house string, приводим к нижнему регистру и убираем пробелы по краям
    hs              TEXT := lower(trim(hs_raw));
    -- номер дома (или доп.номера) после очистки
    prefix          TEXT;
    rest            TEXT;
    result_id       INT;
    tmp             RECORD;
    -- Тип дома (housetype)
    ht_name         TEXT;
    ht_id           INT;
    -- Первый и второй уровни добавочного типа
    a1_id           INT; n1 TEXT;
    a2_id           INT; n2 TEXT;
BEGIN
    -- 0) Чистка от "мусора": берём только последний токен после пробела
    hs := regexp_replace(hs, '^.*\s+', '');

    -- 1) Определяем тип дома (housetype) по префиксу или дефолту 'д'
    SELECT name, id
      INTO ht_name, ht_id
      FROM lookup_types
     WHERE category = 'housetype' AND strpos(hs, name) = 1
     ORDER BY length(name) DESC
     LIMIT 1;

    IF FOUND THEN
        prefix := substr(hs, length(ht_name) + 1);
    ELSE
        SELECT id INTO ht_id
          FROM lookup_types
         WHERE category = 'housetype' AND name = 'д'
         LIMIT 1;
        ht_name := 'д';
        prefix  := regexp_replace(hs, E'^д\.?\s*', '');
    END IF;

    rest := prefix;

    -- 2) Извлекаем первый маркер addtype и номер после него
    SELECT name, id, strpos(rest, name) AS pos
      INTO tmp
      FROM lookup_types
     WHERE category = 'addtype' AND strpos(rest, name) > 0
     ORDER BY pos
     LIMIT 1;

    IF FOUND THEN
        prefix := substr(rest, 1, tmp.pos - 1);
        rest   := substr(rest, tmp.pos);
        a1_id  := tmp.id;
        n1     := substring(rest from '^' || tmp.name || '([0-9]+)');
        rest   := substr(rest, length(tmp.name || coalesce(n1,'')) + 1);
    ELSE
        prefix := rest;
        rest   := '';
    END IF;

    -- 3) Извлекаем второй addtype, если есть
    IF rest <> '' THEN
        SELECT name, id
          INTO tmp.name, tmp.id
          FROM lookup_types
         WHERE category = 'addtype' AND strpos(rest, name) = 1
         ORDER BY length(name) DESC
         LIMIT 1;

        IF FOUND THEN
            a2_id := tmp.id;
            n2    := substring(rest from '^' || tmp.name || '([0-9]+)');
        END IF;
    END IF;

    -- 4) Основной поиск по точному совпадению
    SELECT id
      INTO result_id
      FROM public.fias_houses
     WHERE parentobjid = ANY(parentobjids)
       AND upper(housenum) = upper(prefix)
       AND (addtype1 = a1_id OR (a1_id IS NULL AND addtype1 IS NULL))
       AND coalesce(addnum1::text, '') = coalesce(n1, '')
       AND (addtype2 = a2_id OR (a2_id IS NULL AND addtype2 IS NULL))
       AND coalesce(addnum2::text, '') = coalesce(n2, '')
     LIMIT 1;

    -- 5) Фолбэки: по номеру, затем удаляем буквы
    IF result_id IS NULL THEN
        SELECT id INTO result_id
          FROM public.fias_houses
         WHERE parentobjid = ANY(parentobjids)
           AND upper(housenum) = upper(prefix)
         LIMIT 1;
    END IF;

    IF result_id IS NULL THEN
        prefix := regexp_replace(prefix, '([0-9/]+)[а-яa-z]+$', '\1');
        SELECT id INTO result_id
          FROM public.fias_houses
         WHERE parentobjid = ANY(parentobjids)
           AND upper(housenum) = upper(prefix)
         LIMIT 1;
    END IF;

    -- 6) Вставляем новый дом, только если номер дома содержит цифры и не пустой
    IF result_id IS NULL
       AND prefix IS NOT NULL
       AND prefix <> ''
       AND prefix ~ '[0-9]' THEN
        INSERT INTO public.fias_houses (
            housenum, housetype, addnum1, addtype1, addnum2, addtype2, parentobjid
        ) VALUES (
            prefix, ht_id, n1, a1_id, n2, a2_id, parentobjids[1]
        ) RETURNING id INTO result_id;
    END IF;

    RETURN result_id;
END;
$function$
;
