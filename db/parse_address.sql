-- DROP FUNCTION public.parse_address(text);

CREATE OR REPLACE FUNCTION public.parse_address(street_raw text)
 RETURNS TABLE(street_type text, norm_name text, house_part text)
 LANGUAGE plpgsql
AS $function$
DECLARE
  segments     TEXT[];
  seg          TEXT;
  clean        TEXT;
  words        TEXT[];
  w            TEXT;
  base_w       TEXT;
  tpl          RECORD;
  found_kind   TEXT;
  alias_item   TEXT;
  idx          INT;
  found_index  INT;
BEGIN
  -- 1. Нормализация: только приведение 'ё'→'е', сворачивание точек и обрезка пробелов
  clean := street_raw;
  clean := regexp_replace(clean, '[\u0451]', 'е', 'g');
  clean := regexp_replace(clean, '\.+', '.', 'g');
  clean := btrim(clean);

  -- 2. Разбиение по запятым
  segments := regexp_split_to_array(clean, '\s*,\s*');

  -- 3. Поиск сегмента с street_type
  found_index := 0;
  FOR idx IN array_lower(segments,1)..array_upper(segments,1) LOOP
    seg := segments[idx];
    words := regexp_split_to_array(seg, '\s+');
    FOREACH w IN ARRAY words LOOP
      base_w := regexp_replace(w, '\.$', '', 'g');

      FOR tpl IN
        SELECT l.name AS kind, a AS alias
        FROM public.lookup_types l
             CROSS JOIN unnest(l.aliases) AS a
        WHERE l.category = 'street_type'
      LOOP
        IF base_w ~ tpl.alias     -- строгое, case‑sensitive регулярное сравнение
           OR tpl.kind = base_w    -- строгое совпадение имени типа
        THEN
          found_kind  := tpl.kind;
          clean       := seg;
          found_index := idx;
          EXIT;
        END IF;
      END LOOP;

      EXIT WHEN found_kind IS NOT NULL;
    END LOOP;
    EXIT WHEN found_kind IS NOT NULL;
  END LOOP;

  -- 4. Если не нашли — выходим
  IF found_kind IS NULL THEN
    RETURN;
  END IF;

  -- 5. Убираем все алиасы и имя типа из найденного сегмента
  FOR alias_item IN
    SELECT unnest(aliases)
    FROM public.lookup_types
    WHERE category = 'street_type'
      AND name = found_kind
  LOOP
    clean := regexp_replace(
      clean,
      format('(^|\s)%s\.?($|\s)', alias_item),
      ' ', 'g'
    );
  END LOOP;
  clean := regexp_replace(
    clean,
    format('(^|\s)%s($|\s)', found_kind),
    ' ', 'g'
  );
  clean := btrim(clean);

  -- 6. Определяем house_part (следующий за сегментом)
  IF found_index > 0 AND found_index < array_length(segments,1) THEN
    house_part := segments[found_index + 1];
  ELSE
    house_part := NULL;
  END IF;

  -- 7. Возврат результата
  street_type := found_kind;
  norm_name   := clean;
  RETURN NEXT;
END;
$function$
;
