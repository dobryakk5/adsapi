-- DROP FUNCTION public.parse_and_find_house(_int4, text);

CREATE OR REPLACE FUNCTION public.parse_and_find_house(parentobjids integer[], hs_raw text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- Убираем префикс 'д.' из строки дома и приводим к нижнему регистру
    hs TEXT := regexp_replace(lower(trim(hs_raw)), E'^д\\.?\\s*', '');
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

    -- Уровень 1: есть первый уровень, но больше ничего
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

    -- Уровень 2: извлечь второй уровень
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
$function$
;
