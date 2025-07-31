-- DROP FUNCTION public.find_parentobjids_by_parsed(text, text, int4);

CREATE OR REPLACE FUNCTION public.find_parentobjids_by_parsed(street_type text, norm_name_val text, town_objid integer DEFAULT NULL::integer)
 RETURNS integer[]
 LANGUAGE plpgsql
AS $function$
DECLARE
    result_ids INT[];
    tokens     text[];
    w          text;
    sql        text;
    -- накопим фильтры по «словам»
    where_cond text := '';
    -- для цифр
    num_clean  text := NULL;
    num_pattern text := NULL;
BEGIN
    -- проверка родителя
    IF town_objid IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM public.fias_objects WHERE objectid = town_objid
    ) THEN
        RAISE NOTICE 'Parent objectid % not found', town_objid;
        RETURN '{}';
    END IF;

    -- разбиваем norm_name_val
    tokens := regexp_split_to_array(coalesce(norm_name_val, ''), '\s+');
    FOREACH w IN ARRAY tokens LOOP
        IF w ~ '\d' THEN
            num_clean   := regexp_replace(w, '\D', '', 'g');
            num_pattern := format('(?:^| )%s[^ ]*(?: |$)', num_clean);
        ELSE
            -- для «Большой», «Малый» и т.д. добавляем вариант «Б.»
            IF w IN ('Большой','Малый') THEN
                where_cond := where_cond
                  || ' AND (lower(f.norm_name) LIKE ' 
                  || quote_literal('%' || lower(w) || '%')
                  || ' OR lower(f.norm_name) LIKE '
                  || quote_literal('%' || lower(left(w,1) || '.') || '%')
                  || ')';
            ELSE
                where_cond := where_cond
                  || ' AND lower(f.norm_name) LIKE '
                  || quote_literal('%' || lower(w) || '%');
            END IF;
        END IF;
    END LOOP;

    -- строим основной текст запроса
    sql := 'SELECT array_agg(f.objectid) FROM public.fias_objects f WHERE 1=1';

    -- добавляем условия по street_type и town_objid
    IF street_type IS NOT NULL THEN
        sql := sql || ' AND f.typename = ' || quote_literal(street_type);
    END IF;
    IF town_objid IS NOT NULL THEN
        sql := sql || ' AND f.parent_objectid = ' || town_objid;
    END IF;

    -- условие по цифрам
    IF num_clean IS NOT NULL THEN
        sql := sql || ' AND f.norm_name ~* ' || quote_literal(num_pattern);
    END IF;

    -- прицепляем наш where_cond для «слов»
    sql := sql || where_cond;

    -- выполняем
    EXECUTE sql INTO result_ids;

    RETURN COALESCE(result_ids, '{}');
END;
$function$
;
