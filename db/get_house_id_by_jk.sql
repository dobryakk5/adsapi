-- DROP FUNCTION public.get_house_id_by_jk(text, text);

CREATE OR REPLACE FUNCTION public.get_house_id_by_jk(p_jk_name text, p_full_address text)
 RETURNS smallint
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_complex_id  SMALLINT;
    v_jk_preclean TEXT;
    v_jk_clean    TEXT;
    v_words       TEXT[];
    v_n           INT;
    v_ids         SMALLINT[];
    v_house_id    SMALLINT;
    v_corp_raw    TEXT;
    v_corp_clean  TEXT;
    v_parts       TEXT[];
BEGIN
    -- 0. Проверка входных параметров
    IF p_jk_name IS NULL OR btrim(p_jk_name) = '' THEN
        RETURN NULL;
    END IF;

    -------------------------------------------------------------
    -- 1. Прямой поиск по title_cian
    SELECT id
      INTO v_complex_id
    FROM complexes cmp
    WHERE cmp.title_cian ILIKE p_jk_name
    LIMIT 1;

    -- 1.1. Прямой поиск по title_lat (с учетом оригинальных символов)
    IF v_complex_id IS NULL THEN
        SELECT id
          INTO v_complex_id
        FROM complexes cmp
        WHERE cmp.title_lat ILIKE p_jk_name
        LIMIT 1;
    END IF;

    -------------------------------------------------------------
    -- 2. Предварительная очистка от "шума" (сохранить все буквы и пробелы Unicode)
    v_jk_preclean := regexp_replace(
        p_jk_name,
        '[^[:alpha:][:space:]]',  -- все, кроме букв и пробелов
        ' ',
        'g'
    );
    v_jk_preclean := regexp_replace(v_jk_preclean, '[[:space:]]+', ' ', 'g');
    v_jk_preclean := btrim(
        regexp_replace(v_jk_preclean, '^[[:space:]]*(жк)[[:space:]]*', '', 'gi')
    );

    -- CamelCase-фоллбэк: UltimaCity → Ultima City
    IF v_complex_id IS NULL
       AND v_jk_preclean ~ '^[A-ZА-Я][a-zа-я]+([A-ZА-Я][a-zа-я]+)+$'
    THEN
        v_jk_preclean := regexp_replace(
            v_jk_preclean,
            '([a-zа-я])([A-ZА-Я])',
            E'\\1 \\2',
            'g'
        );
    END IF;

    -- Переводим в нижний регистр для последующих шагов
    v_jk_clean := lower(v_jk_preclean);

    -- Удаляем описательные слова ЖК до поиска по title
    v_jk_clean := regexp_replace(
        v_jk_clean,
        '(апарт-?комплекс|жилой-?комплекс|жк|жилой дом|комплекс апартаментов)',
        '',
        'gi'
    );
    v_jk_clean := btrim(regexp_replace(v_jk_clean, '[[:space:]]+', ' ', 'g'));

    -------------------------------------------------------------
    -- 3. Поиск по title с учётом unaccent для корректной сверки диакритиков и разных апострофов
    IF v_complex_id IS NULL THEN
        SELECT id
          INTO v_complex_id
        FROM complexes cmp
        WHERE unaccent(lower(cmp.title)) ILIKE '%' || unaccent(v_jk_clean) || '%'
        LIMIT 1;
    END IF;

    -------------------------------------------------------------
    -- 4. Поиск по последним 1..3 словам
    IF v_complex_id IS NULL THEN
        v_words := regexp_split_to_array(v_jk_clean, '[[:space:]]+');
        v_n := array_length(v_words, 1);
        FOR i IN 1 .. LEAST(3, v_n) LOOP
            SELECT array_agg(cmp.id) INTO v_ids
            FROM complexes cmp
            WHERE (
                SELECT bool_and(cmp.title ILIKE ('%' || w || '%'))
                FROM unnest(v_words[v_n-i+1 : v_n]) AS w
            );
            IF coalesce(array_length(v_ids, 1), 0) = 1 THEN
                v_complex_id := v_ids[1];
                EXIT;
            END IF;
        END LOOP;
    END IF;

    -------------------------------------------------------------
    -- 5. Поиск по первым 1..3 словам
    IF v_complex_id IS NULL THEN
        FOR i IN 1 .. LEAST(3, v_n) LOOP
            SELECT array_agg(cmp.id) INTO v_ids
            FROM complexes cmp
            WHERE (
                SELECT bool_and(cmp.title ILIKE ('%' || w || '%'))
                FROM unnest(v_words[1 : i]) AS w
            );
            IF coalesce(array_length(v_ids, 1), 0) = 1 THEN
                v_complex_id := v_ids[1];
                EXIT;
            END IF;
        END LOOP;
    END IF;

    -------------------------------------------------------------
    -- 6. Fuzzy-поиск (расширенный)
    IF v_complex_id IS NULL THEN
        WITH sims AS (
            SELECT
                id,
                greatest(
                    system.similarity(
                        lower(system.transliterate_to_ascii(unaccent(title_lat))),
                        lower(unaccent(p_jk_name))
                    ),
                    system.similarity(
                        lower(system.transliterate_to_ascii(unaccent(title_ascii))),
                        lower(unaccent(p_jk_name))
                    )
                ) AS sim
            FROM complexes
        )
        SELECT id
          INTO v_complex_id
        FROM sims
        WHERE sim > 0.4
        ORDER BY sim DESC
        LIMIT 1;
    END IF;

    -------------------------------------------------------------
    -- Если ЖК не найден — уведомление и выход
    IF v_complex_id IS NULL THEN
        RAISE NOTICE 'ЖК "%" не найден', p_jk_name;
        RETURN NULL;
    END IF;

    -------------------------------------------------------------
    -- 7. Разбор корпуса из полного адреса
    v_parts   := regexp_split_to_array(p_full_address, ',');
    v_corp_raw := v_parts[array_upper(v_parts, 1)];
    v_corp_clean := btrim(
        regexp_replace(
            v_corp_raw,
            '^[[:space:]]*(корпус|корп(?:\.|ус)?|к\.?|стр\.|з/у)[[:space:]]*',
            '',
            'i'
        )
    );

    -- 7.1. Точный поиск корпуса
    SELECT id
      INTO v_house_id
    FROM complex_houses ch
    WHERE ch.co_id = v_complex_id
      AND ch.corp ILIKE v_corp_clean
    LIMIT 1;
    IF FOUND THEN
        RETURN v_house_id;
    END IF;

    -- 7.2. Фоллбэк "без корпуса"
    SELECT id
      INTO v_house_id
    FROM complex_houses ch
    WHERE ch.co_id = v_complex_id
      AND ch.corp LIKE 'без к%'
    LIMIT 1;

    RETURN v_house_id;
END;
$function$
;
