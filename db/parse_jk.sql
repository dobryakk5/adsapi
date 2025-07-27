-- DROP FUNCTION public.get_house_id_by_jk(text, text);

CREATE OR REPLACE FUNCTION public.get_house_id_by_jk(p_jk_name text, p_full_address text)
 RETURNS smallint
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_complex_id SMALLINT;
    v_jk_clean   TEXT;
    v_jk_ascii   TEXT;
    v_words      TEXT[];
    v_n          INT;
    v_ids        SMALLINT[];
    v_house_id   SMALLINT;
    v_corp_raw   TEXT;
    v_corp_clean TEXT;
    v_parts TEXT[];  -- Для разбивки адреса
BEGIN
    -- 0. Проверка входа
    IF p_jk_name IS NULL OR btrim(p_jk_name) = '' THEN
        RETURN NULL;
    END IF;

    -- 1. Очищаем ввод: оставляем только буквы и пробелы, удаляем дубли пробелов и префикс "жк"
    v_jk_clean := regexp_replace(p_jk_name, '[^[:alpha:]\s]', ' ', 'g');
    v_jk_clean := regexp_replace(v_jk_clean, '\s+', ' ', 'g');
    v_jk_clean := btrim(lower(regexp_replace(v_jk_clean, '^жк\s*', '', 'gi')));

    -- 1.1. Прямой поиск полной фразы
    SELECT id INTO v_complex_id
    FROM complexes cmp
    WHERE cmp.title ILIKE '%' || v_jk_clean || '%'
    LIMIT 1;

    -- 1.2. Если не найдено — уточняем по последним 1..3 словам
    IF v_complex_id IS NULL THEN
        v_words := regexp_split_to_array(v_jk_clean, '\s+');
        v_n := array_length(v_words, 1);
        FOR i IN 1..LEAST(3, v_n) LOOP
            SELECT array_agg(cmp.id) INTO v_ids
            FROM complexes cmp
            WHERE (
                SELECT bool_and(cmp.title ILIKE ('%' || w || '%'))
                FROM unnest(v_words[v_n - i + 1 : v_n]) AS w
            );
            IF coalesce(array_length(v_ids, 1), 0) = 1 THEN
                v_complex_id := v_ids[1];
                EXIT;
            END IF;
        END LOOP;
    END IF;

    -- 1.3. Если всё ещё не найдено — уточняем по первым 1..3 словам
    IF v_complex_id IS NULL THEN
        FOR i IN 1..LEAST(3, v_n) LOOP
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

    -- 1.4. CamelCase‑фоллбэк
    IF v_complex_id IS NULL AND p_jk_name ~ '^[A-ZА-Я][a-zа-я]+([A-ZА-Я][a-zа-я]+)+$' THEN
        v_jk_clean := lower(
            regexp_replace(
                p_jk_name,
                '([a-zа-я])([A-ZА-Я])',
                E'\\1 \\2',
                'g'
            )
        );
        SELECT id INTO v_complex_id
        FROM complexes cmp
        WHERE cmp.title ILIKE '%' || v_jk_clean || '%'
        LIMIT 1;
    END IF;

    -- 1.5. Fuzzy‑поиск по транслитерированному значению
    IF v_complex_id IS NULL THEN
        v_jk_ascii := system.transliterate_to_ascii(v_jk_clean);
        SELECT id
          INTO v_complex_id
        FROM complexes cmp
        WHERE system.similarity(cmp.title_lat, v_jk_ascii) > 0.5
        ORDER BY system.similarity(cmp.title_lat, v_jk_ascii) DESC
        LIMIT 1;
    END IF;

    -- Если ЖК не найден — уведомляем и возвращаем NULL
    IF v_complex_id IS NULL THEN
        RAISE NOTICE 'ЖК "%" не найден', v_jk_clean;
        RETURN NULL;
    END IF;

    -- 2. Поиск корпуса по p_full_address (новая логика)
    -- проверяем, что ЖК найден
    IF NOT FOUND THEN
        RAISE NOTICE 'ЖК с именем "%" не найден', p_jk_name;
        RETURN NULL;
    END IF;

    -- извлечение корпуса из полного адреса
    v_parts := regexp_split_to_array(p_full_address, ',');
  v_corp_raw := v_parts[array_upper(v_parts, 1)];

  -- убираем префиксы корп, корпус, к., стр., з/у
  v_corp_clean := regexp_replace(
    v_corp_raw,
    '^\s*(корпус|корп(?:\.|ус)?|к\.?|стр\.|з/у)\s*',
    '',
    'i'
  );
  v_corp_clean := btrim(v_corp_clean);

  -- поиск корпуса точным вхождением
    SELECT id
      INTO v_house_id
    FROM complex_houses AS ch
    WHERE ch.co_id = v_complex_id
      AND ch.corp ILIKE v_corp_clean
    LIMIT 1;

    IF FOUND THEN
        RETURN v_house_id;
    END IF;

    -- поиск корпуса "без корпуса"
    SELECT id
      INTO v_house_id
    FROM complex_houses AS ch
    WHERE ch.co_id = v_complex_id
      AND ch.corp LIKE 'без к%';

    RETURN v_house_id;
    
    END;
$function$
;
