CREATE OR REPLACE FUNCTION get_house_id_by_jk_and_corp(
  p_jk_name      TEXT,
  p_full_address TEXT
)
  RETURNS SMALLINT
  LANGUAGE plpgsql
AS $$
DECLARE
  v_complex_id   SMALLINT;
  v_corp_raw     TEXT;
  v_corp_clean   TEXT;
  v_house_id     SMALLINT;
BEGIN
  IF p_jk_name IS NULL OR btrim(p_jk_name) = '' THEN
    RAISE EXCEPTION 'Название ЖК не задано';
  END IF;

  SELECT id
    INTO v_complex_id
    FROM complexes AS cmp
   WHERE cmp.title ILIKE '%' || p_jk_name || '%'
   LIMIT 1;

  IF NOT FOUND THEN
    RAISE NOTICE 'ЖК с именем "%" не найден', p_jk_name;
    RETURN NULL;
  END IF;

  v_corp_raw := regexp_replace(p_full_address, '^.*,\s*', '');
  v_corp_clean := regexp_replace(
    v_corp_raw,
    '^\s*(корпус|корп(?:\.|ус)?|к\.?)\s*',
    '',
    'i'
  );
  v_corp_clean := btrim(v_corp_clean);

  SELECT id
    INTO v_house_id
    FROM complex_houses AS ch
   WHERE ch.co_id = v_complex_id
     AND ch.corp ILIKE v_corp_clean
   LIMIT 1;

  IF FOUND THEN
    RETURN v_house_id;
  END IF;

  SELECT id
    INTO v_house_id
    FROM complex_houses AS ch
   WHERE ch.co_id = v_complex_id
     AND ch.corp ILIKE 'без к%'
   LIMIT 1;

  IF FOUND THEN
    RETURN v_house_id;
  END IF;

  RAISE NOTICE 'Корпус "%" в ЖК id=% не найден', v_corp_clean, v_complex_id;
  RETURN NULL;
END;
$$;
