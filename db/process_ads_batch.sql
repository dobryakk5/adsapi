-- DROP PROCEDURE public.process_ads_batch(int4);

CREATE OR REPLACE PROCEDURE public.process_ads_batch(IN p_batch_size integer DEFAULT 20)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  rec               RECORD;
  parts 			TEXT[];
  part       	    TEXT;
  v_not_jk          BOOLEAN;
  v_try_zelen       BOOLEAN;
  debug_data        JSONB;
  v_st_type         TEXT;
  v_norm_name       TEXT;
  v_house_part      TEXT;
  house_id          INT;
  parent_ids        INT[];
  floor_raw         TEXT;
  floor_val         INT;
  rooms_raw         TEXT;
  rooms_val         INT;
  total_floors_raw  TEXT;
  total_floors      INT;
  area_val          NUMERIC;
  living_val        NUMERIC;
  kitchen_val       NUMERIC;
  house_type_id     SMALLINT;
  object_type_id    SMALLINT;
  ao_id             SMALLINT;
  built_raw         TEXT;
  built_year        SMALLINT;
  v_metro_id        SMALLINT;
  v_km_do_metro     NUMERIC(6,3);
BEGIN
  CREATE TEMP TABLE tmp_flats_history (
    house_id            integer,
    floor               smallint,
    rooms               smallint,
    street              text,
    street_type         varchar(9),
    house               text,
    town                smallint,
    total_floors        smallint,
    area                numeric,
    living_area         numeric,
    kitchen_area        numeric,
    house_type_id       smallint,
    ao_id               smallint,
    built               smallint,
    metro_id            smallint,
    km_do_metro         numeric(6,3),
    source_id           smallint,
    object_type_id      smallint,
    nedvigimost_type_id smallint,
    url                 text,
    person_type_id      smallint,
    price               numeric,
    time_source_created date,
    time_source_updated timestamp,
    avitoid             text,
    is_actual           smallint,
    description         text
  ) ON COMMIT DROP;

  FOR rec IN
    SELECT
      id, address, city, district_only,
      source_id, url, person_type_id, price,
      time_source_created, time_source_updated,
      params, params2, is_actual, avitoid,
      nedvigimost_type_id, description,
      metro_only, km_do_metro,
      COALESCE(
        regexp_replace(
          split_part(
            REPLACE(REPLACE(params ->> 'Название ЖК', 'ё', 'е'), 'Ё', 'Е'),
            ',', 1
          ),
          '\s*\(.*\)', ''
        ),
        ''
      ) AS jk_name
    FROM ads
    WHERE processed IS FALSE
    LIMIT p_batch_size
  LOOP
    v_not_jk      := FALSE;
    v_try_zelen   := FALSE;
    debug_data    := jsonb_build_object();
    house_id      := NULL;

    SELECT street_type, norm_name, house_part
      INTO v_st_type, v_norm_name, v_house_part
      FROM public.parse_address(rec.address)
      LIMIT 1;
    
	-- Разбиваем адрес на части (или пустой массив, если NULL)
	parts := COALESCE(string_to_array(rec.address, ','), ARRAY[]::text[]);
	
	-- Проходим по каждому элементу parts
	FOREACH part IN ARRAY parts LOOP
	  IF part ILIKE '%жилой комплекс%' OR part ILIKE '%ЖК%' THEN
	    IF part ILIKE '%жилой комплекс%' THEN
	      rec.jk_name := btrim(
	        regexp_replace(part, '(?i)жилой комплекс', '', 'g')
	      );
	    ELSE
	      rec.jk_name := btrim(
	        regexp_replace(part, '(?i)ЖК', '', 'g')
	      );
	    END IF;
	    EXIT;  -- нашли, выходим из цикла
	  END IF;
	END LOOP;



    IF rec.jk_name <> '' THEN
      house_id := get_house_id_by_jk_and_corp(rec.jk_name, rec.address);
      IF house_id IS NULL THEN
        v_not_jk := TRUE;
        debug_data := debug_data || jsonb_build_object(
          'jk_name', rec.jk_name,
          'jk_not_found', TRUE
        );
        UPDATE ads SET debug = debug_data, proc_at = now() WHERE id = rec.id;
      END IF;
    ELSE
      debug_data := debug_data || jsonb_build_object('jk_missing', TRUE);
      UPDATE ads SET debug = debug_data, proc_at = now() WHERE id = rec.id;
    END IF;

    IF (rec.jk_name = '' OR v_not_jk) or rec.address ILIKE '%зеленоград%' or rec.city ILIKE '%зеленоград%' OR rec.city ILIKE '%новая москва%' OR rec.district_only ILIKE 'нао' THEN
      debug_data := jsonb_build_object('skip_region', rec.city);
      UPDATE ads SET debug = debug_data, processed = TRUE, proc_at = now() WHERE id = rec.id;
      CONTINUE;
    END IF;

    IF house_id IS NULL AND NOT v_try_zelen AND (rec.jk_name = '' OR v_not_jk) THEN
      house_id := public.get_house_id_by_address(rec.address);
      IF house_id IS NULL THEN
        debug_data := debug_data || jsonb_build_object(
          'general_parse', TRUE,
          'general_not_found', TRUE
        );
        UPDATE ads SET debug = debug_data, proc_at = now() WHERE id = rec.id;
      END IF;
    END IF;

    floor_raw := rec.params->>'Этаж';
    IF floor_raw ~ '^[0-9]+$' THEN
      floor_val := floor_raw::INT;
    ELSE
      floor_val := NULL;
      debug_data := debug_data || jsonb_build_object('floor_raw_error', floor_raw);
      UPDATE ads SET debug = debug_data, proc_at = now() WHERE id = rec.id;
    END IF;

    rooms_raw := rec.params->>'Количество комнат';
    IF rooms_raw ILIKE '%студ%' THEN
      rooms_val := 0;
    ELSIF rooms_raw ~ '^[0-9]+$' THEN
      rooms_val := rooms_raw::INT;
    ELSE
      rooms_val := NULL;
      debug_data := debug_data || jsonb_build_object('rooms_raw_error', rooms_raw);
      UPDATE ads SET debug = debug_data, proc_at = now() WHERE id = rec.id;
    END IF;

    IF house_id IS NULL OR floor_val IS NULL OR rooms_val IS NULL THEN
      debug_data := debug_data || jsonb_build_object(
        'skip', TRUE,
        'house_id', house_id,
        'floor_val', floor_val,
        'rooms_val', rooms_val
      );
      UPDATE ads SET debug = debug_data, processed = NULL, proc_at = now() WHERE id = rec.id;
      CONTINUE;
    END IF;

    total_floors_raw := rec.params->>'Этажей в доме';
    IF total_floors_raw ~ '^[0-9]+$' THEN
      total_floors := total_floors_raw::INT;
    ELSE
      total_floors := NULL;
      debug_data := debug_data || jsonb_build_object('total_floors_error', total_floors_raw);
      UPDATE ads SET debug = debug_data, proc_at = now() WHERE id = rec.id;
    END IF;

    area_val     := (rec.params->>'Площадь')::NUMERIC;
    living_val   := (rec.params->>'Жилая площадь')::NUMERIC;
    kitchen_val  := (rec.params->>'Площадь кухни')::NUMERIC;

    SELECT id INTO house_type_id
      FROM lookup_types
     WHERE category='house_type'
       AND lower(name)=lower(rec.params->>'Тип дома')
     LIMIT 1;

    SELECT id INTO object_type_id
      FROM lookup_types
     WHERE category='object_type'
       AND lower(name)=lower(rec.params->>'Вид объекта')
     LIMIT 1;

    SELECT id INTO ao_id
      FROM districts
     WHERE lower(admin_okrug)=lower(rec.district_only)
     LIMIT 1;

    built_raw := COALESCE(rec.params2->'О здании'->>'Год постройки', right(rec.params->>'Срок сдачи', 4));
    IF built_raw ~ '^[0-9]{4}$' THEN
      built_year := built_raw::INT;
    ELSE
      built_year := NULL;
      debug_data := debug_data || jsonb_build_object('built_raw_error', built_raw);
      UPDATE ads SET debug = debug_data, proc_at = now() WHERE id = rec.id;
    END IF;

    BEGIN
      INSERT INTO tmp_flats_history (
        house_id, floor, rooms,
        street, street_type, house,
        town, total_floors, area, living_area, kitchen_area,
        house_type_id, ao_id, built, metro_id, km_do_metro,
        source_id, object_type_id, nedvigimost_type_id,
        url, person_type_id, price,
        time_source_created, time_source_updated,
        avitoid, is_actual, description
      ) VALUES (
        house_id, floor_val, rooms_val,
        v_norm_name, v_st_type, v_house_part,
        1, total_floors, area_val, living_val, kitchen_val,
        house_type_id, ao_id, built_year, v_metro_id, v_km_do_metro,
        rec.source_id, object_type_id, rec.nedvigimost_type_id,
        rec.url, rec.person_type_id, rec.price,
        rec.time_source_created, rec.time_source_updated,
        rec.avitoid, rec.is_actual, rec.description
      );
      UPDATE ads SET processed = TRUE, proc_at = now() WHERE id = rec.id;
    EXCEPTION WHEN OTHERS THEN
      debug_data := debug_data || jsonb_build_object(
        'error', SQLERRM,
        'rec_id', rec.id
      );
      UPDATE ads SET debug = debug_data, processed = NULL, proc_at = now() WHERE id = rec.id;
      RAISE NOTICE 'Ошибка вставки tmp_flats_history: %, id=%', SQLERRM, rec.id;
    END;

  END LOOP;

  CALL batch_upsert_flats_and_history();
END;
$procedure$
;
