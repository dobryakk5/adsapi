-- DROP PROCEDURE public.process_all_ads();

CREATE OR REPLACE PROCEDURE public.process_all_ads()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  now_ts TIMESTAMP := now();
BEGIN
  -- 1. Подготовка временных таблиц
  TRUNCATE TABLE tmp_flats_history;
  CREATE TEMP TABLE tmp_debug (
    ad_id    INTEGER,
    debug    JSONB,
    success  BOOLEAN
  ) ON COMMIT DROP;

  -- 2. Подготовка обогащённых данных без фильтрации по batch_size
  CREATE TEMP TABLE tmp_enriched ON COMMIT DROP AS
  SELECT 
    a.id            AS ad_id,
    a.*,
    pa.norm_name    AS street,
    pa.street_type,
    pa.house_part   AS house,
    ht.id           AS house_type_id,
    ot.id           AS object_type_id,
    d.id            AS ao_id,
    COALESCE(
      regexp_replace(
        split_part(
          replace(replace(a.params->>'Название ЖК','ё','е'),'Ё','Е'),
          ',',1
        ),
        '\s*\(.*\)',''
      ), ''
    ) AS jk_name
  FROM ads a
  LEFT JOIN LATERAL public.parse_address(a.address) pa ON TRUE
  LEFT JOIN lookup_types ht
    ON ht.category = 'house_type'
   AND lower(ht.name) = lower(a.params->>'Тип дома')
  LEFT JOIN lookup_types ot
    ON ot.category = 'object_type'
   AND lower(ot.name) = lower(a.params->>'Вид объекта')
  LEFT JOIN districts d
    ON lower(d.admin_okrug) = lower(a.district_only)
  WHERE a.processed IS FALSE;

  -- 3. Вставка в tmp_flats_history с дедупликацией и отбором по house_id
  INSERT INTO tmp_flats_history (
    ad_id, house_id, floor, rooms,
    street, street_type, house,
    town, total_floors, area, living_area, kitchen_area,
    house_type_id, ao_id, built, metro_id, km_do_metro,
    source_id, object_type_id, nedvigimost_type_id,
    url, person_type_id, price,
    time_source_created, time_source_updated,
    avitoid, is_actual, description
  )
  SELECT DISTINCT ON (e.avitoid, e.source_id)
    e.ad_id,
    COALESCE(
      get_house_id_by_jk(e.jk_name, e.address),
      gha.result_id
    ) AS house_id,
    CASE WHEN e.params->>'Этаж' ~ '^[0-9]+$' 
         THEN (e.params->>'Этаж')::smallint END AS floor,
    CASE 
      WHEN e.params->>'Количество комнат' ILIKE '%студ%' THEN 0
      WHEN e.params->>'Количество комнат' ~ '^[0-9]+$' 
           THEN (e.params->>'Количество комнат')::smallint
      ELSE 0
    END AS rooms,
    e.street, e.street_type, e.house,
    1 AS town,
    CASE WHEN e.params->>'Этажей в доме' ~ '^[0-9]+$' 
         THEN (e.params->>'Этажей в доме')::smallint END AS total_floors,
    (e.params->>'Площадь')::numeric,
    (e.params->>'Жилая площадь')::numeric,
    (e.params->>'Площадь кухни')::numeric,
    e.house_type_id, e.ao_id,
    CASE 
      WHEN COALESCE(
             e.params2->'О здании'->>'Год постройки',
             right(e.params->>'Срок сдачи',4)
           ) ~ '^[0-9]{4}$'
      THEN COALESCE(
             e.params2->'О здании'->>'Год постройки',
             right(e.params->>'Срок сдачи',4)
           )::smallint
    END AS built,
    NULL AS metro_id,
    e.km_do_metro,
    e.source_id, e.object_type_id, e.nedvigimost_type_id,
    e.url, e.person_type_id, e.price,
    e.time_source_created, e.time_source_updated,
    e.avitoid, e.is_actual, e.description
  FROM tmp_enriched e
  LEFT JOIN LATERAL get_house_id_by_address(e.address) AS gha(result_id, street_found, house_part) ON TRUE
  WHERE COALESCE(
          get_house_id_by_jk(e.jk_name, e.address),
          gha.result_id
        ) IS NOT NULL
  ORDER BY e.avitoid, e.source_id, e.time_source_updated DESC;

  -- 3.1. Обновляем все успешно вставленные объявления
  UPDATE ads a
  SET 
    processed = TRUE,
    proc_at   = now_ts
  FROM tmp_flats_history h
  WHERE a.id = h.ad_id;

  -- 4. Логирование тех, у кого house_id IS NULL
  INSERT INTO tmp_debug(ad_id, debug, success)
  SELECT
    e.ad_id,
    jsonb_build_object(
      'raw_jk_name',   e.jk_name,
      'jk_match_id',   get_house_id_by_jk(e.jk_name, e.address),
      'addr_match_id', gha.result_id,
      'street_found',  gha.street_found,
      'house_part',    gha.house_part
    ),
    FALSE
  FROM tmp_enriched e
  LEFT JOIN LATERAL get_house_id_by_address(e.address) AS gha(result_id, street_found, house_part) ON TRUE
  WHERE COALESCE(
          get_house_id_by_jk(e.jk_name, e.address),
          gha.result_id
        ) IS NULL;

  -- 5. Обновление ads: отмечаем неудачные как processed = NULL, сохраняем debug
  UPDATE ads a
  SET
    processed = NULL,
    proc_at   = now_ts,
    debug     = d.debug
  FROM tmp_debug d
  WHERE a.id = d.ad_id;

  -- 6. Upsert истории и основных данных
  CALL batch_upsert();
END;
$procedure$
;
