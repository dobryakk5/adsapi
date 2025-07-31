-- DROP PROCEDURE public.batch_upsert();

CREATE OR REPLACE PROCEDURE public.batch_upsert()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
  ------------------------------------------------------------
  -- 1. Upsert into flats (add metro_id, km_do_metro)
  ------------------------------------------------------------
  INSERT INTO flats (
    house_id, floor, rooms,
    street, street_type, house,
    town, total_floors,
    area, living_area, kitchen_area,
    house_type_id, ao_id, built,
    metro_id, km_do_metro
  )
  SELECT DISTINCT
    house_id, floor, rooms,
    street, street_type, house,
    town, total_floors,
    area, living_area, kitchen_area,
    house_type_id, ao_id, built,
    metro_id, km_do_metro
  FROM tmp_flats_history
  ON CONFLICT DO NOTHING;


  ------------------------------------------------------------
  -- 2. Подготовка списка для snapshot
  ------------------------------------------------------------
  WITH
  -- 2.1) выявляем старые данные для сравнения
  old_data AS (
    SELECT
      avitoid,
      source_id,
      price      AS old_price,
      is_actual  AS old_status,
      description AS old_desc
    FROM flats_history
  ),
  -- 2.2) определяем, кому нужен snapshot:
  to_snapshot AS (
    -- 2.2a) новые объявления (нет старой записи)
    SELECT
      tmp.avitoid,
      tmp.source_id
    FROM tmp_flats_history tmp
    LEFT JOIN old_data od
      ON od.avitoid   = tmp.avitoid
     AND od.source_id = tmp.source_id
    WHERE od.avitoid IS NULL

    UNION ALL

    -- 2.2b) уже существующие, но с изменениями в любом из трёх полей
    SELECT
      tmp.avitoid,
      tmp.source_id
    FROM tmp_flats_history tmp
    JOIN old_data od
      ON od.avitoid   = tmp.avitoid
     AND od.source_id = tmp.source_id
    WHERE od.old_price    IS DISTINCT FROM tmp.price
       OR od.old_status   IS DISTINCT FROM tmp.is_actual
       OR od.old_desc     IS DISTINCT FROM tmp.description
  ),

  ------------------------------------------------------------
  -- 3. UPSERT into flats_history, возвращаем новые/обновлённые строки
  ------------------------------------------------------------
  upserted AS (
    INSERT INTO flats_history (
      house_id, floor, rooms,
      source_id, object_type_id, ad_type,
      url, person_type_id, price,
      time_source_created, time_source_updated,
      avitoid, is_actual, description
    )
    SELECT
      house_id, floor, rooms,
      source_id, object_type_id, nedvigimost_type_id,
      url, person_type_id, price,
      time_source_created, time_source_updated,
      avitoid, is_actual, description
    FROM tmp_flats_history

    ON CONFLICT (avitoid, source_id) DO UPDATE
      SET
        price               = EXCLUDED.price,
        time_source_updated = EXCLUDED.time_source_updated,
        is_actual           = EXCLUDED.is_actual,
        description         = EXCLUDED.description

    RETURNING
      id,
      avitoid,
      source_id,
      time_source_updated,
      price,
      is_actual,
      description
  )

  ------------------------------------------------------------
  -- 4. Snapshot‑based history: пишем только для тех, кто в to_snapshot
  ------------------------------------------------------------
  INSERT INTO flats_changes (
    flats_history_id,
    updated,
    price,
    is_actual,
    description
  )
  SELECT
    u.id,
    u.time_source_updated,
    u.price,
    u.is_actual,
    u.description
  FROM upserted u
  JOIN to_snapshot ts
    ON ts.avitoid   = u.avitoid
   AND ts.source_id = u.source_id
  ;


  ------------------------------------------------------------
  -- 5. Clean up processed ads
  ------------------------------------------------------------
  DELETE FROM ads
  WHERE processed IS TRUE;

END;
$procedure$
;
