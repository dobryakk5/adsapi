-- 1) Создание/обновление временной таблицы tmp_flats_history

CREATE TEMP TABLE IF NOT EXISTS tmp_flats_history (
  -- для flats
  house_id            integer,
  floor               smallint,
  rooms               smallint,
  street              text,
  street_type         varchar(9),
  house               varchar(8),
  town                smallint,
  total_floors        smallint,
  area                numeric,
  living_area         numeric,
  kitchen_area        numeric,
  house_type_id       smallint,
  ao_id               smallint,
  built               smallint,

  -- для flats_history
  source_id           smallint,
  object_type         smallint,
  nedvigimost_type_id smallint,
  url                 text,
  person_type_id      smallint,
  price               numeric,
  time_source_created date,
  time_source_updated timestamp,
  avitoid             bigint,
  is_actual           smallint,
  description         text
) ON COMMIT DROP;

-- 2) Функция пакетного UPSERT + логирования изменений цены
CREATE OR REPLACE FUNCTION batch_upsert_flats_and_history()
RETURNS void AS $$
BEGIN
  -- 1. Вставка в flats
  INSERT INTO flats (
    house_id, floor, rooms,
    street, street_type, house,
    town, total_floors,
    area, living_area, kitchen_area,
    house_type_id, ao_id, built
  )
  SELECT DISTINCT
    house_id, floor, rooms,
    street, street_type, house,
    town, total_floors,
    area, living_area, kitchen_area,
    house_type_id, ao_id, built
  FROM tmp_flats_history
  ON CONFLICT DO NOTHING;

  -- 2. Сравнение и фиксация изменений цены
  INSERT INTO flats_price_changes (
    avitoid, source_id, old_price, new_price
  )
  SELECT
    tmp.avitoid,
    tmp.source_id,
    fh.price AS old_price,
    tmp.price AS new_price
  FROM tmp_flats_history tmp
  JOIN flats_history fh
    ON fh.avitoid = tmp.avitoid AND fh.source_id = tmp.source_id
  WHERE fh.price IS DISTINCT FROM tmp.price;

  -- 3. Вставка в flats_history, если такая запись ещё не существует
  INSERT INTO flats_history (
    house_id, floor, rooms,
    source_id, object_type, ad_type,
    url, person_type_id, price,
    time_source_created, time_source_updated,
    avitoid, is_actual, description
  )
  SELECT
    house_id, floor, rooms,
    source_id, object_type, nedvigimost_type_id,
    url, person_type_id, price,
    time_source_created, time_source_updated,
    avitoid, is_actual, description
  FROM tmp_flats_history
  ON CONFLICT (avitoid, source_id) DO NOTHING;
END;
$$ LANGUAGE plpgsql;


-- 3) Использование из Python:
--    3.1. Залить данные (включая description) в tmp_flats_history (через COPY или execute_values).
--    3.2. Выполнить:
--        cur.execute("CALL batch_upsert_flats_history();")
