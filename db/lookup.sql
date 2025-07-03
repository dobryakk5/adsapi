-- Скрипт создания справочника lookup_types и наполнения его значениями
-- по официальной документации ads-api.ru (https://ads-api.ru/api)

-- 1) Создаём таблицу lookup_types (без авто-инкремента, т.к. id берём из API)
CREATE TABLE IF NOT EXISTS lookup_types (
  id       SMALLINT NOT NULL,
  category TEXT     NOT NULL,   -- 'ad_type', 'source_id', 'object_type', 'house_type'
  name     TEXT     NOT NULL,
  UNIQUE(category, name)
);

-- 2) Наполняем справочник значениями в соответствии с API

-- 2.1) Тип объявления (nedvigimost_type):
--     1 — Продам
--     2 — Сдам
--     3 — Куплю
--     4 — Сниму
INSERT INTO lookup_types(id, category, name) VALUES
  ( 1, 'ad_type',    'Продам'),
  ( 2, 'ad_type',    'Сдам'),
  ( 3, 'ad_type',    'Куплю'),
  ( 4, 'ad_type',    'Сниму')
ON CONFLICT(category, name) DO NOTHING;

-- 2.2) Источник объявления (source_id):
--     1  — avito.ru  
--     3  — realty.yandex.ru  
--     4  — cian.ru  
--     5  — sob.ru  
--     6  — youla.io  
--     7  — n1.ru  
--    10  — moyareklama.ru  
--    11  — domclick.ru  
INSERT INTO lookup_types(id, category, name) VALUES
  ( 1, 'source_id', 'avito.ru'),
  ( 3, 'source_id', 'realty.yandex.ru'),
  ( 4, 'source_id', 'cian.ru'),
  ( 5, 'source_id', 'sob.ru'),
  ( 6, 'source_id', 'youla.io'),
  ( 7, 'source_id', 'n1.ru'),
  (10, 'source_id', 'moyareklama.ru'),
  (11, 'source_id', 'domclick.ru')
ON CONFLICT(category, name) DO NOTHING;

-- 2.3) Вид объекта (object_type):
--     1 — Первичка
--     2 — Вторичка
INSERT INTO lookup_types(id, category, name) VALUES
  ( 1, 'object_type', 'Первичка'),
  ( 2, 'object_type', 'Вторичка')
ON CONFLICT(category, name) DO NOTHING;

-- 2.4) Тип дома (house_type):
--     1 — Панельный
--     2 — Кирпичный
--     3 — Монолитный
--     4 — Газобетонный
--     5 — Блочный
INSERT INTO lookup_types(id, category, name) VALUES
  ( 1, 'house_type', 'Панельный'),
  ( 2, 'house_type', 'Кирпичный'),
  ( 3, 'house_type', 'Монолитный'),
  ( 4, 'house_type', 'Газобетонный'),
  ( 5, 'house_type', 'Блочный')
ON CONFLICT(category, name) DO NOTHING;
