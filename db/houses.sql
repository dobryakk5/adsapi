--------
-- 1. Добавляем в таблицу houses колонку parentobjid
ALTER TABLE fias_houses 
  ADD COLUMN IF NOT EXISTS parentobjid BIGINT;

-- 2. Обновляем её значениями из fias_mun_hierarchy
UPDATE fias_houses h
SET parentobjid = m.parentobjid
FROM fias_mun_hierarchy m
WHERE h.objectid = m.objectid
  AND m.parentobjid IS NOT NULL;

-- 3. (Опционально) создаём индекс для ускорения запросов по parentobjid
CREATE INDEX IF NOT EXISTS idx_houses_parentobjid
  ON fias_houses(parentobjid);