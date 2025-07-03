-- 1) Заполняем parent_objectid по данным из fias_division
ALTER TABLE fias_objects
  ADD COLUMN IF NOT EXISTS parent_objectid BIGINT;

UPDATE fias_objects f
SET parent_objectid = d.parentid
FROM fias_division d
WHERE f.objectid = d.childid;

-- 2) Теперь на основе parent_objectid проставляем parentguid
ALTER TABLE fias_objects
  ADD COLUMN IF NOT EXISTS parentguid UUID;

UPDATE fias_objects f
SET parentguid = p.objectguid
FROM fias_objects p
WHERE f.parent_objectid = p.objectid;
