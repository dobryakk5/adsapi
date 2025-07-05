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

-- нормализация имен
UPDATE fias_objects
   SET norm_name = LOWER(REPLACE(name, 'ё', 'е'))

-- убрать точки в типах ул.
UPDATE fias_objects
   SET typename=replace(lower(typename), '.', '') 

UPDATE fias_objects
   SET typename='пр-д'
   where typename='проезд'
UPDATE fias_objects
   SET typename='аллея'
   where typename='ал'
UPDATE fias_objects
   SET typename='сквер'
   where typename='с-р'