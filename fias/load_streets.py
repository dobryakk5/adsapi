#!/usr/bin/env python3
import os
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# === Настройки ===
MOCKVA_DIR = "/Users/pavellebedev/Downloads/fias_xml/77"
PREFIX = "AS_ADDR_OBJ"
TYPE_FULL = {
    "ул":       "улица",
    "пер":      "переулок",
    "пр-кт":    "проспект",
    "просп":    "проспект",
    "ш":        "шоссе",
    "бул":      "бульвар",
    "б-р":      "бульвар",
    "пл":       "площадь",
    "наб":      "набережная",
    "пр-д":     "проезд",
    "проезд":   "проезд",
}

# Конфигурация подключения
DB_DSN = os.getenv("DATABASE_URL")

# Парсим улицы из XML: возвращает кортежи (guid, objectid, name)
def parse_moscow_streets():
    out = []
    for fn in sorted(os.listdir(MOCKVA_DIR)):
        if not fn.startswith(PREFIX) or not fn.endswith('.XML'):
            continue
        path = os.path.join(MOCKVA_DIR, fn)
        print(f"Reading {path}…")
        for _, elem in ET.iterparse(path, events=("start",)):
            if elem.tag == "OBJECT":
                if elem.attrib.get("LEVEL") == "8" and elem.attrib.get("ISACTUAL") == "1":
                    guid = elem.attrib.get("OBJECTGUID")
                    oid = int(elem.attrib.get("OBJECTID"))
                    name = elem.attrib.get("NAME", "").strip()
                    t = elem.attrib.get("TYPENAME", "").strip().lower()
                    full_t = TYPE_FULL.get(t, t)
                    full_name = f"{name} {full_t}".strip()
                    out.append((guid, oid, full_name))
                elem.clear()
    print(f"Parsed {len(out)} street objects")
    return out

# Импортируем в БД: добавляем objectid и parentobjid
def import_to_db(rows):
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    # Создаём таблицу с нужными полями
    cur.execute("""
    CREATE TABLE IF NOT EXISTS streets (
      id            SERIAL PRIMARY KEY,
      fias_id       UUID     UNIQUE NOT NULL,
      objectid      BIGINT   NOT NULL,
      parentobjid   BIGINT,
      name          TEXT     NOT NULL
    );
    """)
    conn.commit()

    # Получаем parentobjid для каждого objectid
    objectids = [oid for _, oid, _ in rows]
    sql_parent = "SELECT objectid, parentobjid FROM fias_adm_hierarchy WHERE objectid = ANY(%s)"
    cur.execute(sql_parent, (objectids,))
    mapping = {obj: parent for obj, parent in cur.fetchall()}

    # Склеиваем для вставки (fias_id, objectid, parentobjid, name)
    data = []
    for guid, oid, name in rows:
        parent = mapping.get(oid)
        data.append((guid, oid, parent, name))

    # Вставка пакетом
    insert_sql = "INSERT INTO streets(fias_id, objectid, parentobjid, name) VALUES %s ON CONFLICT (fias_id) DO NOTHING"
    execute_values(cur, insert_sql, data, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Imported {len(data)} streets with objectid and parentobjid")

if __name__ == "__main__":
    streets = parse_moscow_streets()
    import_to_db(streets)
