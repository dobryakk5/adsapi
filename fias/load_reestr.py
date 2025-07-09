import os
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# Параметр подключения к БД из env
DB_DSN = os.getenv("DATABASE_URL")

# Путь к файлу AS_REESTR_OBJECTS
FILE = "/Users/pavellebedev/Downloads/fias_xml/77/AS_REESTR_OBJECTS_20250626_e0fd548b-f84a-4eaf-934c-ed7272266b8b.XML"

# DDL для создания таблицы
DDL = """
CREATE TABLE IF NOT EXISTS fias_reestr_objects (
    objectid    INT    PRIMARY KEY,
    levelid     SMALLINT  NOT NULL
);
"""

# Размер батча
BATCH_SIZE = 50000

# Кол-во активных записей, которые нужно пропустить
SKIP_ACTIVE_COUNT = 4580000  # ← измените при необходимости

def load_reestr_objects(xml_path: str, dsn: str, skip_active: int = 0):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()

    insert_sql = """
    INSERT INTO fias_reestr_objects (objectid, levelid)
    VALUES %s
    ON CONFLICT (objectid) DO NOTHING;
    """

    rows = []
    batch_cnt = 0
    total_loaded = 0
    active_seen = 0

    for event, elem in ET.iterparse(xml_path, events=('end',)):
        if elem.tag == 'OBJECT':
            if elem.get('ISACTIVE') == '1':
                active_seen += 1
                if active_seen <= skip_active:
                    elem.clear()
                    continue

                oid = elem.get('OBJECTID')
                lvl = elem.get('LEVELID')
                if oid is not None and lvl is not None:
                    rows.append((int(oid), int(lvl)))

            elem.clear()

            if len(rows) >= BATCH_SIZE:
                batch_cnt += 1
                execute_values(cur, insert_sql, rows)
                conn.commit()
                total_loaded += len(rows)
                print(f"Batch {batch_cnt}: inserted {len(rows)} rows (after skipping {skip_active} active records)")
                rows.clear()

    if rows:
        batch_cnt += 1
        execute_values(cur, insert_sql, rows)
        conn.commit()
        total_loaded += len(rows)
        print(f"Batch {batch_cnt}: inserted {len(rows)} rows (final batch)")

    cur.close()
    conn.close()
    print(f"Done: total {total_loaded} active objects loaded in {batch_cnt} batches (skipped {skip_active})")

if __name__ == "__main__":
    load_reestr_objects(FILE, DB_DSN, SKIP_ACTIVE_COUNT)
