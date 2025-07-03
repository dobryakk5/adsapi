#!/usr/bin/env python3
import os
import time
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# === Настройки ===
DB_DSN = os.getenv("DATABASE_URL")
FILE = "/Users/pavellebedev/Downloads/fias_xml/77/AS_HOUSES_PARAMS_20250627_525c3636-4b55-47d4-8458-0d17ce96a914.XML"

# DDL для создания таблицы fias_house_params без UPDATEDATE, STARTDATE, CHANGEID
DDL = """
CREATE TABLE IF NOT EXISTS fias_house_params (
    id            BIGINT PRIMARY KEY,
    objectid      BIGINT NOT NULL,
    changeidend   BIGINT,
    typeid        INTEGER,
    value         TEXT,
    enddate       DATE
);
"""

# Размер батча
BATCH_SIZE = 20000


def load_house_params(xml_path: str, dsn: str, skip_count: int = 260000):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    # создаём таблицу
    cur.execute(DDL)
    conn.commit()

    insert_sql = """
    INSERT INTO fias_house_params (
      id, objectid, changeidend, typeid, value, enddate
    ) VALUES %s
    ON CONFLICT (id) DO NOTHING;
    """

    rows = []
    batch_number = 0
    count_loaded = 0
    processed = 0

    for event, elem in ET.iterparse(xml_path, events=('end',)):
        if elem.tag == 'PARAM':
            processed += 1
            if processed <= skip_count:
                elem.clear()
                continue
            # собираем поля
            rows.append((
                int(elem.get('ID')),
                int(elem.get('OBJECTID')),
                int(elem.get('CHANGEIDEND')) if elem.get('CHANGEIDEND') and elem.get('CHANGEIDEND') != '0' else None,
                int(elem.get('TYPEID')) if elem.get('TYPEID') else None,
                elem.get('VALUE'),
                elem.get('ENDDATE')
            ))
            elem.clear()

            if len(rows) >= BATCH_SIZE:
                batch_number += 1
                execute_values(cur, insert_sql, rows)
                conn.commit()
                count_loaded += len(rows)
                print(f"Batch {batch_number}: committed {len(rows)} records (after skipping {skip_count})")
                rows.clear()
                # пауза для разгрузки БД
                time.sleep(1)

    # остаток
    if rows:
        batch_number += 1
        execute_values(cur, insert_sql, rows)
        conn.commit()
        count_loaded += len(rows)
        print(f"Batch {batch_number}: committed {len(rows)} records (final batch)")

    cur.close()
    conn.close()
    print(f"Всего загружено {count_loaded} записей в {batch_number} батчах.")

if __name__ == "__main__":
    load_house_params(FILE, DB_DSN)
