#!/usr/bin/env python3
import os
import time
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# === Настройки ===
DB_DSN = os.getenv("DATABASE_URL")
FILE = "/Users/pavellebedev/Downloads/fias_xml/77/AS_MUN_HIERARCHY_20250626_88fab33f-c535-4906-85e4-4fdedd5784ff.XML"

# DDL для создания таблицы fias_mun_hierarchy
DDL = """
CREATE TABLE IF NOT EXISTS fias_mun_hierarchy (
    objectid     BIGINT PRIMARY KEY,
    parentobjid  BIGINT,
    oktmo        VARCHAR(20),
    path         TEXT
);
"""

# Параметры обработки
BATCH_SIZE = 20000
# Если нужно пропустить уже загруженные строки, задайте SKIP_COUNT
SKIP_COUNT = 5160000


def load_mun_hierarchy(xml_path: str, dsn: str, skip_count: int = SKIP_COUNT):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    # создаём таблицу
    cur.execute(DDL)
    conn.commit()

    insert_sql = """
    INSERT INTO fias_mun_hierarchy (objectid, parentobjid, oktmo, path)
    VALUES %s
    ON CONFLICT (objectid) DO NOTHING;
    """

    rows = []
    count_loaded = 0
    batch_number = 0
    processed = 0

    # потоковый парсинг XML
    for event, elem in ET.iterparse(xml_path, events=('end',)):
        if elem.tag == 'ITEM':
            processed += 1
            # пропускаем первые skip_count элементов, если нужно
            if processed <= skip_count:
                elem.clear()
                continue
            # фильтр активных
            if elem.get('ISACTIVE') == '1':
                rows.append((
                    int(elem.get('OBJECTID')),
                    int(elem.get('PARENTOBJID')) if elem.get('PARENTOBJID') else None,
                    elem.get('OKTMO'),
                    elem.get('PATH')
                ))
            elem.clear()

            if len(rows) >= BATCH_SIZE:
                batch_number += 1
                execute_values(cur, insert_sql, rows)
                conn.commit()
                count_loaded += len(rows)
                print(f"Batch {batch_number}: committed {len(rows)} records (after skipping {skip_count})")
                rows.clear()
                time.sleep(1)  # пауза для разгрузки БД

    # вставка остатка
    if rows:
        batch_number += 1
        execute_values(cur, insert_sql, rows)
        conn.commit()
        count_loaded += len(rows)
        print(f"Batch {batch_number}: committed {len(rows)} records (final batch)")
        rows.clear()

    cur.close()
    conn.close()
    print(f"Loaded {count_loaded} active municipal hierarchy items in {batch_number} batches (skipped {skip_count} items)")

if __name__ == "__main__":
    # Если нужно пропустить ранее загруженные строки, передайте число вместо 0
    load_mun_hierarchy(FILE, DB_DSN, skip_count=SKIP_COUNT)
