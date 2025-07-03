import os
import time
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# Параметр подключения к БД из env
DB_DSN = os.getenv("DATABASE_URL")

# Путь к файлу AS_ADM_HIERARCHY
FILE = "/Users/pavellebedev/Downloads/fias_xml/77/AS_ADM_HIERARCHY_20250626_548b27cb-2de7-4cfa-8a72-24e823e77156.XML"

# DDL для создания таблицы hierarchy
DDL = """
CREATE TABLE IF NOT EXISTS fias_adm_hierarchy (
    id             BIGINT PRIMARY KEY,
    objectid       BIGINT NOT NULL,
    parentobjid    BIGINT,
    changeid       BIGINT,
    regioncode     VARCHAR(10),
    areacode       VARCHAR(10),
    citycode       VARCHAR(10),
    placecode      VARCHAR(10),
    plancode       VARCHAR(10),
    streetcode     VARCHAR(10),
    previd         BIGINT,
    nextid         BIGINT,
    updatedate     DATE,
    startdate      DATE,
    enddate        DATE,
    isactive       BOOLEAN,
    path           TEXT
);
"""

# Размер батча и количество строк для пропуска
BATCH_SIZE = 20000
SKIP_COUNT = 4390000  # количество уже обработанных строк

def load_hierarchy(xml_path: str, dsn: str):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    # создаём таблицу
    cur.execute(DDL)
    conn.commit()

    # Подготовка вставки
    insert_sql = """
    INSERT INTO fias_adm_hierarchy (
      id, objectid, parentobjid, 
      areacode, citycode, placecode, plancode, streetcode, path
    ) VALUES %s
    ON CONFLICT (id) DO NOTHING;
    """

    rows = []
    count_loaded = 0
    batch_number = 0
    processed = 0

    # потоковый парсинг XML
    for event, elem in ET.iterparse(xml_path, events=('end',)):
        if elem.tag == 'ITEM':
            processed += 1
            # пропускаем первые SKIP_COUNT элементов
            if processed <= SKIP_COUNT:
                elem.clear()
                continue
            # обрабатываем только активные
            if elem.get('ISACTIVE') == '1':
                rows.append((
                    int(elem.get('ID')),
                    int(elem.get('OBJECTID')),
                    int(elem.get('PARENTOBJID')) if elem.get('PARENTOBJID') else None,
                    elem.get('AREACODE'),
                    elem.get('CITYCODE'),
                    elem.get('PLACECODE'),
                    elem.get('PLANCODE'),
                    elem.get('STREETCODE'),
                    elem.get('PATH')
                ))
            elem.clear()

            # коммит батча по BATCH_SIZE
            if len(rows) >= BATCH_SIZE:
                batch_number += 1
                execute_values(cur, insert_sql, rows)
                conn.commit()
                count_loaded += len(rows)
                print(f"Batch {batch_number}: committed {len(rows)} records (after skipping {SKIP_COUNT})")
                rows.clear()
                #time.sleep(1) 

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
    print(f"Loaded {count_loaded} active hierarchy items in {batch_number} batches (skipped {SKIP_COUNT} items)")

if __name__ == "__main__":
    load_hierarchy(FILE, DB_DSN)
