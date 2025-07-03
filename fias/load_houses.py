import os
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# Параметр подключения к БД из env
DB_DSN = os.getenv("DATABASE_URL")

# Путь к файлу AS_HOUSES
FILE = "/Users/pavellebedev/Downloads/fias_xml/77/AS_HOUSES_20250626_2dc5398d-6bdb-493b-829c-9100f0f05bb9.XML"

# DDL для создания таблицы fias_houses без полей previd/nextid/updatedate/startdate/enddate
DDL = """
CREATE TABLE IF NOT EXISTS fias_houses (
    id            BIGINT PRIMARY KEY,
    objectid      BIGINT NOT NULL,
    objectguid    UUID NOT NULL,
    changeid      BIGINT,
    housenum      VARCHAR(50),
    housetype     SMALLINT,
    opertypeid    SMALLINT,
    addnum1       VARCHAR(20),
    addtype1      SMALLINT,
    addnum2       VARCHAR(20),
    addtype2      SMALLINT,
    addnum3       VARCHAR(20),
    addtype3      SMALLINT,
    addnum4       VARCHAR(20),
    addtype4      SMALLINT,
    addnum5       VARCHAR(20),
    addtype5      SMALLINT,
    addnum6       VARCHAR(20),
    addtype6      SMALLINT,
    isactual      BOOLEAN,
    isactive      BOOLEAN
);
"""

# Размер батча
BATCH_SIZE = 20000

def load_houses(xml_path: str, dsn: str):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    # создаём таблицу
    cur.execute(DDL)
    conn.commit()

    # SQL для вставки без полей previd/nextid/updatedate/startdate/enddate
    insert_sql = """
    INSERT INTO fias_houses (
      id, objectid, objectguid, changeid, housenum, housetype, opertypeid,
      addnum1, addtype1, addnum2, addtype2, addnum3, addtype3,
      addnum4, addtype4, addnum5, addtype5, addnum6, addtype6,
      isactual, isactive
    ) VALUES %s
    ON CONFLICT (id) DO NOTHING;
    """

    rows = []
    batch_number = 0
    count_loaded = 0

    # потоковый парсинг XML
    for event, elem in ET.iterparse(xml_path, events=('end',)):
        if elem.tag == 'HOUSE':
            if elem.get('ISACTIVE') == '1':
                rows.append((
                    int(elem.get('ID')),
                    int(elem.get('OBJECTID')),
                    elem.get('OBJECTGUID'),
                    int(elem.get('CHANGEID')) if elem.get('CHANGEID') else None,
                    elem.get('HOUSENUM'),
                    int(elem.get('HOUSETYPE')) if elem.get('HOUSETYPE') else None,
                    int(elem.get('OPERTYPEID')) if elem.get('OPERTYPEID') else None,
                    elem.get('ADDNUM1'),
                    int(elem.get('ADDTYPE1')) if elem.get('ADDTYPE1') else None,
                    elem.get('ADDNUM2'),
                    int(elem.get('ADDTYPE2')) if elem.get('ADDTYPE2') else None,
                    elem.get('ADDNUM3'),
                    int(elem.get('ADDTYPE3')) if elem.get('ADDTYPE3') else None,
                    elem.get('ADDNUM4'),
                    int(elem.get('ADDTYPE4')) if elem.get('ADDTYPE4') else None,
                    elem.get('ADDNUM5'),
                    int(elem.get('ADDTYPE5')) if elem.get('ADDTYPE5') else None,
                    elem.get('ADDNUM6'),
                    int(elem.get('ADDTYPE6')) if elem.get('ADDTYPE6') else None,
                    bool(int(elem.get('ISACTUAL'))) if elem.get('ISACTUAL') else None,
                    True
                ))
            elem.clear()

            if len(rows) >= BATCH_SIZE:
                batch_number += 1
                execute_values(cur, insert_sql, rows)
                conn.commit()
                count_loaded += len(rows)
                print(f"Batch {batch_number}: committed {len(rows)} records")
                rows.clear()

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
    load_houses(FILE, DB_DSN)
