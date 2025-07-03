import os
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# Параметр подключения к БД из env
DB_DSN = os.getenv("DATABASE_URL")

# Путь к XML-файлу
FILE = "/Users/pavellebedev/Downloads/fias_xml/77/AS_ADDR_OBJ_DIVISION_20250627_703710d4-5a2d-4855-8d13-4ec68fd38bb8.XML"

# DDL: создаём таблицу для DIVISION
DDL = """
CREATE TABLE IF NOT EXISTS fias_division (
    id        BIGINT PRIMARY KEY,
    parentid  BIGINT NOT NULL,
    childid   BIGINT NOT NULL,
    changeid  BIGINT
);
"""

def load_division(xml_path: str, dsn: str):
    # подключаемся
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    # создаём таблицу
    cur.execute(DDL)
    conn.commit()

    # парсим XML
    tree = ET.parse(xml_path)
    root = tree.getroot()

    rows = []
    for item in root.findall('.//ITEM'):
        rows.append((
            int(item.get('ID')),
            int(item.get('PARENTID')),
            int(item.get('CHILDID')),
            int(item.get('CHANGEID')) if item.get('CHANGEID') else None
        ))

    # пакетная вставка
    insert_sql = """
    INSERT INTO fias_division (id, parentid, childid, changeid)
    VALUES %s
    ON CONFLICT (id) DO NOTHING;
    """
    execute_values(cur, insert_sql, rows, page_size=1000)
    conn.commit()

    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} rows into fias_division")

if __name__ == '__main__':
    load_division(FILE, DB_DSN)
