import os
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# Параметры подключения к БД из переменной окружения
DB_DSN = os.getenv("DATABASE_URL")
# Путь к XML-файлу
FILE = os.path.join(
    "/Users/pavellebedev/Downloads/fias_xml/77",
    "AS_ADDR_OBJ_20250626_06fae588-3bf8-479b-bf63-8675b28bd808.XML"
)

# DDL для создания таблицы со всеми полями из XML
DDL = '''
CREATE TABLE IF NOT EXISTS fias_objects (
    id              BIGINT PRIMARY KEY,        -- ATTRIBUTE ID
    objectid        BIGINT NOT NULL,
    objectguid      UUID NOT NULL,
    changeid        BIGINT,
    name            TEXT,
    typename        TEXT,
    level           INTEGER,
    opertypeid      INTEGER,
    previd          BIGINT,
    nextid          BIGINT,
    updatedate      DATE,
    startdate       DATE,
    enddate         DATE,
    isactual        BOOLEAN,
    isactive        BOOLEAN
);
'''

def load_fias_to_db(xml_path: str, dsn: str):
    # Подключаемся к БД
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    # Создание таблицы при необходимости
    cur.execute(DDL)
    conn.commit()

    # Парсинг XML-файла
    tree = ET.parse(xml_path)
    root = tree.getroot()

    rows = []
    for obj in root.findall('.//OBJECT'):
        rows.append(
            (
                int(obj.get('ID')),
                int(obj.get('OBJECTID')),
                obj.get('OBJECTGUID'),
                int(obj.get('CHANGEID')) if obj.get('CHANGEID') else None,
                obj.get('NAME'),
                obj.get('TYPENAME'),
                int(obj.get('LEVEL')),
                int(obj.get('OPERTYPEID')) if obj.get('OPERTYPEID') else None,
                int(obj.get('PREVID')) if obj.get('PREVID') and obj.get('PREVID') != '0' else None,
                int(obj.get('NEXTID')) if obj.get('NEXTID') and obj.get('NEXTID') != '0' else None,
                obj.get('UPDATEDATE'),
                obj.get('STARTDATE'),
                obj.get('ENDDATE'),
                obj.get('ISACTUAL') == '1',
                obj.get('ISACTIVE') == '1'
            )
        )

    # Вставка данных пакетно
    insert_sql = '''
    INSERT INTO fias_objects (
        id, objectid, objectguid, changeid, name, typename,
        level, opertypeid, previd, nextid,
        updatedate, startdate, enddate, isactual, isactive
    ) VALUES %s
    ON CONFLICT (id) DO NOTHING;
    '''
    execute_values(cur, insert_sql, rows, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Загружено {len(rows)} объектов в таблицу fias_objects")

if __name__ == '__main__':
    load_fias_to_db(FILE, DB_DSN)
