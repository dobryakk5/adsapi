import os
import re
import hashlib
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Binary
from psycopg2.extras import execute_values

# Загрузка .env и установка DATABASE_URL
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Парсинг адреса: если после первой запятой текст длиннее 10 символов,
# то считаем, что street = всё до второй запятой, а house = текст после второй запятой
# Иначе street = всё до первой запятой, house = после первой

def parse_address(addr: str) -> tuple[str | None, str | None]:
    parts = [p.strip() for p in addr.split(',')]
    candidate = None
    # определяем кандидата для house
    if len(parts) >= 3:
        candidate = parts[2]
    elif len(parts) == 2:
        candidate = parts[1]
    # если candidate валиден и <=10 символов, разбиваем на street/house
    if candidate and len(candidate) <= 10:
        # street = всё до последней запятой
        street = addr.rsplit(',', maxsplit=1)[0].strip()
        return street, candidate
    # во всех остальных случаях считаем весь addr улицей
    return addr.strip(), None

def truncated_sha256_bytes(s: str) -> bytes:
    """Возвращает первые 16 байт SHA-256 в виде raw bytes"""
    return hashlib.sha256(s.encode('utf-8')).digest()[:16]

# Преобразует количество комнат; студия -> 0, иначе int
def parse_rooms(val) -> int | None:
    if val is None:
        return None
    if isinstance(val, str) and val.strip().lower() == 'студия':
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # Создание таблиц (если не созданы) и обеспечение PK для lookup_types
    cur.execute("""
    -- Towns
    CREATE TABLE IF NOT EXISTS towns (
      town_id   SMALLINT PRIMARY KEY,
      town_name TEXT     UNIQUE NOT NULL
    );
    INSERT INTO towns(town_id, town_name) VALUES (1, 'Москва') ON CONFLICT DO NOTHING;

    -- Lookup types
    CREATE TABLE IF NOT EXISTS lookup_types (
      id       SMALLINT,
      category TEXT     NOT NULL,
      name     TEXT     NOT NULL,
      UNIQUE(category, name)
    );
    DO $$BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'lookup_types'::regclass
          AND contype = 'p'
      ) THEN
        ALTER TABLE lookup_types ADD CONSTRAINT lookup_types_pkey PRIMARY KEY (id);
      END IF;
    END$$;

    -- Flats
    CREATE TABLE IF NOT EXISTS flats (
      address_hash    BYTEA PRIMARY KEY,
      street          TEXT,
      house           VARCHAR(10),
      town            SMALLINT     NOT NULL REFERENCES towns(town_id),
      floor           INTEGER,
      total_floors    SMALLINT,
      area            DECIMAL(6,2),
      living_area     DECIMAL(6,2),
      kitchen_area    DECIMAL(5,2),
      house_type_id   SMALLINT     REFERENCES lookup_types(id),
      rooms           SMALLINT
    );

    -- Flats history
    CREATE TABLE IF NOT EXISTS flats_history (
      id                   SERIAL PRIMARY KEY,
      address_hash         BYTEA NOT NULL REFERENCES flats(address_hash),
      source               SMALLINT     REFERENCES lookup_types(id),
      object_type_id       SMALLINT     REFERENCES lookup_types(id),
      ad_type_id           SMALLINT     REFERENCES lookup_types(id),
      url                  TEXT,
      person_type_id       SMALLINT,
      price                BIGINT,
      time_source_created  TIMESTAMP,
      time_source_updated  TIMESTAMP,
      recorded_at          TIMESTAMP DEFAULT now()
    );
    """)
    conn.commit()

    # Загрузка справочников
    cur.execute("SELECT town_id, town_name FROM towns;")
    town_map = {name.lower(): tid for tid, name in cur.fetchall()}
    default_town = town_map.get('москва', 1)

    cur.execute("SELECT id, category, name FROM lookup_types;")
    lookup_map = {(row[1], row[2]): row[0] for row in cur.fetchall()}

    # Чтение объявлений
    cur.execute("""
        SELECT address, city, source, url,
               person_type_id, price,
               time_source_created, time_source_updated,
               params
        FROM ads WHERE address IS NOT NULL;
    """)
    rows = cur.fetchall()

    flats_rows = []
    history_rows = []

    for address, city, source_txt, url, ptid, price, t_created, t_updated, params in rows:
        town = town_map.get((city or '').strip().lower(), default_town)
        street, house = parse_address(address)

        # Извлечение и преобразование полей из JSON params
        floor = params.get('Этаж') if params else None
        total_floors = params.get('Этажей в доме') if params else None
        area = float(params.get('Площадь')) if params and params.get('Площадь') else None
        living = float(params.get('Жилая площадь')) if params and params.get('Жилая площадь') else None
        kitchen = float(params.get('Площадь кухни')) if params and params.get('Площадь кухни') else None
        rooms = parse_rooms(params.get('Количество комнат') if params else None)
        house_txt = params.get('Тип дома') if params else None
        object_txt = params.get('Вид объекта') if params else None
        ad_txt = params.get('Тип объявления') if params else None

        # Получение lookup id
        house_type_id = lookup_map.get(('house_type', house_txt))
        object_type_id = lookup_map.get(('object_type', object_txt))
        ad_type_id = lookup_map.get(('ad_type', ad_txt))
        source_id = lookup_map.get(('source_id', source_txt))

        # Хеш адреса
        ah = truncated_sha256_bytes(f"{town}|{address}")

        flats_rows.append((Binary(ah), street, house, town,
                            floor, total_floors, area, living, kitchen,
                            house_type_id, rooms))
        history_rows.append((Binary(ah), source_id,
                             object_type_id, ad_type_id,
                             url, ptid, price, t_created, t_updated))

    # Вставка в flats
    execute_values(cur,
        """
        INSERT INTO flats(
          address_hash, street, house, town,
          floor, total_floors, area, living_area, kitchen_area,
          house_type_id, rooms
        ) VALUES %s ON CONFLICT (address_hash) DO NOTHING
        """,
        flats_rows,
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    # Вставка в flats_history
    execute_values(cur,
        """
        INSERT INTO flats_history
          (address_hash, source, object_type_id, ad_type_id,
           url, person_type_id, price,
           time_source_created, time_source_updated)
        VALUES %s
        """,
        history_rows,
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Imported {len(flats_rows)} flats and {len(history_rows)} history rows.")

if __name__ == '__main__':
    main()
