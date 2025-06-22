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

# Регэксп: всё до первой запятой — улица, всё после — дом (VARCHAR(10))
ADDR_RE = re.compile(r'^(?P<street>[^,]+),\s*(?P<house>[^,]+)', re.IGNORECASE)

def truncated_sha256_bytes(s: str) -> bytes:
    """Возвращает первые 16 байт SHA-256 в виде raw bytes"""
    return hashlib.sha256(s.encode('utf-8')).digest()[:16]

def parse_address(addr: str) -> tuple[str | None, str | None]:
    """Парсит адрес в формат (улица, дом)"""
    m = ADDR_RE.match(addr.strip())
    if not m:
        return None, None
    return m.group('street').strip(), m.group('house').strip()

def main() -> None:
    # 1. Подключаемся, выключаем autocommit для единого коммита в конце
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # 2. Создаём справочные таблицы и целевые таблицы, если нужно
    cur.execute("""
    CREATE TABLE IF NOT EXISTS towns (
      town_id   SMALLINT PRIMARY KEY,
      town_name TEXT     UNIQUE NOT NULL
    );
    -- Примеры начальных данных
    INSERT INTO towns(town_id, town_name) VALUES
      (1, 'Москва')
    ON CONFLICT DO NOTHING;

    CREATE TABLE IF NOT EXISTS flats (
      address_hash BYTEA PRIMARY KEY,
      street       TEXT,
      house        VARCHAR(10),
      town         SMALLINT NOT NULL DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS flats_history (
      id                   SERIAL PRIMARY KEY,
      address_hash         BYTEA NOT NULL,
      source               TEXT,
      url                  TEXT,
      person_type_id       SMALLINT,
      price                BIGINT,
      time_source_created  TIMESTAMP,
      time_source_updated  TIMESTAMP,
      recorded_at          TIMESTAMP DEFAULT now()
    );
    """)
    conn.commit()  # фиксируем DDL

    # 3. Загружаем справочник городов в память
    cur.execute("SELECT town_id, town_name FROM towns;")
    town_map = {name.lower(): tid for tid, name in cur.fetchall()}
    default_town_id = town_map.get('москва', 1)

    # 4. Извлекаем объявления
    cur.execute("""
        SELECT address, city, source, url,
               person_type_id, price,
               time_source_created, time_source_updated
        FROM ads
        WHERE address IS NOT NULL;
    """)
    rows = cur.fetchall()

    flats_rows = []
    history_rows = []

    for address, city, source, url, ptid, price, t_created, t_updated in rows:
        # 5a. Определяем town_id через справочник
        raw_city = (city or '').strip().lower()
        town = town_map.get(raw_city, default_town_id)

        # 5b. Парсим адрес
        street, house = parse_address(address)

        # 5c. Хешируем вместе с town
        hash_input = f"{town}|{address}"
        ah = truncated_sha256_bytes(hash_input)

        flats_rows.append((Binary(ah), street, house, town))
        history_rows.append((Binary(ah), source, url, ptid, price, t_created, t_updated))

    # 6. Bulk-вставка в flats с ON CONFLICT DO NOTHING
    execute_values(
        cur,
        """
        INSERT INTO flats(address_hash, street, house, town) VALUES %s
        ON CONFLICT (address_hash) DO NOTHING
        """,
        flats_rows,
        template="(%s, %s, %s, %s)"
    )

    # 7. Bulk-вставка в flats_history
    execute_values(
        cur,
        """
        INSERT INTO flats_history
          (address_hash, source, url, person_type_id, price,
           time_source_created, time_source_updated)
        VALUES %s
        """,
        history_rows,
        template="(%s, %s, %s, %s, %s, %s, %s)"
    )

    # 8. Финальный коммит
    conn.commit()
    cur.close()
    conn.close()

    print(f"Imported {len(flats_rows)} flats and {len(history_rows)} history rows.")

if __name__ == '__main__':
    main()
