import os
import re
import hashlib
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Binary
from psycopg2.extras import execute_values

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Загрузка .env и установка DATABASE_URL
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Регулярное выражение для разбора номера дома: цифры и слэши в основной части
PART_PATTERN = re.compile(r"([0-9/]+)(?:([/А-Яа-яA-Za-z])(.*))?")

# Сопоставление сокращений типов улиц с FIAS-typename
TYPE_SYNONYMS = {
    'просп.': 'пр-кт',
    'пр-кт': 'пр-кт',
    'бульвар': 'б-р',
    'б-р': 'б-р',
    'ул.': 'ул',
    'улица': 'ул',
    'наб.': 'наб',
    'набережная': 'наб',
    # добавьте другие по необходимости
}

def load_addtype_map(cur):
    cur.execute("SELECT name, id FROM lookup_types WHERE category = 'addtype';")
    return {name.lower(): id_ for name, id_ in cur.fetchall()}

def load_street_types(cur):
    cur.execute("""
      CREATE TABLE IF NOT EXISTS street_types (
        typename TEXT PRIMARY KEY
      );
      INSERT INTO street_types(typename)
        SELECT DISTINCT typename FROM public.fias_objects
        WHERE typename IS NOT NULL
      ON CONFLICT DO NOTHING;
    """)
    cur.execute("SELECT typename FROM street_types;")
    return {row[0] for row in cur.fetchall()}

def split_street(full: str, street_types: set[str]) -> tuple[str|None, str]:
    parts = full.strip().split(" ", 1)
    if len(parts) == 2:
        a, b = parts
        norm = TYPE_SYNONYMS.get(a.lower())
        if norm and norm in street_types:
            return norm, b
        norm_b = TYPE_SYNONYMS.get(b.lower())
        if norm_b and norm_b in street_types:
            return norm_b, a
    return None, full.strip()

def find_street_objectid(cur, street_name: str, street_type: str | None) -> int | None:
    if street_type:
        street_type = street_type.rstrip('.')
        cur.execute(
            "SELECT objectid FROM public.fias_objects WHERE LOWER(name)=LOWER(%s) AND typename=%s LIMIT 1;",
            (street_name, street_type)
        )
    else:
        cur.execute(
            "SELECT objectid FROM public.fias_objects WHERE LOWER(name)=LOWER(%s) LIMIT 1;",
            (street_name,)
        )
    row = cur.fetchone()
    return row[0] if row else None

def get_houses_by_parent(cur, parentobjid: int) -> dict[str, list[dict]]:
    cur.execute(
        "SELECT objectid, housenum, addnum1, addtype1, addnum2, addtype2 "
        "FROM public.fias_houses WHERE parentobjid = %s;",
        (parentobjid,)
    )
    houses = {}
    for objid, housenum, addnum1, addtype1, addnum2, addtype2 in cur.fetchall():
        houses.setdefault(housenum, []).append({
            'objectid': objid,
            'addnum1': str(addnum1) if addnum1 is not None else None,
            'addtype1': addtype1,
            'addnum2': str(addnum2) if addnum2 is not None else None,
            'addtype2': addtype2,
        })
    return houses

def parse_and_find_house(cur, street_id: int, house_str: str, addtype_map: dict) -> int | None:
    m = PART_PATTERN.match(house_str)
    if not m:
        logger.debug(f"House parse failed regex: '{house_str}'")
        return None
    housenum, sep1, rest = m.group(1), m.group(2), m.group(3) or ''

    houses = get_houses_by_parent(cur, street_id)
    candidates = houses.get(housenum, [])
    if not candidates:
        logger.debug(f"No candidates for housenum '{housenum}'")
        return None
    if not sep1:
        return candidates[0]['objectid']

    addtype1 = addtype_map.get(sep1.lower())
    m2 = PART_PATTERN.match(rest)
    if not m2:
        logger.debug(f"Failed second match for rest '{rest}'")
        return None
    addnum1, sep2, rest2 = m2.group(1), m2.group(2), m2.group(3) or ''
    sub = [c for c in candidates if c['addtype1']==addtype1 and c['addnum1']==addnum1]
    if not sep2:
        return sub[0]['objectid'] if sub else None

    addtype2 = addtype_map.get(sep2.lower())
    m3 = re.match(r"(\d+)", rest2)
    if not m3:
        logger.debug(f"Third part not numeric: '{rest2}'")
        return None
    addnum2 = m3.group(1)
    sub2 = [c for c in sub if c['addtype2']==addtype2 and c['addnum2']==addnum2]
    return sub2[0]['objectid'] if sub2 else None

def truncated_sha256_bytes(s: str) -> bytes:
    return hashlib.sha256(s.encode('utf-8')).digest()[:16]

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
    # Проверка индекса
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_fias_houses_objectid ON public.fias_houses(objectid);")
    conn.commit()

    # Создание целевых таблиц (если нужно)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS flats (
      address_hash BYTEA PRIMARY KEY,
      street TEXT,
      street_type TEXT,
      house TEXT,
      house_id INT NOT NULL,
      town SMALLINT NOT NULL REFERENCES towns(town_id),
      floor INTEGER,
      total_floors SMALLINT,
      area DECIMAL(6,2),
      living_area DECIMAL(6,2),
      kitchen_area DECIMAL(5,2),
      house_type_id SMALLINT,
      house_type_cat TEXT NOT NULL DEFAULT 'house_type',
      rooms SMALLINT,
      CONSTRAINT fk_house FOREIGN KEY (house_id) REFERENCES public.fias_houses(objectid),
      CONSTRAINT fk_house_type FOREIGN KEY (house_type_id, house_type_cat)
        REFERENCES lookup_types(id, category)
    );
    CREATE TABLE IF NOT EXISTS flats_history (
      id SERIAL PRIMARY KEY,
      address_hash BYTEA NOT NULL REFERENCES flats(address_hash),
      source SMALLINT,
      source_cat TEXT NOT NULL DEFAULT 'source_id',
      object_type_id SMALLINT,
      object_type_cat TEXT NOT NULL DEFAULT 'object_type',
      ad_type_id SMALLINT,
      ad_type_cat TEXT NOT NULL DEFAULT 'ad_type',
      url TEXT,
      person_type_id SMALLINT,
      price BIGINT,
      time_source_created TIMESTAMP,
      time_source_updated TIMESTAMP,
      recorded_at TIMESTAMP DEFAULT now(),
      CONSTRAINT fk_source FOREIGN KEY (source, source_cat)
        REFERENCES lookup_types(id, category),
      CONSTRAINT fk_object_type FOREIGN KEY (object_type_id, object_type_cat)
        REFERENCES lookup_types(id, category),
      CONSTRAINT fk_ad_type FOREIGN KEY (ad_type_id, ad_type_cat)
        REFERENCES lookup_types(id, category)
    );
    """)
    conn.commit()

    addtype_map = load_addtype_map(cur)
    street_types = load_street_types(cur)

    flats_rows = []
    history_rows = []

    # Отладочные счётчики
    total = skipped_street = skipped_sid = skipped_house = succeeded = 0

    cur.execute("""
        SELECT address, city, source, url,
               person_type_id, price,
               time_source_created, time_source_updated,
               params
        FROM ads
        WHERE address IS NOT NULL AND processed IS TRUE;
    """)
    for total, row in enumerate(cur.fetchall(), start=1):
        address, city, source_txt, url, ptid, price, t_created, t_updated, params = row
        parts = [p.strip() for p in address.split(',')]
        st_type, street_text = split_street(parts[0], street_types)
        street_id = find_street_objectid(cur, street_text, st_type)
        if not street_id:
            skipped_sid += 1
            logger.debug(f"Row {total}: street not found '{street_text}' ({st_type})")
            continue
        house_text = parts[1] if len(parts) > 1 else ''
        house_id = parse_and_find_house(cur, street_id, house_text, addtype_map)
        if not house_id:
            skipped_house += 1
            logger.debug(f"Row {total}: house not found '{house_text}' on street_id {street_id}")
            continue

        # Формируем поля
        floor = params.get('Этаж') if params else None
        total_floors = params.get('Этажей в доме') if params else None
        area = float(params.get('Площадь')) if params and params.get('Площадь') else None
        living = float(params.get('Жилая площадь')) if params and params.get('Жилая площадь') else None
        kitchen = float(params.get('Площадь кухни')) if params and params.get('Площадь кухни') else None
        rooms = parse_rooms(params.get('Количество комнат') if params else None)

        ah = truncated_sha256_bytes(f"{house_id}|{floor}|{rooms}")

        flats_rows.append((
            Binary(ah),
            street_text, st_type, house_text,
            house_id,  # справочный ключ
            # town: можно парсить из city или использовать дефолт
            1,
            floor, total_floors,
            area, living, kitchen,
            # house_type_id и house_type_cat
            # например:
            addtype_map.get((params or {}).get('Тип дома', '').lower()),
            rooms
        ))
        history_rows.append((
            Binary(ah),
            # source / source_cat
            # lookup_map надо загрузить отдельно аналогично addtype_map
            # например source_id = lookup_map[('source_id', source_txt)]
            None,  # source
            None,  # object_type_id
            None,  # ad_type_id
            url, ptid, price,
            t_created, t_updated
        ))
        succeeded += 1

    # Вставляем пачками
    if flats_rows:
        execute_values(cur, """
            INSERT INTO flats (
              address_hash, street, street_type, house, house_id,
              town, floor, total_floors,
              area, living_area, kitchen_area,
              house_type_id, rooms
            ) VALUES %s ON CONFLICT DO NOTHING;
        """, flats_rows)

    if history_rows:
        execute_values(cur, """
            INSERT INTO flats_history (
              address_hash, source, object_type_id, ad_type_id,
              url, person_type_id, price,
              time_source_created, time_source_updated
            ) VALUES %s;
        """, history_rows)

    conn.commit()
    logger.info(f"Total: {total}, Succeeded: {succeeded}, Skipped street: {skipped_sid}, Skipped house: {skipped_house}")

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
