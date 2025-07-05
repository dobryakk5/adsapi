import os
import re
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Включаем DEBUG‑логирование
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Синонимы типов улиц
TYPE_SYNONYMS = {
    'просп.': 'пр-кт', 'пр-кт': 'пр-кт',
    'бульвар': 'б-р',   'проезд': 'пр-д',
    'ул.': 'ул',        'улица': 'ул',
    'наб.': 'наб',      'набережная': 'наб',
    'пер.': 'пер',      'пер': 'пер',
    'бул.': 'б-р',
    'ш.': 'ш',          'ш': 'ш',
}

ZELENOGRAD_PARENTOBJID = 1405230

def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def load_lookup_map(cur):
    cur.execute("SELECT id, category, name FROM lookup_types;")
    return {(category, name.lower()): id for id, category, name in cur.fetchall()}

def load_addtype_map(cur):
    cur.execute("SELECT name, id FROM lookup_types WHERE category='addtype';")
    return {name.lower(): id for name, id in cur.fetchall()}

def load_street_types(cur):
    cur.execute("SELECT DISTINCT typename FROM public.fias_objects WHERE typename IS NOT NULL;")
    return {row[0].rstrip('.').lower() for row in cur.fetchall()}

def split_street(full: str, street_types: set[str]) -> tuple[str | None, str]:
    words = full.strip().split()
    if len(words) >= 2:
        def normalize(tok):
            raw = tok.rstrip('.')
            return TYPE_SYNONYMS.get(tok) or TYPE_SYNONYMS.get(raw)

        first = normalize(words[0])
        if first:
            return first, ' '.join(words[1:])
        last = normalize(words[-1])
        if last:
            return last, ' '.join(words[:-1])

        first_raw = words[0].rstrip('.')
        last_raw = words[-1].rstrip('.')
        if first_raw.lower() in street_types:
            return first_raw, ' '.join(words[1:])
        if last_raw.lower() in street_types:
            return last_raw, ' '.join(words[:-1])
    return None, full.strip()

def find_street_objectids(cur, name, typename):
    if typename:
        cur.execute(
            "SELECT objectid FROM public.fias_objects WHERE norm_name=LOWER(%s) AND typename=%s;",
            (name, typename)
        )
    else:
        cur.execute(
            "SELECT objectid FROM public.fias_objects WHERE norm_name=LOWER(%s);",
            (name,)
        )
    return [row[0] for row in cur.fetchall()]

def get_houses_by_parents(cur, parent_ids):
    cur.execute(
        """
        SELECT objectid, housenum, addnum1, addtype1, addnum2, addtype2
        FROM public.fias_houses
        WHERE parentobjid = ANY(%s);
        """,
        (parent_ids,)
    )
    houses = {}
    for objid, housenum, addnum1, addtype1, addnum2, addtype2 in cur.fetchall():
        houses.setdefault(housenum, []).append({
            'objectid': objid,
            'addnum1': str(addnum1) if addnum1 is not None else None,
            'addtype1': addtype1,
            'addnum2': str(addnum2) if addnum2 is not None else None,
            'addtype2': addtype2
        })
    return houses

def parse_and_find_house(cur, street_id, hs_raw, addmap):
    hs = hs_raw.strip().lower()
    logger.debug(f"Parsing house string: '{hs_raw}' (lower: '{hs}') on street_id={street_id}")

    all_houses = get_houses_by_parents(cur, street_id)

    # Поиск позиции первого разделителя из addmap
    addtype_matches = [
        (name, hs.find(name)) for name in addmap.keys()
        if hs.find(name) != -1
    ]
    addtype_matches.sort(key=lambda x: x[1])  # Сортируем по позиции в строке

    if addtype_matches:
        first_addtype, pos = addtype_matches[0]
        prefix = hs[:pos]
        rest = hs[pos:]
    else:
        prefix = hs
        rest = ''

    logger.debug(f"  Parsed prefix: '{prefix}', remaining: '{rest}'")

    candidates = all_houses.get(prefix.upper(), [])
    if not candidates:
        logger.debug(f"  No candidates for prefix '{prefix}'")
        return None

    # Если нет суффиксов — просто возвращаем по префиксу
    if not rest:
        logger.debug(f"  Matched house by prefix only: {prefix}")
        return candidates[0]['objectid']

    def extract_addtype_num(text):
        for name in sorted(addmap.keys(), key=len, reverse=True):
            if text.startswith(name):
                num = text[len(name):]
                if num.isdigit():
                    return addmap[name], num, name
        return None, None, None

    # Разбор add1
    addtype1, addnum1, used_add1 = extract_addtype_num(rest)
    logger.debug(f"  Parsed add1: type_id={addtype1}, num={addnum1}")
    if not addtype1:
        logger.debug(f"  Failed to extract valid addtype1 from '{rest}'")
        return candidates[0]['objectid']

    # Разбор add2, если есть
    rest2 = rest[len(used_add1 + addnum1):] if used_add1 and addnum1 else ''
    addtype2, addnum2, _ = extract_addtype_num(rest2) if rest2 else (None, None, None)
    logger.debug(f"  Parsed add2: type_id={addtype2}, num={addnum2}")

    # Ищем полное совпадение
    for c in candidates:
        ok1 = c['addtype1'] == addtype1 and (c['addnum1'] or '') == addnum1
        ok2 = not addtype2 or (c['addtype2'] == addtype2 and (c['addnum2'] or '') == addnum2)
        if ok1 and ok2:
            logger.debug(f"  Found matching house_id={c['objectid']} with full match")
            return c['objectid']

    logger.debug(f"  No exact match found, fallback to first for prefix '{prefix}'")
    return candidates[0]['objectid']



def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Создание таблиц flats и flats_history
    cur.execute("""
    CREATE TABLE IF NOT EXISTS flats (
        house_id INT NOT NULL,
        floor SMALLINT,
        rooms SMALLINT,
        street TEXT,
        street_type TEXT,
        house TEXT,
        town SMALLINT REFERENCES towns(town_id),
        total_floors SMALLINT,
        area DECIMAL(6,2),
        living_area DECIMAL(6,2),
        kitchen_area DECIMAL(5,2),
        house_type_id SMALLINT,
        PRIMARY KEY (house_id, floor, rooms),
        FOREIGN KEY (house_id)      REFERENCES public.fias_houses(objectid),
        FOREIGN KEY (house_type_id) REFERENCES lookup_types(id)
    );
    CREATE TABLE IF NOT EXISTS flats_history (
        id SERIAL PRIMARY KEY,
        house_id INT NOT NULL,
        floor SMALLINT NOT NULL,
        rooms SMALLINT NOT NULL,
        source SMALLINT,
        object_type SMALLINT,
        ad_type SMALLINT,
        url TEXT,
        person_type_id SMALLINT,
        price BIGINT,
        time_source_created TIMESTAMP,
        time_source_updated TIMESTAMP,
        recorded_at TIMESTAMP DEFAULT now(),
        FOREIGN KEY (house_id,floor,rooms) REFERENCES flats(house_id,floor,rooms),
        FOREIGN KEY (source)      REFERENCES lookup_types(id),
        FOREIGN KEY (object_type) REFERENCES lookup_types(id),
        FOREIGN KEY (ad_type)     REFERENCES lookup_types(id)
    );
    """)
    conn.commit()

    # Загрузка справочников
    addtype_map   = load_addtype_map(cur)
    lookup_map    = load_lookup_map(cur)
    street_types  = load_street_types(cur)

    # Обработка объявлений
    cur.execute("""
        SELECT id, address, city, source, url, person_type_id, price,
               time_source_created, time_source_updated, params
          FROM ads
         WHERE processed IS false
         LIMIT 2;
    """
    )
    rows = cur.fetchall()
    logger.info(f"Total rows to process: {len(rows)}")

    flats_rows, history_rows, processed_ids = [], [], []

    for idx, (ad_id, address, city, src, url, ptype, price, tcr, tup, params) in enumerate(rows, start=1):
        parts = [p.strip() for p in address.split(',') if p.strip()]
        if not parts:
            logger.debug(f"Row {idx}: empty address")
            continue

        if 'зеленоград' in city.lower():
            street_id = ZELENOGRAD_PARENTOBJID
            match_num = re.search(r"(\d+)", parts[0])
            house_str = match_num.group(1) if match_num else parts[0]
            street_name, street_type = None, None
        else:
            street_type, street_name = split_street(parts[0], street_types)
            street_ids = find_street_objectids(cur, street_name, street_type)
            if not street_ids:
                logger.debug(f"Row {idx}: street '{street_name}' not found")
                continue
            house_str = parts[1] if len(parts) > 1 else ''

        house_id = parse_and_find_house(cur, street_ids, house_str, addtype_map)
        if not house_id:
            logger.debug(f"Row {idx}: house '{house_str}' not found on parentobjid {street_id}")
            continue

        floor = to_int(params.get('Этаж')) if params else None
        rooms = to_int(params.get('Количество комнат')) if params else None
        if floor is None or rooms is None:
            logger.debug(f"Row {idx}: invalid floor={floor!r}, rooms={rooms!r}")
            continue

        raw_house_type = (params.get('Тип дома') or '').lower().strip()
        house_type_id  = lookup_map.get(('house_type', raw_house_type))
        if raw_house_type and house_type_id is None:
            logger.warning(f"Row {idx}: house_type '{raw_house_type}' missing in lookup")

        flats_rows.append((
            house_id, floor, rooms,
            street_name, street_type, house_str,
            1,
            to_int(params.get('Этажей в доме')) if params else None,
            float(params.get('Площадь'))       if params and params.get('Площадь')       else None,
            float(params.get('Жилая площадь')) if params and params.get('Жилая площадь') else None,
            float(params.get('Площадь кухни')) if params and params.get('Площадь кухни') else None,
            house_type_id
        ))
        history_rows.append((
            house_id, floor, rooms,
            lookup_map.get(('source_id', src.lower())),
            lookup_map.get(('object_type', (params.get('Вид объекта') or '').lower())) if params else None,
            lookup_map.get(('ad_type', (params.get('Тип объявления') or '').lower())) if params else None,
            url, ptype, price, tcr, tup
        ))
        processed_ids.append(ad_id)

    if flats_rows:
        execute_values(cur, """
            INSERT INTO flats (
                house_id, floor, rooms, street, street_type, house,
                town, total_floors, area, living_area, kitchen_area, house_type_id
            ) VALUES %s ON CONFLICT DO NOTHING;
        """, flats_rows)
    if history_rows:
        execute_values(cur, """
            INSERT INTO flats_history (
                house_id, floor, rooms, source, object_type, ad_type,
                url, person_type_id, price, time_source_created, time_source_updated
            ) VALUES %s;
        """, history_rows)
    if processed_ids:
        cur.execute("UPDATE ads SET processed = TRUE WHERE id = ANY(%s);", (processed_ids,))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
