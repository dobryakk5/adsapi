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

# Синонимы типов улиц: ключи — возможные входы, значения — канонические коды
TYPE_SYNONYMS = {
    'просп.': 'пр-кт', 'пр-кт': 'пр-кт',
    'бульвар': 'б-р',   'проезд': 'пр-д',
    'ул.': 'ул',        'улица': 'ул',
    'наб.': 'наб',      'набережная': 'наб',
    'пер.': 'пер',      'пер': 'пер',
    'бул.': 'б-р',
    'ш.': 'ш',          'ш': 'ш',
}

# Специальный parentobjid для Зеленограда
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

def load_ao_map(cur):
    cur.execute("SELECT id, admin_okrug FROM public.districts;")
    return {okrug.lower(): id for id, okrug in cur.fetchall()}

def split_street(full, street_types):
    words = full.strip().split()
    if len(words) >= 2:
        def lookup_type(tok):
            raw = tok.lower()
            stripped = raw.rstrip('.')
            return TYPE_SYNONYMS.get(raw) or TYPE_SYNONYMS.get(stripped)

        for tok in (words[0], words[-1]):
            t = lookup_type(tok)
            if t:
                parts = words[1:] if tok == words[0] else words[:-1]
                return t, ' '.join(parts)
        for tok, rest in ((words[0], words[1:]), (words[-1], words[:-1])):
            stripped = tok.rstrip('.').lower()
            if stripped in street_types:
                return stripped, ' '.join(rest)
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

def get_houses_by_parents(cur, parentobjids):
    cur.execute(
        """
        SELECT objectid, housenum, addnum1, addtype1, addnum2, addtype2
          FROM public.fias_houses
         WHERE parentobjid = ANY(%s);
        """,
        (parentobjids,)
    )
    houses = {}
    for objid, housenum, addnum1, addtype1, addnum2, addtype2 in cur.fetchall():
        key = housenum.upper()
        houses.setdefault(key, []).append({
            'objectid': objid,
            'addnum1': str(addnum1) if addnum1 is not None else None,
            'addtype1': addtype1,
            'addnum2': str(addnum2) if addnum2 is not None else None,
            'addtype2': addtype2
        })
    return houses

def extract_addtype_num(text, addmap):
    for name in sorted(addmap.keys(), key=len, reverse=True):
        if text.startswith(name):
            num = text[len(name):]
            if num.isdigit():
                return addmap[name], num, name
    return None, None, None

def parse_and_find_house(cur, street_ids, hs_raw, addmap):
    hs = hs_raw.strip().lower()
    logger.debug(f"Parsing house string: '{hs_raw}' -> '{hs}' on street_ids={street_ids}")
    houses_map = get_houses_by_parents(cur, street_ids)
    matches = [(name, hs.find(name)) for name in addmap if hs.find(name) != -1]
    matches.sort(key=lambda x: x[1])
    if matches:
        first_add, pos = matches[0]
        prefix = hs[:pos]
        rest = hs[pos:]
    else:
        prefix, rest = hs, ''
    logger.debug(f"  prefix='{prefix}', rest='{rest}'")
    candidates = houses_map.get(prefix.upper(), [])
    logger.debug(f"  {len(candidates)} candidates for prefix '{prefix}'")
    if not candidates:
        return None
    if not rest:
        for c in candidates:
            if c['addnum1'] is None and c['addnum2'] is None:
                logger.debug(f"  Selected by prefix only -> {c['objectid']}")
                return c['objectid']
        return candidates[0]['objectid']
    add1_id, num1, used1 = extract_addtype_num(rest, addmap)
    rest2 = rest[len(used1) + len(num1):] if used1 and num1 else ''
    if not rest2:
        for c in candidates:
            if c['addtype1'] == add1_id and (c['addnum1'] or '') == num1 and c['addnum2'] is None:
                logger.debug(f"  Selected by prefix+add1 -> {c['objectid']}")
                return c['objectid']
        return candidates[0]['objectid']
    add2_id, num2, used2 = extract_addtype_num(rest2, addmap)
    for c in candidates:
        if (c['addtype1'] == add1_id and (c['addnum1'] or '') == num1 and
            add2_id and c['addtype2'] == add2_id and (c['addnum2'] or '') == num2):
            logger.debug(f"  Full match -> {c['objectid']}")
            return c['objectid']
    return candidates[0]['objectid']

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Добавить новые колонки
    cur.execute("ALTER TABLE flats ADD COLUMN IF NOT EXISTS ao_id SMALLINT;")
    cur.execute("ALTER TABLE flats_history ADD COLUMN IF NOT EXISTS is_actual SMALLINT;")
    conn.commit()

    addmap = load_addtype_map(cur)
    lookup_map = load_lookup_map(cur)
    street_t = load_street_types(cur)
    ao_map = load_ao_map(cur)

    # Изменено: читаем is_actual из ads
    cur.execute("""
    SELECT id, address, city, district_only, source, url, person_type_id, price,
           time_source_created, time_source_updated, params, is_actual
      FROM ads WHERE processed IS FALSE LIMIT 10;
    """
    )
    rows = cur.fetchall()
    logger.info(f"Total rows to process: {len(rows)}")

    flats_rows, history_rows, processed_ids = [], [], []
    for idx, (ad_id, address, city, district, src, url, ptype, price, tcr, tup, params, is_act) in enumerate(rows, 1):
        parts = [p.strip() for p in address.split(',') if p.strip()]
        if not parts:
            continue
        if 'зеленоград' in city.lower():
            street_ids = [ZELENOGRAD_PARENTOBJID]
            match = re.search(r"(\d+)", parts[0])
            house_str = match.group(1) if match else parts[0]
            street_name = street_type = None
        else:
            street_type, street_name = split_street(parts[0], street_t)
            street_ids = find_street_objectids(cur, street_name, street_type)
            if not street_ids:
                continue
            house_str = parts[1] if len(parts) > 1 else ''

            house_id = parse_and_find_house(cur, street_ids, house_str, addmap)
        if not house_id:
            # Не удалось определить дом — отмечаем запись processed=NULL
            cur.execute("UPDATE ads SET processed = NULL WHERE id = %s;", (ad_id,))
            logger.debug(f"Row {idx}: house '{house_str}' not found, setting processed=NULL for ad {ad_id}")
            continue

        floor = to_int(params.get('Этаж')) if params else None
        rooms = to_int(params.get('Количество комнат')) if params else None
        if floor is None or rooms is None:
            continue

        raw_ht = (params.get('Тип дома') or '').lower().strip()
        htype_id = lookup_map.get(('house_type', raw_ht))
        ao_id = ao_map.get((district or '').lower())

        flats_rows.append((
            house_id, floor, rooms, street_name, street_type, house_str, 1,
            to_int(params.get('Этажей в доме')), float(params.get('Площадь') or 0),
            float(params.get('Жилая площадь') or 0), float(params.get('Площадь кухни') or 0),
            htype_id, ao_id
        ))
        history_rows.append((
            house_id, floor, rooms,
            lookup_map.get(('source_id', src.lower())),
            lookup_map.get(('object_type', (params.get('Вид объекта') or '').lower())),
            lookup_map.get(('ad_type', (params.get('Тип объявления') or '').lower())),
            url, ptype, price, tcr, tup, is_act
        ))
        processed_ids.append(ad_id)

    if flats_rows:
        execute_values(cur,
            "INSERT INTO flats(house_id,floor,rooms,street,street_type,house,town,total_floors,area,living_area,kitchen_area,house_type_id,ao_id) VALUES %s ON CONFLICT DO NOTHING;",
            flats_rows
        )
    if history_rows:
        execute_values(cur,
            "INSERT INTO flats_history(house_id,floor,rooms,source,object_type,ad_type,url,person_type_id,price,time_source_created,time_source_updated,is_actual) VALUES %s;",
            history_rows
        )
    if processed_ids:
        cur.execute("UPDATE ads SET processed=TRUE WHERE id=ANY(%s);", (processed_ids,))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
