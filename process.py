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
    'просп.': 'пр-кт', 'пр-кт': 'пр-кт', 'пр.': 'пр-кт',
    'бульвар': 'б-р', 'бул.': 'б-р', 'проезд': 'пр-д',
    'ул.': 'ул', 'улица': 'ул', 'пос.': 'п', 
    'наб.': 'наб', 'набережная': 'наб',
    'пер.': 'пер', 'пер': 'пер',
    'ш.': 'ш', 'ш': 'ш', 'шоссе': 'ш',
}
ZELENOGRAD_PARENTOBJID = 1405230

def to_int(v):
    try:
        return int(v)
    except:
        return None

def get_built_year(params2):
    try:
        return to_int(params2.get('О здании', {}).get('Год постройки'))
    except:
        return None

def load_lookup_map(cur):
    cur.execute("SELECT id, category, name FROM lookup_types;")
    return {(cat, name.lower()): id for id, cat, name in cur.fetchall()}

def load_addtype_map(cur):
    cur.execute("SELECT name, id FROM lookup_types WHERE category='addtype';")
    return {name.lower(): id for name, id in cur.fetchall()}

def load_street_types(cur):
    cur.execute("SELECT DISTINCT typename FROM public.fias_objects WHERE typename IS NOT NULL;")
    return {r[0].rstrip('.').lower() for r in cur.fetchall()}

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
    name_norm = name.lower().replace('ё', 'е')
    if typename:
        sql = "SELECT objectid FROM public.fias_objects WHERE norm_name LIKE LOWER(%s) || '%%' AND typename=%s;"
        params = (name_norm, typename)
    else:
        sql = "SELECT objectid FROM public.fias_objects WHERE norm_name LIKE LOWER(%s) || '%%';"
        params = (name_norm,)
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]

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
    for obj, num, a1, t1, a2, t2 in cur.fetchall():
        key = num.upper()
        houses.setdefault(key, []).append({'objectid': obj, 'addnum1': str(a1) if a1 else None,
            'addtype1': t1, 'addnum2': str(a2) if a2 else None, 'addtype2': t2})
    return houses

def extract_addtype_num(text, addmap):
    for name in sorted(addmap, key=len, reverse=True):
        if text.startswith(name):
            num = text[len(name):]
            if num.isdigit(): return addmap[name], num, name
    return None, None, None

def parse_and_find_house(cur, street_ids, hs_raw, addmap):
    hs = hs_raw.strip().lower()
    houses_map = get_houses_by_parents(cur, street_ids)
    for add, pos in sorted(((n, hs.find(n)) for n in addmap if n in hs), key=lambda x: x[1]):
        prefix, rest = hs[:pos], hs[pos:]
        break
    else:
        prefix, rest = hs, ''
    candidates = houses_map.get(prefix.upper(), [])
    if not candidates: return None
    if not rest:
        for c in candidates:
            if c['addnum1'] is None and c['addnum2'] is None:
                return c['objectid']
        return candidates[0]['objectid']
    a1, n1, _ = extract_addtype_num(rest, addmap)
    rem = rest[len(n1 or ''):]
    if not rem:
        for c in candidates:
            if c['addtype1']==a1 and (c['addnum1'] or '')==n1 and c['addnum2'] is None:
                return c['objectid']
        return candidates[0]['objectid']
    a2, n2, _ = extract_addtype_num(rem, addmap)
    for c in candidates:
        if c['addtype1']==a1 and (c['addnum1'] or '')==n1 and a2 and c['addtype2']==a2 and (c['addnum2'] or '')==n2:
            return c['objectid']
    return candidates[0]['objectid']

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    addmap = load_addtype_map(cur)
    lookup_map = load_lookup_map(cur)
    street_types = load_street_types(cur)
    ao_map = load_ao_map(cur)

    cur.execute("""
    SELECT id, address, city, district_only,
           source_id, url, person_type_id, price,
           time_source_created, time_source_updated,
           params, params2, is_actual, avitoid,
           nedvigimost_type_id, description
      FROM ads WHERE processed IS FALSE LIMIT 50;
    """
    )
    rows = cur.fetchall()
    logger.info(f"Processing {len(rows)} rows")

    flats_rows, history_rows, processed_ids = [], [], []
    for idx, (ad_id, address, city, district,
              source_id, url, ptype, price,
              tcr, tup, params, params2,
              is_act, avitoid, nedvig_type_id, description) in enumerate(rows, 1):

        parts = [p.strip() for p in address.split(',') if p.strip()]
        if len(parts) >= 2:
            street_part, house_str = parts[-2], parts[-1]
        else:
            street_part, house_str = parts[0], ''

        if 'зеленоград' in city.lower():
            street_ids = [ZELENOGRAD_PARENTOBJID]
            m = re.search(r"(\d+)", street_part)
            house_str = m.group(1) if m else street_part
        else:
            street_type, street_name = split_street(street_part, street_types)
            if street_name is None:
                logger.debug(f"Row {idx}: street not parsed from '{street_part}'")
                cur.execute("UPDATE ads SET processed = NULL WHERE id = %s;", (ad_id,))
                continue
            street_ids = find_street_objectids(cur, street_name, street_type)
            if not street_ids:
                logger.debug(f"Row {idx}: no FIAS objectids for '{street_name}' (type='{street_type}')")
                cur.execute("UPDATE ads SET processed = NULL WHERE id = %s;", (ad_id,))
                continue

        house_id = parse_and_find_house(cur, street_ids, house_str, addmap)
        if not house_id:
            logger.debug(f"Row {idx}: house '{house_str}' not found on {street_ids}")
            continue

        floor = to_int(params.get('Этаж')) if params else None
        rooms_raw = params.get('Количество комнат') if params else None
        if rooms_raw and isinstance(rooms_raw, str) and rooms_raw.lower() == 'студия':
            rooms = 0
        else:
            rooms = to_int(rooms_raw)
        if floor is None or rooms is None:
            logger.debug(f"Row {idx}: missing floor or rooms (Этаж={params.get('Этаж')}, Комнат={params.get('Количество комнат')})")
            cur.execute("UPDATE ads SET processed = NULL WHERE id = %s;", (ad_id,))
            continue

        flats_rows.append((
            house_id, floor, rooms,
            street_name, street_type, house_str,
            1, to_int(params.get('Этажей в доме')),
            float(params.get('Площадь') or 0),
            float(params.get('Жилая площадь') or 0),
            float(params.get('Площадь кухни') or 0),
            lookup_map.get(('house_type', (params.get('Тип дома') or '').lower())),
            ao_map.get((district or '').lower()),
            get_built_year(params2)
        ))

        history_rows.append((
            house_id, floor, rooms,
            street_name, street_type, house_str,
            1, to_int(params.get('Этажей в доме')),
            float(params.get('Площадь') or 0),
            float(params.get('Жилая площадь') or 0),
            float(params.get('Площадь кухни') or 0),
            lookup_map.get(('house_type', (params.get('Тип дома') or '').lower())),
            ao_map.get((district or '').lower()),
            get_built_year(params2),
            source_id,
            lookup_map.get(('object_type', (params.get('Вид объекта') or '').lower())),
            nedvig_type_id,
            url, ptype, price,
            tcr, tup, avitoid, is_act, description
        ))
        processed_ids.append(ad_id)

    if history_rows:
        execute_values(cur,
            """
            CREATE TEMP TABLE IF NOT EXISTS tmp_flats_history (
              house_id integer, floor smallint, rooms smallint,
              street text, street_type varchar(9), house varchar(8),
              town smallint, total_floors smallint,
              area numeric, living_area numeric, kitchen_area numeric,
              house_type_id smallint, ao_id smallint, built smallint,
              source_id smallint, object_type smallint, nedvigimost_type_id smallint,
              url text, person_type_id smallint, price numeric,
              time_source_created date, time_source_updated timestamp,
              avitoid bigint, is_actual smallint, description text
            ) ON COMMIT DROP;
            INSERT INTO tmp_flats_history VALUES %s;
            """,
            history_rows
        )
        cur.execute("CALL batch_upsert_flats_and_history();")

    if processed_ids:
        cur.execute("UPDATE ads SET processed=TRUE WHERE id=ANY(%s);", (processed_ids,))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
