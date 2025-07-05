import os
import re
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Включаем DEBUG‑логирование
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

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
    except:
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

# Парсинг улицы: проверяем первый и последний токен
# Учитываем ключи как со точкой, так и без

def split_street(full, street_types):
    words = full.strip().split()
    if len(words) >= 2:
        # Функция для получения возможного значения из TYPE_SYNONYMS
        def lookup_type(tok):
            raw = tok.lower()
            stripped = raw.rstrip('.')
            if raw in TYPE_SYNONYMS:
                return TYPE_SYNONYMS[raw]
            if stripped in TYPE_SYNONYMS:
                return TYPE_SYNONYMS[stripped]
            return None

        # Проверяем первый токен
        first = words[0]
        t = lookup_type(first)
        if t:
            return t, ' '.join(words[1:])
        # Проверяем последний токен
        last = words[-1]
        t = lookup_type(last)
        if t:
            return t, ' '.join(words[:-1])
        # Динамические street_types (без точки)
        first_stripped = words[0].rstrip('.').lower()
        if first_stripped in street_types:
            return first_stripped, ' '.join(words[1:])
        last_stripped = words[-1].rstrip('.').lower()
        if last_stripped in street_types:
            return last_stripped, ' '.join(words[:-1])
    # Не удалось определить тип
    return None, full.strip()

def find_street_objectid(cur, name, typename):
    if typename:
        cur.execute(
            "SELECT objectid FROM public.fias_objects "
            "WHERE norm_name=LOWER(%s) AND typename=%s LIMIT 1;",
            (name, typename)
        )
    else:
        cur.execute(
            "SELECT objectid FROM public.fias_objects "
            "WHERE norm_name=LOWER(%s) LIMIT 1;",
            (name,)
        )
    row = cur.fetchone()
    return row[0] if row else None

def get_houses_by_parent(cur, parentobjid):
    cur.execute("""
        SELECT objectid, housenum, addnum1, addtype1, addnum2, addtype2
          FROM public.fias_houses
         WHERE parentobjid = %s;
    """, (parentobjid,))
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

# Главная функция
def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Создание таблиц
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
    addtype_map = load_addtype_map(cur)
    lookup_map = load_lookup_map(cur)
    street_types = load_street_types(cur)

    # Сборка шаблона для разбора дома
    addtypes = sorted(addtype_map.keys(), key=len, reverse=True)
    addtypes_pattern = "|".join(re.escape(a) for a in addtypes)
    HOUSE_PATTERN = re.compile(rf"^(.+?)({addtypes_pattern})(\d*)$", re.IGNORECASE)

    def parse_and_find_house(cur, street_id, hs, addmap):
        m = HOUSE_PATTERN.match(hs)
        if m:
            house_name = m.group(1)
            add_type_str = m.group(2).lower()
            suffix_num = m.group(3)
            addtype_id = addmap.get(add_type_str)
            if addtype_id is None:
                logger.debug(f"Unknown addtype '{add_type_str}' in '{hs}'")
                return None
            candidates = get_houses_by_parent(cur, street_id).get(house_name, [])
            for c in candidates:
                if c['addtype1'] == addtype_id and (c['addnum1'] or '') == suffix_num:
                    return c['objectid']
            return None
        simple = get_houses_by_parent(cur, street_id).get(hs)
        if simple:
            return simple[0]['objectid']
        logger.debug(f"House '{hs}' did not match HOUSE_PATTERN")
        return None

    flats_rows, history_rows, processed_ids = [], [], []

    # Выборка объявлений
    cur.execute("""
        SELECT id, address, city, source, url, person_type_id, price,
               time_source_created, time_source_updated, params
          FROM ads
         WHERE processed IS FALSE
         LIMIT 10;
    """)
    rows = cur.fetchall()
    logger.info(f"Total rows to process: {len(rows)}")

    for idx, (ad_id, address, city, src, url, ptype, price, tcr, tup, params) in enumerate(rows, start=1):
        parts = [p.strip() for p in address.split(',') if p.strip()]
        if not parts:
            logger.debug(f"Row {idx}: empty address")
            continue

        # Спецобработка для Зеленограда
        if 'зеленоград' in city.lower():
            street_id = ZELENOGRAD_PARENTOBJID
            raw = parts[0]
            m = re.search(r"(\d+)", raw)
            house_str = m.group(1) if m else raw
            street_name = None
            street_type = None
        else:
            street_type, street_name = split_street(parts[0], street_types)
            street_id = find_street_objectid(cur, street_name, street_type)
            if not street_id:
                logger.debug(f"Row {idx}: street '{street_name}' not found")
                continue
            house_str = parts[1] if len(parts) > 1 else ""

        house_id = parse_and_find_house(cur, street_id, house_str, addtype_map)
        if not house_id:
            logger.debug(f"Row {idx}: house '{house_str}' not found on parentobjid {street_id}")
            continue

        floor = to_int(params.get('Этаж')) if params else None
        rooms = to_int(params.get('Количество комнат')) if params else None
        if floor is None or rooms is None:
            logger.debug(f"Row {idx}: invalid floor={floor!r}, rooms={rooms!r}")
            continue

        raw_house_type = (params.get('Тип дома') or '').lower().strip()
        house_type_id = lookup_map.get(('house_type', raw_house_type))
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

    logger.info(f"Prepared: {len(flats_rows)} flats, {len(history_rows)} history rows")

    if flats_rows:
        execute_values(cur, """
            INSERT INTO flats (
                house_id, floor, rooms,
                street, street_type, house,
                town, total_floors,
                area, living_area, kitchen_area,
                house_type_id
            ) VALUES %s
            ON CONFLICT DO NOTHING;
        """, flats_rows)
        logger.info(f"Inserted into flats: {len(flats_rows)}")

    if history_rows:
        execute_values(cur, """
            INSERT INTO flats_history (
                house_id, floor, rooms,
                source, object_type, ad_type,
                url, person_type_id, price,
                time_source_created, time_source_updated
            ) VALUES %s;
        """, history_rows)
        logger.info(f"Inserted into flats_history: {len(history_rows)}")

    if processed_ids:
        cur.execute(
            "UPDATE ads SET processed = TRUE WHERE id = ANY(%s);",
            (processed_ids,)
        )
        logger.info(f"Updated processed flag for {len(processed_ids)} ads")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
