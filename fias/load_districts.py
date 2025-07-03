#!/usr/bin/env python3
import os
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

# === Конфигурация ===
ROOT_DIR = "/Users/pavellebedev/Downloads/fias_xml/77"
PREFIX   = "AS_ADDR_OBJ"

def build_index() -> dict:
    """
    Собираем словарь всех объектов из папки 77:
      guid → { name, type, parent, path, is_actual }
    """
    idx = {}
    for fn in os.listdir(ROOT_DIR):
        if not fn.startswith(PREFIX) or not fn.endswith(".XML"):
            continue
        full = os.path.join(ROOT_DIR, fn)
        for _, elem in ET.iterparse(full, events=("start",)):
            if elem.tag == "OBJECT":
                guid = elem.attrib.get("OBJECTGUID")
                if guid:
                    idx[guid] = {
                        "name":      elem.attrib.get("NAME", "").strip(),
                        "type":      elem.attrib.get("TYPENAME", "").strip(),
                        "parent":    elem.attrib.get("PARENTGUID"),
                        "path":      elem.attrib.get("PATH", ""),
                        "is_actual": elem.attrib.get("ISACTUAL")
                    }
            elem.clear()
    return idx

def extract_districts_with_quarters(idx: dict) -> list[tuple[str,str,str,str]]:
    """
    Для каждого внутригородского округа (TYPENAME='вн.тер.г.', ISACTUAL='1'):
      - Формируем display_name = 'район {BaseName}', где BaseName = NAME без 'муниципальный округ '.
      - Собираем кварталы (TYPENAME='кв-л') в CSV.
    """
    rows = []
    # Сначала маппинг кварталов по районам
    quarter_map: dict[str, list[str]] = {}
    for guid, rec in idx.items():
        if rec["type"] == "кв-л" and rec["is_actual"] == "1":
            for d_guid, d_rec in idx.items():
                if d_rec["type"] == "вн.тер.г." and d_guid in rec["path"]:
                    quarter_map.setdefault(d_guid, []).append(rec["name"])

    # Теперь обходим районы
    for guid, rec in idx.items():
        if rec["type"] == "вн.тер.г." and rec["is_actual"] == "1":
            # Убираем префикс "муниципальный округ " если он есть
            base_name = rec["name"]
            prefix = "муниципальный округ "
            if base_name.lower().startswith(prefix):
                base_name = base_name[len(prefix):].strip()
            display_name = f"район {base_name}"

            quarters = quarter_map.get(guid, [])
            quarters_csv = ", ".join(sorted(set(quarters))) if quarters else None
            rows.append((None, guid, display_name, quarters_csv))
    return rows


def import_to_db(rows):
    """
    Создает таблицу districts:
      id SERIAL, admin_okrug TEXT, fias_id UUID, name TEXT, quarters TEXT
    и заливает данные.
    """
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS districts (
      id            SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      admin_okrug   TEXT,
      fias_id       UUID    UNIQUE NOT NULL,
      name          TEXT    NOT NULL,
      quarters      TEXT
    );
    """)
    conn.commit()

    execute_values(
        cur,
        """
        INSERT INTO districts(admin_okrug, fias_id, name, quarters)
        VALUES %s
        ON CONFLICT (fias_id) DO NOTHING
        """,
        rows
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Импортировано районов с кварталами: {len(rows)}")

def main():
    print("1) Индексируем слепок ФИАС (папка 77)…")
    idx = build_index()
    print(f"   Всего элементов: {len(idx)}")

    print("2) Извлекаем внутригородские районы и их кварталы…")
    rows = extract_districts_with_quarters(idx)
    print(f"   Найдено районов: {len(rows)}")

    print("3) Записываем в БД…")
    import_to_db(rows)

if __name__ == "__main__":
    main()
