#!/usr/bin/env python3
import os
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values

ROOT_DIR = "/Users/pavellebedev/Downloads/fias_xml/77"
PREFIX = "AS_ADDR_OBJ"

def build_index():
    idx = {}
    for fn in os.listdir(ROOT_DIR):
        if not fn.startswith(PREFIX) or not fn.endswith(".XML"):
            continue
        full = os.path.join(ROOT_DIR, fn)
        for _, elem in ET.iterparse(full, events=("start",)):
            if elem.tag == "OBJECT":
                guid = elem.attrib.get("OBJECTGUID")
                aoi = elem.attrib.get("AOID")  # идентификатор
                idx[guid] = {
                    "aoid":      aoi,
                    "name":      elem.attrib.get("NAME", "").strip(),
                    "type":      elem.attrib.get("TYPENAME", "").strip(),
                    "path":      elem.attrib.get("PATH", ""),
                    "is_actual": elem.attrib.get("ISACTUAL")
                }
            elem.clear()
    return idx

def extract_quarters(idx):
    rows = []
    aoid_to_guid = {rec['aoid']: guid for guid, rec in idx.items() if rec['type']=='вн.тер.г.'}
    for guid, rec in idx.items():
        if rec["type"]=="кв-л" and rec["is_actual"]=="1":
            path_aoids = rec["path"].split(';')
            parent_guid = next((aoid_to_guid[aoid] for aoid in path_aoids if aoid in aoid_to_guid), None)
            if not parent_guid:
                continue
            name = f"кв‑л {rec['name']}"
            rows.append((guid, name, parent_guid))
    return rows


def import_to_db(rows):
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS quarters (
        id               SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        fias_id          UUID     UNIQUE NOT NULL,
        name             TEXT     NOT NULL,
        district_fias_id UUID     NOT NULL REFERENCES districts(fias_id)
      );
    """)
    conn.commit()

    execute_values(
      cur,
      "INSERT INTO quarters(fias_id, name, district_fias_id) VALUES %s ON CONFLICT(fias_id) DO NOTHING",
      rows
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Импортировано кварталов: {len(rows)}")

def main():
    print("Indexing...")
    idx = build_index()
    print(f"Objects in index: {len(idx)}")

    print("Extracting quarters via AOID paths...")
    rows = extract_quarters(idx)
    print(f"Quarters found: {len(rows)}")

    print("Importing to DB...")
    import_to_db(rows)

if __name__ == "__main__":
    main()
