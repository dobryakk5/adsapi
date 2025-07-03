import os
import json
import requests
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Загрузка конфигурации из .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
ADS_API_USER = os.getenv("ADS_API_USER")
ADS_API_TOKEN = os.getenv("ADS_API_TOKEN")
ADS_API_URL = "https://ads-api.ru/main/api"

def fetch_ads(
    city: str = None,
    metro: str = None,
    rooms_count: int = None,
    source: str = None,
    person_type: int = 3,
    created_from: str = None,
    created_to: str = None,
    updated_from: str = None,
    updated_to: str = None,
    limit: int = 10
):
    # Устанавливаем фильтр по дате создания: вчера и сегодня, если не заданы явно
    if not created_from and not created_to:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        created_from = yesterday.strftime('%Y-%m-%d')
        #created_from = today.strftime('%Y-%m-%d')
        # устанавливаем до конца сегодняшнего дня
        created_to = today.strftime('%Y-%m-%d')

    params = {
        "user": ADS_API_USER,
        "token": ADS_API_TOKEN,
        "format": "json",
        "limit": limit,
        # Фильтры: тип объявления (person_type), продажа квартир
        "person_type": person_type,
        "category_id": 2,            # Квартиры
        "nedvigimost_type": 1,        # Продам
    }
    # Фильтры по датам создания
    if created_from:
        params["date1"] = created_from
    if created_to:
        params["date2"] = created_to

    # Прочие фильтры
    if city:
        params["city"] = city
    if metro:
        params["metro"] = metro
    if rooms_count:
        params["param[2313]"] = rooms_count
    if source:
        params["source"] = source

    # Запрос
    print(f"Requesting URL: {ADS_API_URL} with params: {params}")
    response = requests.get(ADS_API_URL, params=params)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Error requesting {response.url}: {response.status_code} - {response.text}")
        raise

    ads = response.json().get("data", [])

    # Фильтрация по дате обновления, если задано
    def in_range(dt_str, start, end):
        if not dt_str:
            return False
        dt = datetime.fromisoformat(dt_str)
        if start and dt < datetime.fromisoformat(start):
            return False
        if end and dt > datetime.fromisoformat(end):
            return False
        return True

    if updated_from or updated_to:
        ads = [
            ad for ad in ads
            if in_range(ad.get("time_source_updated"), updated_from, updated_to)
        ]

    return ads


def insert_ad(cursor, ad):
    coords = ad.get("coords") or {}
    try:
        lat = float(coords.get("lat")) if coords.get("lat") else None
        lng = float(coords.get("lng")) if coords.get("lng") else None
    except:
        lat = lng = None

    try:
        km = float(ad.get("km_do_metro")) if ad.get("km_do_metro") else None
    except:
        km = None

    cursor.execute("""
        INSERT INTO ads (
            id, url, title, price, price_metric, time, time_source_created,
            time_source_updated, phone, phone_protected, phone_operator, phone_region,
            person, person_type, person_type_id, contactname, city, city1, region,
            metro, metro_only, district_only, address, description,
            nedvigimost_type, nedvigimost_type_id, avitoid, cat1_id, cat2_id,
            cat1, cat2, source, source_id, is_actual, km_do_metro, coords_lat, coords_lng,
            images, params, params2, param_raw, count_ads_same_phone
        ) VALUES (
            %(id)s, %(url)s, %(title)s, %(price)s, %(price_metric)s, %(time)s, %(time_source_created)s,
            %(time_source_updated)s, %(phone)s, %(phone_protected)s, %(phone_operator)s, %(phone_region)s,
            %(person)s, %(person_type)s, %(person_type_id)s, %(contactname)s, %(city)s, %(city1)s, %(region)s,
            %(metro)s, %(metro_only)s, %(district_only)s, %(address)s, %(description)s,
            %(nedvigimost_type)s, %(nedvigimost_type_id)s, %(avitoid)s, %(cat1_id)s, %(cat2_id)s,
            %(cat1)s, %(cat2)s, %(source)s, %(source_id)s, %(is_actual)s, %(km_do_metro)s, %(coords_lat)s, %(coords_lng)s,
            %(images)s, %(params)s, %(params2)s, %(param_raw)s, %(count_ads_same_phone)s
        )
        ON CONFLICT (id) DO NOTHING;
    """, {
        "id": ad.get("id"),
        "url": ad.get("url"),
        "title": ad.get("title"),
        "price": ad.get("price"),
        "price_metric": ad.get("price_metric"),
        "time": ad.get("time"),
        "time_source_created": ad.get("time_source_created"),
        "time_source_updated": ad.get("time_source_updated"),
        "phone": ad.get("phone"),
        "phone_protected": bool(ad.get("phone_protected")) if ad.get("phone_protected") is not None else None,
        "phone_operator": ad.get("phone_operator"),
        "phone_region": ad.get("phone_region"),
        "person": ad.get("person"),
        "person_type": ad.get("person_type"),
        "person_type_id": ad.get("person_type_id"),
        "contactname": ad.get("contactname"),
        "city": ad.get("city"),
        "city1": ad.get("city1"),
        "region": ad.get("region"),
        "metro": ad.get("metro"),
        "metro_only": ad.get("metro_only"),
        "district_only": ad.get("district_only"),
        "address": ad.get("address"),
        "description": ad.get("description"),
        "nedvigimost_type": ad.get("nedvigimost_type"),
        "nedvigimost_type_id": ad.get("nedvigimost_type_id"),
        "avitoid": ad.get("avitoid"),
        "cat1_id": ad.get("cat1_id"),
        "cat2_id": ad.get("cat2_id"),
        "cat1": ad.get("cat1"),
        "cat2": ad.get("cat2"),
        "source": ad.get("source"),
        "source_id": ad.get("source_id"),
        "is_actual": ad.get("is_actual"),
        "km_do_metro": km,
        "coords_lat": lat,
        "coords_lng": lng,
        "images": Json(ad.get("images", [])),
        "params": Json(ad.get("params", {})),
        "params2": Json(ad.get("params2", {})),
        "param_raw": Json({k: v for k, v in ad.items() if k.startswith("param[")}),
        "count_ads_same_phone": ad.get("count_ads_same_phone")
    })

def main():
    ads = fetch_ads(
        city="Москва",
        source="1,2",
        person_type=3,
        limit=50
    )
    print(f"Fetched {len(ads)} ads from API.")

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    for ad in ads:
        try:
            insert_ad(cursor, ad)
        except Exception as e:
            print(f"Failed to insert ad {ad.get('id')}: {e}")
            conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()
    print("Done inserting ads.")

if __name__ == "__main__":
    main()
