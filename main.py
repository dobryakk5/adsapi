import os
import time
import requests
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Загрузка настроек из .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
ADS_API_USER = os.getenv("ADS_API_USER")
ADS_API_TOKEN = os.getenv("ADS_API_TOKEN")
ADS_API_URL = "https://ads-api.ru/main/api"

# Параметры работы
DATE_START = os.getenv("DATE_START", datetime.now().strftime('%Y-%m-%d'))
DAYS_COUNT = int(os.getenv("DAYS_COUNT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "30"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10"))


def fetch_ads_batch(date1: str, date2: str, city: str = None, source: str = None, limit: int = 1000):
    """
    Получает страницу объявлений за интервал date1..date2.
    Фильтры: category_id=2, nedvigimost_type=1.
    При HTTP 429 делает до MAX_RETRIES попыток с задержкой RETRY_DELAY.
    """
    params = {
        "user": ADS_API_USER,
        "token": ADS_API_TOKEN,
        "format": "json",
        "limit": limit,
        "category_id": 2,
        "nedvigimost_type": 1,
        "sort": "asc",
        "date1": date1,
        "date2": date2,
    }
    if city:
        params["city"] = city
    if source:
        params["source"] = source

    attempt = 0
    while True:
        try:
            print(f"Requesting ads {date1}–{date2}, attempt {attempt+1}")
            resp = requests.get(ADS_API_URL, params=params)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                attempt += 1
                print(f"429 Too Many Requests, retry {attempt}/{MAX_RETRIES} in {RETRY_DELAY}s")
                time.sleep(RETRY_DELAY)
                continue
            else:
                raise


def insert_ads_batch(cursor, ads):
    """
    Вставляет пачку объявлений в БД, удаляя лишние поля и нормируя координаты.
    """
    for ad in ads:
        for fld in ["person_type", "nedvigimost_type", "cat1", "cat2", "source"]:
            ad.pop(fld, None)

        coords = ad.get("coords") or {}
        try:
            lat = float(coords.get("lat")) if coords.get("lat") else None
            lng = float(coords.get("lng")) if coords.get("lng") else None
        except (ValueError, TypeError):
            lat = lng = None

        try:
            km = float(ad.get("km_do_metro")) if ad.get("km_do_metro") else None
        except (ValueError, TypeError):
            km = None

        cursor.execute(
            """
            INSERT INTO ads (
                id, url, price, "time", time_source_created, time_source_updated,
                person, person_type_id, city, metro_only, district_only,
                address, description, nedvigimost_type_id, avitoid,
                cat1_id, cat2_id, source_id, is_actual, km_do_metro,
                coords_lat, coords_lng, images, params, params2,
                processed, debug
            ) VALUES (
                %(id)s, %(url)s, %(price)s, %(time)s, %(time_source_created)s, %(time_source_updated)s,
                %(person)s, %(person_type_id)s, %(city)s, %(metro_only)s, %(district_only)s,
                %(address)s, %(description)s, %(nedvigimost_type_id)s, %(avitoid)s,
                %(cat1_id)s, %(cat2_id)s, %(source_id)s, %(is_actual)s, %(km_do_metro)s,
                %(coords_lat)s, %(coords_lng)s, %(images)s, %(params)s, %(params2)s,
                %(processed)s, %(debug)s
            ) ON CONFLICT (id) DO NOTHING;
            """, {
                "id": ad.get("id"),
                "url": ad.get("url"),
                "price": ad.get("price"),
                "time": ad.get("time"),
                "time_source_created": ad.get("time_source_created"),
                "time_source_updated": ad.get("time_source_updated"),
                "person": ad.get("person"),
                "person_type_id": ad.get("person_type_id"),
                "city": ad.get("city"),
                "metro_only": ad.get("metro_only"),
                "district_only": ad.get("district_only"),
                "address": ad.get("address"),
                "description": ad.get("description"),
                "nedvigimost_type_id": ad.get("nedvigimost_type_id"),
                "avitoid": ad.get("avitoid"),
                "cat1_id": ad.get("cat1_id"),
                "cat2_id": ad.get("cat2_id"),
                "source_id": ad.get("source_id"),
                "is_actual": ad.get("is_actual"),
                "km_do_metro": km,
                "coords_lat": lat,
                "coords_lng": lng,
                "images": Json(ad.get("images", [])),
                "params": Json(ad.get("params", {})),
                "params2": Json(ad.get("params2", {})),
                "processed": False,
                "debug": Json({})
            }
        )


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # 1. Определяем старт с помощью максимальной даты в БД
    cursor.execute('SELECT MAX("time") FROM ads;')
    last_time_in_db = cursor.fetchone()[0]

    if last_time_in_db is None:
        # Если БД пуста — стартуем с 00:00 DATE_START
        start_date = datetime.strptime(DATE_START, '%Y-%m-%d')
        last_start = start_date.replace(hour=0, minute=0, second=0)
    else:
        # Иначе — сразу после последнего времени
        last_start = last_time_in_db + timedelta(seconds=1)

    # Проходим DAYS_COUNT дней, начиная с даты last_start
    for offset in range(DAYS_COUNT):
        current_day = (last_start.date() - timedelta(days=offset))
        day_start = datetime.combine(current_day, datetime.min.time())
        day_end = datetime.combine(current_day, datetime.max.time())

        # Для первого дня используем last_start, далее — от начала дня
        batch_start = last_start if offset == 0 else day_start
        batch_end = day_end

        print(f"Processing {current_day}: {batch_start} → {batch_end}")
        total = 0
        next_start = batch_start

        while next_start <= batch_end:
            date1 = next_start.strftime('%Y-%m-%d %H:%M:%S')
            date2 = batch_end.strftime('%Y-%m-%d %H:%M:%S')

            ads = fetch_ads_batch(date1=date1, date2=date2, city="Москва", limit=1000)
            if not ads:
                break

            insert_ads_batch(cursor, ads)
            conn.commit()

            count = len(ads)
            total += count
            print(f"  Inserted {count} ads from {date1}")

            # Готовимся к следующей порции
            last_item_time = datetime.fromisoformat(ads[-1]["time"])
            next_start = last_item_time + timedelta(seconds=1)

            # Пауза 6 секунд между запросами
            time.sleep(6)

            if count < 1000:
                break

        print(f"Finished {current_day}: {total} ads inserted\n")

    cursor.close()
    conn.close()
    print("All days processed.")


if __name__ == "__main__":
    main()
