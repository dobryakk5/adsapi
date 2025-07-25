import os
import sys
import time
import requests
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Настройки: начало отсчёта и количество дней назад
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
ADS_API_USER = os.getenv("ADS_API_USER")
ADS_API_TOKEN = os.getenv("ADS_API_TOKEN")
ADS_API_URL = "https://ads-api.ru/main/api"

# Через .env задаются:
# DATE_START: 'YYYY-MM-DD'
# DAYS_COUNT: число дней для обработки (можно переопределить аргументом)
# BATCH_DELAY: задержка между пачками и между днями в секундах (по умолчанию 5)
# MAX_RETRIES: число повторных попыток при HTTPError (429)
# RETRY_DELAY: пауза при retry
DATE_START = os.getenv("DATE_START", datetime.now().strftime('%Y-%m-%d'))
# Считываем DAYS_COUNT из argv при наличии
if len(sys.argv) > 1:
    try:
        DAYS_COUNT = int(sys.argv[1])
    except ValueError:
        print(f"Invalid DAYS_COUNT argument: {sys.argv[1]}")
        sys.exit(1)
else:
    DAYS_COUNT = int(os.getenv("DAYS_COUNT", "30"))

BATCH_DELAY = int(os.getenv("BATCH_DELAY", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "30"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10"))


def fetch_ads_batch(date1: str, date2: str, city: str = None, source: str = None, limit: int = 1000):
    """
    Получает одну страницу объявлений за интервал date1..date2.
    Фильтры: квартиры (category_id=2), продажа (nedvigimost_type=1).
    При HTTPError 429 ретрай не более MAX_RETRIES раз с задержкой RETRY_DELAY.
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
            print(f"Requesting ads from {date1} to {date2}, attempt {attempt+1}")
            resp = requests.get(ADS_API_URL, params=params)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except requests.exceptions.HTTPError:
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                attempt += 1
                print(f"429 Too Many Requests, retry in {RETRY_DELAY}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
                continue
            else:
                raise


def insert_ads_batch(cursor, ads):
    """
    Вставляет пачку объявлений в БД, исключая ненужные поля.
    """
    for ad in ads:
        for field in ["person_type", "nedvigimost_type", "cat1", "cat2", "source"]:
            ad.pop(field, None)
        coords = ad.get("coords") or {}
        try:
            lat = float(coords.get("lat")) if coords.get("lat") else None
            lng = float(coords.get("lng")) if coords.get("lng") else None
        except (ValueError, TypeError): lat = lng = None
        try:
            km = float(ad.get("km_do_metro")) if ad.get("km_do_metro") else None
        except (ValueError, TypeError): km = None

        cursor.execute("""
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
        })


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # Логирование минимального дня и максимального времени в нём
    cursor.execute("SELECT MIN(date(time)) FROM ads;")
    min_day = cursor.fetchone()[0]
    if not min_day:
        min_day = datetime.strptime(DATE_START, '%Y-%m-%d').date()
    cursor.execute(
        "SELECT MAX(time) FROM ads WHERE date(time) = %s;", (min_day,)
    )
    max_time = cursor.fetchone()[0]
    print(f"[LOG] Minimal day: {min_day}", flush=True)
    print(f"[LOG] Max time in minimal day: {max_time}", flush=True)

        # Основной алгоритм:
    # 1) Сначала добираем остатки минимального дня (min_day)
    start_of_min = datetime.combine(min_day, datetime.min.time())
    end_of_min = datetime.combine(min_day, datetime.max.time())
    # Если в минимальном дне есть макстайм, начинаем после него, иначе — с начала дня
    if max_time:
        next_start = (max_time + timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        next_start = start_of_min.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Processing minimal day: {min_day} from {next_start} to {end_of_min}", flush=True)
    total_min = 0
    while True:
        batch = fetch_ads_batch(date1=next_start, date2=end_of_min.strftime('%Y-%m-%d %H:%M:%S'), city="Москва", source="1,2,3,4")
        if not batch:
            break
        insert_ads_batch(cursor, batch)
        conn.commit()
        count = len(batch)
        total_min += count
        last_time = datetime.fromisoformat(batch[-1]["time"]) + timedelta(seconds=1)
        next_start = last_time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"  Inserted {count} ads for {min_day} (from {next_start})", flush=True)
        if count < 1000:
            break
        time.sleep(BATCH_DELAY)
    print(f"Finished minimal day {min_day}: {total_min} ads inserted", flush=True)
    time.sleep(BATCH_DELAY)

    # 2) Дальше обрабатываем предыдущие дни: min_day-1, минус DAYS_COUNT-1 дней
    for d in range(1, DAYS_COUNT):
        current = min_day - timedelta(days=d)
        date1 = datetime.combine(current, datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S')
        date2 = datetime.combine(current, datetime.max.time()).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Processing day: {current} (Day {d+1}/{DAYS_COUNT})", flush=True)
        total_day = 0
        next_start = date1
        while True:
            batch = fetch_ads_batch(date1=next_start, date2=date2, city="Москва", source="1,2,3,4")
            if not batch:
                break
            insert_ads_batch(cursor, batch)
            conn.commit()
            count = len(batch)
            total_day += count
            last_time = datetime.fromisoformat(batch[-1]["time"]) + timedelta(seconds=1)
            next_start = last_time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"  Inserted {count} ads for {current} (from {next_start})", flush=True)
            if count < 1000:
                break
            time.sleep(BATCH_DELAY)
        print(f"Finished processing {current}: {total_day} ads inserted", flush=True)
        time.sleep(BATCH_DELAY)

    cursor.close()
    conn.close()
    print("All days processed.", flush=True)

if __name__ == "__main__":
    main()
