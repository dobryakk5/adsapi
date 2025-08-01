import os
import sys
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import requests
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from datetime import datetime, timedelta

# --- Настройки логирования с ротацией по дням ---
log_dir = os.getenv("LOG_DIR", "./logs")
os.makedirs(log_dir, exist_ok=True)
logger = logging.getLogger("ads_fetcher")
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(
    filename=os.path.join(log_dir, "ads_fetcher.log"),
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8"
)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Загрузка настроек из .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
ADS_API_USER = os.getenv("ADS_API_USER")
ADS_API_TOKEN = os.getenv("ADS_API_TOKEN")
ADS_API_URL = os.getenv("ADS_API_URL", "https://ads-api.ru/main/api")

# Параметры
DATE_START = os.getenv("DATE_START", "2025-01-01")  # e.g. "2025-01-01" или None
DAYS_COUNT = int(os.getenv("DAYS_COUNT", "0"))
BATCH_DELAY = int(os.getenv("BATCH_DELAY", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "30"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10"))
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "200"))


def fetch_ads_batch(date1: str, date2: str, city: str = None, source: str = None, limit: int = BATCH_LIMIT):
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
            logger.info(f"Requesting ads from {date1} to {date2}, attempt {attempt+1}")
            resp = requests.get(ADS_API_URL, params=params)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                attempt += 1
                logger.warning(f"429 Too Many Requests, retry {attempt}/{MAX_RETRIES} after {RETRY_DELAY}s")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Failed to fetch ads: {e}")
                raise


def insert_ads_batch(cursor, ads):
    exclude_city = ['зеленоград', 'новая москва', 'область']
    exclude_district = ['нао']
    exclude_address = ['новомосковский', 'зеленоград', 'троицк', 'красногорск', 'обл.', 'люберцы', 'балашиха','нао']

    for ad in ads:
        city = (ad.get("city") or "").lower()
        district = (ad.get("district_only") or "").lower()
        address = (ad.get("address") or "").lower()

        if any(sub in city for sub in exclude_city): continue
        if any(sub in district for sub in exclude_district): continue
        if any(sub in address for sub in exclude_address): continue

        for f in ["person_type", "nedvigimost_type", "cat1", "cat2", "source"]:
            ad.pop(f, None)

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

    # Начальный процессинг любых прошлых данных
    cursor.execute("CALL process_all_ads();")
    conn.commit()


    # ждём, пока не обнулится число необработанных объявлений
    while True:
        cursor.execute("SELECT COUNT(*) FROM ads WHERE processed = FALSE;")
        remaining = cursor.fetchone()[0]
        if remaining == 0:
            logger.info("Все объявления обработаны.")
            break
        #logger.info(f"Ожидание обработки: {remaining} записей осталось...")
        time.sleep(5)

    # Определяем точку продолжения
    cursor.execute("SELECT MAX(time_source_updated) FROM ads;")
    last_saved = cursor.fetchone()[0]
    if last_saved:
        start_dt = last_saved + timedelta(seconds=1)
        logger.info(f"Resuming from last saved time {start_dt}")
    else:
        if not DATE_START:
            logger.error("DATE_START не задан, и данных в БД нет — выходим")
            sys.exit(1)
        start_dt = datetime.strptime(DATE_START, '%Y-%m-%d')
        logger.info(f"No existing data, starting from DATE_START = {start_dt.date()}")

    # Определяем конец периода
    if DAYS_COUNT > 0:
        end_dt = min(start_dt + timedelta(days=DAYS_COUNT), datetime.now())
    else:
        end_dt = datetime.now()

    total = 0
    current_day = start_dt
    while current_day < end_dt:
        day_start = current_day.strftime('%Y-%m-%d 00:00:00')
        day_end = (current_day + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        logger.info(f"Processing {current_day.date()} ({day_start} — {day_end})")
        next_start = day_start

        while True:
            batch = fetch_ads_batch(date1=next_start, date2=day_end, city="Москва", source="1,2,3,4", limit=BATCH_LIMIT)
            if not batch:
                break

            insert_ads_batch(cursor, batch)
            conn.commit()

            cnt = len(batch)
            total += cnt
            last_time = datetime.fromisoformat(batch[-1]["time"]) + timedelta(seconds=1)
            next_start = last_time.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"  Inserted {cnt} ads, next start: {next_start}")

            # Продолжаем, пока есть пачки по BATCH_LIMIT
            if cnt < BATCH_LIMIT:
                break

            time.sleep(BATCH_DELAY)

        current_day += timedelta(days=1)
        time.sleep(BATCH_DELAY)

    logger.info(f"Done. Total inserted for period: {total}")
    cursor.execute("CALL process_all_ads();")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
