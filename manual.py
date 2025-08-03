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

# Диапазон обработки: ручные даты в формате YYYY-MM-DD
MANUAL_START = os.getenv("DATE_START", "2024-09-01")
MANUAL_END = os.getenv("DATE_END", "2024-12-31")  # либо None для now
if not MANUAL_START:
    logger.error("DATE_START не задан. Укажите начальную дату в формате YYYY-MM-DD")
    sys.exit(1)
try:
    manual_start_dt = datetime.strptime(MANUAL_START, '%Y-%m-%d')
except ValueError:
    logger.error("Неверный формат DATE_START. Ожидается YYYY-MM-DD")
    sys.exit(1)
if MANUAL_END:
    try:
        manual_end_dt = datetime.strptime(MANUAL_END, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        logger.error("Неверный формат DATE_END. Ожидается YYYY-MM-DD")
        sys.exit(1)
else:
    manual_end_dt = datetime.now()

# Константы
BATCH_DELAY = int(os.getenv("BATCH_DELAY", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "30"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10"))
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "1000"))

# Функции запроса и вставки
...
# (оставляем fetch_ads_batch и insert_ads_batch без изменений)


def main():
    conn = psycopg2.connect(DATABASE_URL)
    # Создаем схему и таблицу состояния
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS system.fetch_state (
                id SERIAL PRIMARY KEY,
                last_processed TIMESTAMP NOT NULL
            );
            """
        )
        conn.commit()
    # Получаем последнюю сохраненную точку
    with conn.cursor() as cur:
        cur.execute("SELECT last_processed FROM system.fetch_state ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()
    if row:
        start_dt = row[0]
        logger.info(f"Resuming from saved state {start_dt}")
    else:
        start_dt = manual_start_dt
        logger.info(f"Starting from manual DATE_START = {start_dt.date()}")

    # Обрабатываем дни от start_dt до manual_end_dt
    current = start_dt
    total_inserted = 0
    while current < manual_end_dt:
        period_end = min(current + timedelta(days=1), manual_end_dt)
        day_start = current.strftime('%Y-%m-%d %H:%M:%S')
        day_end = period_end.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Processing interval {day_start} - {day_end}")

        next_start = day_start
        while True:
            batch = fetch_ads_batch(
                date1=next_start,
                date2=day_end,
                city="Москва",
                source="1,2,3,4",
                limit=BATCH_LIMIT
            )
            if not batch:
                break

            with conn.cursor() as batch_cur:
                insert_ads_batch(batch_cur, batch)
                conn.commit()
                batch_cur.execute("CALL process_all_ads();")
                conn.commit()

            cnt = len(batch)
            total_inserted += cnt
            last_time = datetime.fromisoformat(batch[-1]["time"]) + timedelta(seconds=1)
            next_start = last_time.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Inserted {cnt} ads, next start {next_start}")

            if cnt < BATCH_LIMIT:
                break
            time.sleep(BATCH_DELAY)

        # Сохраняем прогресс в таблицу состояния
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO system.fetch_state (last_processed) VALUES (%s);",
                (period_end,)
            )
            conn.commit()
        logger.info(f"Saved state up to {period_end}")

        current = period_end
        time.sleep(BATCH_DELAY)

    logger.info(f"Processing completed, total ads inserted: {total_inserted}")
    conn.close()

if __name__ == "__main__":
    main()
