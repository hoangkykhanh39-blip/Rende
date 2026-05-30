import csv
import io
import logging
import os
import sys
import tempfile
import time
import zipfile
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

# ===== CẤU HÌNH =====
SYMBOL = "BTCUSDT"
OUTPUT_RAW_FILE = "BTCUSDT_30s_3Y.csv"
DAILY_DIR = "daily"
CHECKPOINT_FILE = "checkpoint.txt"
LOG_FILE = "download.log"
MAX_RETRIES = 3
YEARS_BACK = 3
END_DATE_OFFSET = 2
MAX_WORKERS = 5                     # Tải song song 5 ngày
PROCESS_TIMEOUT = 600               # 10 phút cho mỗi ngày
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
LOCALE = os.environ.get("LOCALE", "vi")
# ===================

LANGUAGES = {
    "vi": {
        "health_ok": "✅ Kết nối Binance thành công.",
        "starting": "🚀 Bắt đầu tải {total} ngày – từ {start} đến {end}",
        "progress": "📥 {date} | Đã tải: {done}/{total} | còn {remaining} ngày",
        "complete": "🎉 Hoàn tất! File: {file} | Thời gian: {elapsed:.1f}s",
        "completed_flag": "🏁 Đã tạo completed.flag – workflow sẽ không chạy lại.",
        "error_fatal": "💥 LỖI NGHIÊM TRỌNG: {error}",
        "process_timeout": "⏰ Timeout cho ngày {date}, hủy bỏ và fill bằng last_close."
    },
    "en": {
        "health_ok": "✅ Connected to Binance successfully.",
        "starting": "🚀 Starting download of {total} days – from {start} to {end}",
        "progress": "📥 {date} | Downloaded: {done}/{total} | {remaining} days left",
        "complete": "🎉 Done! File: {file} | Time: {elapsed:.1f}s",
        "completed_flag": "🏁 Completed flag created.",
        "error_fatal": "💥 FATAL ERROR: {error}",
        "process_timeout": "⏰ Timeout for {date}, cancelling and filling with last_close."
    }
}

def translate(key, **kwargs):
    return LANGUAGES.get(LOCALE, LANGUAGES["vi"]).get(key, key).format(**kwargs)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
def download_day(date_str):
    """Tải file zip từ Binance, parse thành dict candles {ts_30s: candle}.
    Trả về None nếu lỗi hoặc 404."""
    url = f"https://data.binance.vision/data/spot/daily/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{date_str}.zip"
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    raw_zip = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                raw_zip = resp.content
                break
            elif resp.status_code == 404:
                logger.info(f"   {date_str}: 404 - không có dữ liệu")
                return None
            elif resp.status_code == 429:
                wait = min(60, 15 * attempt)
                logger.warning(f"   {date_str}: 429, chờ {wait}s...")
                time.sleep(wait)
            else:
                logger.warning(f"   {date_str}: mã {resp.status_code}, thử lại...")
                time.sleep(3)
        except requests.RequestException as e:
            logger.warning(f"   {date_str}: lỗi mạng, thử lại ({attempt}/{MAX_RETRIES})")
            time.sleep(5 * attempt)

    if raw_zip is None:
        logger.error(f"   {date_str}: thất bại sau {MAX_RETRIES} lần thử")
        return None

    candles = {}
    try:
        with zipfile.ZipFile(io.BytesIO(raw_zip)) as z:
            file_list = z.namelist()
            if not file_list:
                return None
            with z.open(file_list[0]) as csv_file:
                reader = csv.reader(io.TextIOWrapper(csv_file, encoding="utf-8"))
                for row in reader:
                    if len(row) < 6:
                        continue
                    try:
                        price = float(row[1])
                        qty = float(row[2])
                        ts = int(row[5])
                        quote_qty = float(row[3]) if len(row) > 3 else 0.0
                        is_buyer_maker = row[6].strip().lower() == 'true' if len(row) > 6 else False
                    except (ValueError, IndexError):
                        continue
                    ts_30s = (ts // 30000) * 30000
                    if ts_30s not in candles:
                        candles[ts_30s] = {
                            "o": price, "h": price, "l": price, "c": price,
                            "v": 0.0, "qv": 0.0, "n": 0,
                            "vwap_sum": 0.0, "tbv": 0.0, "tbqv": 0.0
                        }
                    c = candles[ts_30s]
                    c["h"] = max(c["h"], price)
                    c["l"] = min(c["l"], price)
                    c["c"] = price
                    c["v"] += qty
                    c["qv"] += quote_qty
                    c["n"] += 1
                    c["vwap_sum"] += price * qty
                    if not is_buyer_maker:
                        c["tbv"] += qty
                        c["tbqv"] += quote_qty
        return candles
    except zipfile.BadZipFile:
        logger.error(f"   {date_str}: ZIP hỏng")
        return None
    except Exception as e:
        logger.error(f"   {date_str}: lỗi parse - {e}")
        return None

# ----------------------------------------------------------------------
def save_raw_candles(date_str, candles):
    """Lưu file CSV với dữ liệu có sẵn (không fill)."""
    day_dt = datetime.strptime(date_str, "%Y-%m-%d")
    day_start_ts = int(day_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    day_end_ts = day_start_ts + 24 * 60 * 60 * 1000
    daily_file = os.path.join(DAILY_DIR, date_str + ".csv")

    rows = []
    for ts in range(day_start_ts, day_end_ts, 30000):
        if ts in candles:
            c = candles[ts]
            dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            vwap = c["vwap_sum"] / c["v"] if c["v"] > 0 else c["o"]
            rows.append([dt, c["o"], c["h"], c["l"], c["c"],
                         c["v"], c["qv"], c["n"], round(vwap, 8), c["tbv"], c["tbqv"]])

    os.makedirs(DAILY_DIR, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=DAILY_DIR, suffix=".tmp")
    os.close(fd)
    with open(tmp_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Open_Time","Open","High","Low","Close","Volume","Quote_Volume","Trades","VWAP","Taker_Buy_Volume","Taker_Buy_Quote_Volume"])
        writer.writerows(rows)
    if os.path.exists(daily_file):
        os.remove(daily_file)
    os.rename(tmp_path, daily_file)

# ----------------------------------------------------------------------
def get_last_close_from_file(date_str):
    """Đọc giá đóng cửa cuối cùng từ file daily (dòng cuối). Trả về float hoặc None."""
    filepath = os.path.join(DAILY_DIR, date_str + ".csv")
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "r") as f:
            last_line = deque(csv.reader(f), maxlen=1)
            if last_line:
                return float(last_line[0][4])
    except Exception:
        pass
    return None

# ----------------------------------------------------------------------
def fill_gaps(start_dt, end_dt):
    """Điền last_close cho tất cả các ngày từ start_dt đến end_dt.
    Nếu chưa có last_close (ví dụ ngày đầu tiên chưa có dữ liệu) thì bắt đầu từ
    nến có giao dịch đầu tiên, bỏ qua các nến trống trước đó."""
    # Lấy last_close ban đầu từ ngày trước start_dt
    prev_day = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    last_close = get_last_close_from_file(prev_day)

    current = start_dt
    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        daily_file = os.path.join(DAILY_DIR, date_str + ".csv")
        day_start_ts = int(current.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        day_end_ts = day_start_ts + 24 * 60 * 60 * 1000

        existing_candles = {}
        if os.path.exists(daily_file):
            with open(daily_file, "r") as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    try:
                        dt_str = row[0]
                        dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        ts = int(dt_obj.timestamp() * 1000)
                        existing_candles[ts] = {
                            "o": float(row[1]), "h": float(row[2]), "l": float(row[3]), "c": float(row[4]),
                            "v": float(row[5]), "qv": float(row[6]), "n": int(row[7]),
                            "vwap": float(row[8]), "tbv": float(row[9]), "tbqv": float(row[10])
                        }
                    except (ValueError, IndexError, KeyError):
                        continue

        rows = []
        for ts in range(day_start_ts, day_end_ts, 30000):
            if ts in existing_candles:
                c = existing_candles[ts]
                last_close = c["c"]
            else:
                if last_close is None:
                    # Chưa có giá tham chiếu -> không thể fill, bỏ qua nến này
                    continue
                c = {
                    "o": last_close, "h": last_close, "l": last_close, "c": last_close,
                    "v": 0.0, "qv": 0.0, "n": 0, "vwap": last_close, "tbv": 0.0, "tbqv": 0.0
                }
            dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            rows.append([dt, c["o"], c["h"], c["l"], c["c"],
                         c["v"], c["qv"], c["n"], round(c["vwap"], 8), c["tbv"], c["tbqv"]])

        # Ghi file
        fd, tmp_path = tempfile.mkstemp(dir=DAILY_DIR, suffix=".tmp")
        os.close(fd)
        with open(tmp_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Open_Time","Open","High","Low","Close","Volume","Quote_Volume","Trades","VWAP","Taker_Buy_Volume","Taker_Buy_Quote_Volume"])
            writer.writerows(rows)
        if os.path.exists(daily_file):
            os.remove(daily_file)
        os.rename(tmp_path, daily_file)

        current += timedelta(days=1)

# ----------------------------------------------------------------------
def merge_all_daily(output_file):
    files = sorted([f for f in os.listdir(DAILY_DIR) if f.endswith(".csv")])
    if not files:
        return
    with open(output_file, "w", newline="") as out:
        writer = csv.writer(out)
        header = ["Open_Time","Open","High","Low","Close","Volume","Quote_Volume","Trades","VWAP","Taker_Buy_Volume","Taker_Buy_Quote_Volume"]
        writer.writerow(header)
        for fname in files:
            with open(os.path.join(DAILY_DIR, fname), "r") as f:
                reader = csv.reader(f)
                next(reader)
                writer.writerows(reader)

# ----------------------------------------------------------------------
def main():
    start_time = time.time()
    try:
        # 1. Health check
        resp = requests.head("https://data.binance.vision/", timeout=10)
        if resp.status_code != 200:
            logger.error("Không kết nối được Binance.")
            sys.exit(1)
        logger.info(translate("health_ok"))

        # 2. Khoảng ngày
        end_date = datetime.now(timezone.utc) - timedelta(days=END_DATE_OFFSET)
        start_date = end_date - timedelta(days=YEARS_BACK * 365 + 1)

        # 3. Resume
        resume_date = start_date.date()
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r") as f:
                ds = f.read().strip()
                if ds:
                    try:
                        last_done = datetime.strptime(ds, "%Y-%m-%d").date()
                        resume_date = last_done + timedelta(days=1)
                    except:
                        pass

        while resume_date <= end_date.date():
            fpath = os.path.join(DAILY_DIR, resume_date.strftime("%Y-%m-%d") + ".csv")
            if os.path.exists(fpath):
                with open(fpath, "r") as f:
                    reader = csv.reader(f)
                    next(reader)
                    first_row = next(reader, None)
                    if first_row is not None:
                        resume_date += timedelta(days=1)
                        continue
            break

        if resume_date > end_date.date():
            logger.info("✅ Tất cả ngày đã có dữ liệu. Tạo file tổng hợp...")
            merge_all_daily(OUTPUT_RAW_FILE)
            logger.info(translate("complete", file=OUTPUT_RAW_FILE, elapsed=time.time()-start_time))
            return

        dates_to_download = []
        d = datetime.combine(resume_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        while d <= end_date:
            dates_to_download.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        total = len(dates_to_download)
        logger.info(translate("starting", total=total, start=resume_date, end=end_date.date()))

        # 4. Tải song song
        downloaded_count = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_date = {executor.submit(download_day, date_str): date_str for date_str in dates_to_download}
            for future in as_completed(future_to_date):
                date_str = future_to_date[future]
                try:
                    candles = future.result()
                    if candles is not None:
                        save_raw_candles(date_str, candles)
                except Exception as e:
                    logger.error(f"   {date_str}: lỗi không xác định - {e}")
                downloaded_count += 1
                remaining = total - downloaded_count
                if downloaded_count % 5 == 0 or remaining == 0:
                    logger.info(translate("progress", date=date_str, done=downloaded_count, total=total, remaining=remaining))

        # 5. Fill gaps
        logger.info("🔄 Đang fill forward các nến trống...")
        fill_start = datetime.combine(resume_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        fill_gaps(fill_start, end_date)

        # 6. Gộp file tổng
        logger.info("📦 Đang tạo file tổng hợp raw data...")
        merge_all_daily(OUTPUT_RAW_FILE)

        with open(CHECKPOINT_FILE, "w") as f:
            f.write(end_date.strftime("%Y-%m-%d"))

        with open("completed.flag", "w") as f:
            f.write(f"Completed at {datetime.now()}\n")

        elapsed = time.time() - start_time
        logger.info(translate("complete", file=OUTPUT_RAW_FILE, elapsed=elapsed))

    except Exception as e:
        logger.exception(translate("error_fatal", error=e))
        sys.exit(1)

if __name__ == "__main__":
    main()
