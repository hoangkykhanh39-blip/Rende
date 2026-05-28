import csv
import time
import zipfile
import io
import os
import logging
import requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

# ===== CẤU HÌNH =====
SYMBOL = "BTCUSDT"
OUTPUT_FILE = "BTCUSDT_30s_full.csv"
DAILY_DIR = "daily"
CHECKPOINT_FILE = "checkpoint.txt"
LOG_FILE = "download.log"
MAX_WORKERS = 8
MAX_RETRIES = 3
DAYS_BACK = 365
END_DATE_OFFSET = 2           # lùi 2 ngày để đảm bảo dữ liệu có sẵn
# ===================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def health_check():
    """Kiểm tra kết nối đến máy chủ dữ liệu Binance."""
    try:
        resp = requests.head("https://data.binance.vision/", timeout=10)
        if resp.status_code == 200:
            return True
        else:
            logger.error(f"Máy chủ Binance trả về mã {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"Không thể kết nối đến Binance: {e}")
        return False

def download_and_process(date_str):
    """Tải file ZIP của một ngày, giải nén và trả về dict candles."""
    url = f"https://data.binance.vision/data/spot/daily/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{date_str}.zip"
    raw_zip = None
    with requests.Session() as session:
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(url, timeout=30)
                if resp.status_code == 200:
                    raw_zip = resp.content
                    break
                elif resp.status_code == 404:
                    return (date_str, None, "404")
                elif resp.status_code == 429:
                    wait = 15 * attempt
                    logger.warning(f"429 {date_str} (lần {attempt}) -> đợi {wait}s")
                    time.sleep(wait)
                else:
                    time.sleep(3)
            except Exception as e:
                logger.warning(f"Lỗi mạng {date_str} (lần {attempt}): {e}")
                time.sleep(5 * attempt)
    if raw_zip is None:
        return (date_str, None, f"Thất bại sau {MAX_RETRIES} lần thử")
    # Giải nén và xây dựng candles
    candles = {}
    try:
        with zipfile.ZipFile(io.BytesIO(raw_zip)) as z:
            file_list = z.namelist()
            if not file_list:
                return (date_str, None, "ZIP rỗng")
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
                    if not is_buyer_maker:   # Taker buy
                        c["tbv"] += qty
                        c["tbqv"] += quote_qty
        return (date_str, candles, None)
    except Exception as e:
        return (date_str, None, str(e))

def save_daily(date_str, candles, last_close):
    """Ghi file CSV cho một ngày, cập nhật checkpoint. Trả về last_close mới."""
    day_dt = datetime.strptime(date_str, "%Y-%m-%d")
    day_start_ts = int(day_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    day_end_ts = day_start_ts + 24 * 60 * 60 * 1000

    daily_file = os.path.join(DAILY_DIR, date_str + ".csv")
    rows = []

    if candles is None:  # fill forward
        if last_close is not None:
            for ts in range(day_start_ts, day_end_ts, 30000):
                dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                rows.append([dt, last_close, last_close, last_close, last_close, 0.0, 0.0, 0, last_close, 0.0, 0.0])
        else:
            logger.warning(f"Bỏ qua {date_str} vì chưa có last_close")
            return last_close
    else:
        for ts in range(day_start_ts, day_end_ts, 30000):
            if ts in candles:
                c = candles[ts]
                last_close = c["c"]
            else:
                if last_close is None:
                    continue
                c = {
                    "o": last_close, "h": last_close, "l": last_close, "c": last_close,
                    "v": 0.0, "qv": 0.0, "n": 0,
                    "vwap_sum": 0.0, "tbv": 0.0, "tbqv": 0.0
                }
            dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            vwap = c["vwap_sum"] / c["v"] if c["v"] > 0 else c["o"]
            rows.append([
                dt,
                c["o"], c["h"], c["l"], c["c"],
                c["v"], c["qv"], c["n"],
                round(vwap, 8),
                c["tbv"], c["tbqv"]
            ])

    os.makedirs(DAILY_DIR, exist_ok=True)
    with open(daily_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Open_Time","Open","High","Low","Close","Volume","Quote_Volume","Trades","VWAP","Taker_Buy_Volume","Taker_Buy_Quote_Volume"])
        writer.writerows(rows)

    with open(CHECKPOINT_FILE, "w") as f:
        f.write(date_str)

    return last_close

def merge_daily_files():
    """Gộp tất cả file daily thành file tổng, có kiểm tra header."""
    if not os.path.exists(DAILY_DIR):
        return
    files = sorted([f for f in os.listdir(DAILY_DIR) if f.endswith(".csv")])
    if not files:
        return

    first_header = None
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        for fname in files:
            with open(os.path.join(DAILY_DIR, fname), "r") as inf:
                reader = csv.reader(inf)
                header = next(reader, None)
                if header is None:
                    continue
                if first_header is None:
                    first_header = header
                    writer.writerow(header)
                elif header != first_header:
                    logger.warning(f"Header của {fname} khác với header chuẩn, vẫn ghi dữ liệu nhưng cần kiểm tra.")
                    # Vẫn ghi dữ liệu dòng (không ghi lại header)
                for row in reader:
                    writer.writerow(row)
    logger.info(f"Đã gộp {len(files)} file daily.")

def verify_output(expected_days):
    """Kiểm tra sơ bộ số dòng của file tổng."""
    if not os.path.exists(OUTPUT_FILE):
        return
    with open(OUTPUT_FILE, "r") as f:
        reader = csv.reader(f)
        next(reader)  # bỏ header
        row_count = sum(1 for _ in reader)
    expected = expected_days * 24 * 60 * 2   # mỗi ngày có 2880 nến 30s
    if abs(row_count - expected) > 10:
        logger.warning(f"Số dòng ({row_count}) lệch nhiều so với dự kiến ({expected}). Có thể có ngày bị thiếu.")
    else:
        logger.info(f"Số dòng phù hợp: {row_count} (dự kiến ~{expected})")

def main():
    start_time = time.time()

    # Health check
    if not health_check():
        logger.error("Dừng chương trình do không kết nối được đến Binance.")
        return

    end_date = datetime.now(timezone.utc) - timedelta(days=END_DATE_OFFSET)
    start_date = end_date - timedelta(days=DAYS_BACK - 1)

    os.makedirs(DAILY_DIR, exist_ok=True)

    # Đọc checkpoint
    last_done_date = None
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            ds = f.read().strip()
            if ds:
                try:
                    last_done_date = datetime.strptime(ds, "%Y-%m-%d").date()
                    logger.info(f"Checkpoint: đã xong đến {last_done_date}")
                except:
                    pass

    resume_date = start_date
    if last_done_date:
        resume_date = last_done_date + timedelta(days=1)
        while os.path.exists(os.path.join(DAILY_DIR, resume_date.strftime("%Y-%m-%d") + ".csv")):
            logger.info(f"Ngày {resume_date} đã có file, bỏ qua.")
            last_done_date = resume_date.date()
            resume_date += timedelta(days=1)
        if last_done_date:
            with open(CHECKPOINT_FILE, "w") as f:
                f.write(last_done_date.strftime("%Y-%m-%d"))

    if resume_date > end_date:
        logger.info("Tất cả các ngày đã được xử lý. Gộp file...")
        merge_daily_files()
        verify_output(DAYS_BACK)
        logger.info(f"Hoàn tất trong {time.time()-start_time:.1f}s")
        return

    dates_to_do = []
    d = resume_date
    while d <= end_date:
        dates_to_do.append(d)
        d += timedelta(days=1)
    total_dates = len(dates_to_do)
    logger.info(f"Bắt đầu tải {total_dates} ngày ({MAX_WORKERS} luồng)")

    # Lấy last_close từ ngày trước resume_date
    last_close = None
    if resume_date > start_date:
        prev_date = resume_date - timedelta(days=1)
        prev_file = os.path.join(DAILY_DIR, prev_date.strftime("%Y-%m-%d") + ".csv")
        if os.path.exists(prev_file):
            try:
                with open(prev_file, "r") as f:
                    last_line = deque(csv.reader(f), maxlen=1)
                    if last_line:
                        last_close = float(last_line[0][4])
            except Exception as e:
                logger.warning(f"Không đọc được last_close từ {prev_file}: {e}")

    # Tải song song
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_date = {}
        for day in dates_to_do:
            future = executor.submit(download_and_process, day.strftime("%Y-%m-%d"))
            future_to_date[future] = day

        done_count = 0
        for future in as_completed(future_to_date):
            day = future_to_date[future]
            try:
                date_str, candles, err = future.result(timeout=600)
                results[day] = (candles, err)
                done_count += 1
                # Giảm nhiễu: chỉ in mốc 10 hoặc hoàn thành
                if done_count % 10 == 0 or done_count == total_dates:
                    logger.info(f"📥 Tiến độ tải: {done_count}/{total_dates} ngày")
            except Exception as e:
                logger.error(f"Worker ngày {day.date()} gặp lỗi: {e}")
                results[day] = (None, f"Worker exception: {e}")
                done_count += 1  # vẫn tính là đã xử lý xong (dù lỗi)

    # Ghi file tuần tự
    for idx, day in enumerate(dates_to_do):
        date_str = day.strftime("%Y-%m-%d")
        if os.path.exists(os.path.join(DAILY_DIR, date_str + ".csv")):
            # Cập nhật last_close từ file sẵn có
            try:
                with open(os.path.join(DAILY_DIR, date_str + ".csv"), "r") as f:
                    last_line = deque(csv.reader(f), maxlen=1)
                    if last_line:
                        last_close = float(last_line[0][4])
            except Exception:
                pass
            continue

        candles, err = results.get(day, (None, "missing"))
        last_close = save_daily(date_str, candles, last_close)
        remaining = total_dates - (idx + 1)
        logger.info(f"✔ {date_str} | còn {remaining} ngày")

    merge_daily_files()
    verify_output(DAYS_BACK)
    elapsed = time.time() - start_time
    logger.info(f"🎉 Hoàn tất! File: {OUTPUT_FILE} | Tổng thời gian: {elapsed:.1f}s")

if __name__ == "__main__":
    main()