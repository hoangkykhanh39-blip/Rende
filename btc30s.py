import csv, time, zipfile, io, os, logging, requests, sys, tempfile
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from collections import deque

# ===== CẤU HÌNH =====
SYMBOL = "BTCUSDT"
OUTPUT_RAW_FILE = "BTCUSDT_30s_raw.csv"
OUTPUT_ULTIMATE_FILE = "BTCUSDT_30s_5Y_Ultimate_Indicators.csv"
DAILY_DIR = "daily"
CHECKPOINT_FILE = "checkpoint.txt"
LOG_FILE = "download.log"
MAX_WORKERS = 10
MAX_RETRIES = 3
YEARS_BACK = 5
END_DATE_OFFSET = 2
BATCH_SIZE = 30           # Số ngày tối đa mỗi lần chạy
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
LOCALE = os.environ.get("LOCALE", "vi")
WORKER_TIMEOUT = 300
# ===================

LANGUAGES = {
    "vi": {
        "health_ok": "✅ Kết nối Binance thành công.",
        "starting": "🚀 Bắt đầu tải {total} ngày ({workers} luồng) – từ {start} đến {end}",
        "progress": "📥 Đã tải: {done}/{total} ngày",
        "save_daily": "✔ {date} | còn {remaining} ngày",
        "complete": "🎉 Hoàn tất! File: {file} | Thời gian: {elapsed:.1f}s",
        "completed_flag": "🏁 Đã tạo completed.flag – workflow sẽ không chạy lại.",
        "error_fatal": "💥 LỖI NGHIÊM TRỌNG: {error}",
        "worker_timeout": "⏰ Worker cho {date} bị timeout sau {timeout}s, hủy bỏ.",
        "batch_done": "📦 Đã xong đợt này, checkpoint đã lưu. Sẽ tiếp tục lần sau."
    },
    "en": {
        "health_ok": "✅ Connected to Binance successfully.",
        "starting": "🚀 Starting download of {total} days ({workers} threads) – from {start} to {end}",
        "progress": "📥 Downloaded: {done}/{total} days",
        "save_daily": "✔ {date} | {remaining} days left",
        "complete": "🎉 Done! File: {file} | Time: {elapsed:.1f}s",
        "completed_flag": "🏁 Completed flag created.",
        "error_fatal": "💥 FATAL ERROR: {error}",
        "worker_timeout": "⏰ Worker for {date} timed out after {timeout}s, cancelling.",
        "batch_done": "📦 Batch finished, checkpoint saved. Will continue next run."
    }
}

def _(key, **kwargs):
    return LANGUAGES.get(LOCALE, LANGUAGES["vi"]).get(key, key).format(**kwargs)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def health_check():
    try:
        resp = requests.head("https://data.binance.vision/", timeout=10)
        if resp.status_code == 200:
            logger.info(_("health_ok"))
            return True
        else:
            logger.error(f"Máy chủ Binance trả về mã {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"Không kết nối được Binance: {e}")
        return False

def download_and_process(date_str):
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
                    wait = min(60, 15 * attempt)
                    logger.warning(f"429 {date_str} (lần {attempt}) -> đợi {wait}s")
                    time.sleep(wait)
                else:
                    time.sleep(3)
            except requests.RequestException as e:
                logger.warning(f"Lỗi mạng {date_str} (lần {attempt}): {e}")
                time.sleep(5 * attempt)
    if raw_zip is None:
        return (date_str, None, f"Thất bại sau {MAX_RETRIES} lần thử")
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
                    if not is_buyer_maker:
                        c["tbv"] += qty
                        c["tbqv"] += quote_qty
        return (date_str, candles, None)
    except zipfile.BadZipFile:
        return (date_str, None, "ZIP hỏng")
    except Exception as e:
        return (date_str, None, str(e))

def is_daily_file_valid(filepath):
    if not os.path.exists(filepath):
        return False
    try:
        with open(filepath, "r") as f:
            reader = csv.reader(f)
            next(reader)
            first_row = next(reader, None)
            return first_row is not None
    except Exception:
        return False

def save_daily(date_str, candles, last_close):
    day_dt = datetime.strptime(date_str, "%Y-%m-%d")
    day_start_ts = int(day_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    day_end_ts = day_start_ts + 24 * 60 * 60 * 1000
    daily_file = os.path.join(DAILY_DIR, date_str + ".csv")
    rows = []
    if candles is None:
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
                c = {"o": last_close, "h": last_close, "l": last_close, "c": last_close,
                     "v": 0.0, "qv": 0.0, "n": 0, "vwap_sum": 0.0, "tbv": 0.0, "tbqv": 0.0}
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
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(date_str)
    return last_close

def merge_daily_files_and_compute_indicators():
    import pandas as pd
    import pandas_ta as ta

    if not os.path.exists(DAILY_DIR):
        return
    files = sorted([f for f in os.listdir(DAILY_DIR) if f.endswith(".csv")])
    if not files:
        return
    logger.info("📊 Đang gộp các file daily và tính chỉ báo...")
    df_list = []
    for fname in files:
        fpath = os.path.join(DAILY_DIR, fname)
        if not is_daily_file_valid(fpath):
            logger.warning(f"File {fname} không hợp lệ, bỏ qua.")
            continue
        df = pd.read_csv(fpath, dtype={
            "Open": "float32", "High": "float32", "Low": "float32",
            "Close": "float32", "Volume": "float32", "Quote_Volume": "float32",
            "VWAP": "float32", "Taker_Buy_Volume": "float32",
            "Taker_Buy_Quote_Volume": "float32"
        })
        df['Open_Time'] = pd.to_datetime(df['Open_Time'], utc=True)
        df.set_index('Open_Time', inplace=True)
        df_list.append(df)

    full_df = pd.concat(df_list).sort_index()
    full_df.rename(columns={
        "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"
    }, inplace=True)

    logger.info("🧮 Đang tính toán chỉ báo kỹ thuật...")
    full_df.ta.ema(length=9, append=True)
    full_df.ta.ema(length=21, append=True)
    full_df.ta.sma(length=50, append=True)
    full_df.ta.sma(length=200, append=True)
    full_df.ta.rsi(length=14, append=True)
    full_df.ta.macd(fast=12, slow=26, signal=9, append=True)
    full_df.ta.stoch(k=14, d=3, smooth_k=3, append=True)
    full_df.ta.cci(length=20, append=True)
    full_df.ta.adx(length=14, append=True)
    full_df.ta.bbands(length=20, std=2, append=True)
    full_df.ta.atr(length=14, append=True)
    full_df.ta.obv(append=True)

    full_df.to_csv(OUTPUT_ULTIMATE_FILE)
    logger.info(f"🎉 Đã tạo file chỉ báo: {OUTPUT_ULTIMATE_FILE}")

def main():
    start_time = time.time()
    try:
        if not health_check():
            logger.error("Dừng do không kết nối được Binance.")
            sys.exit(1)

        end_date = datetime.now(timezone.utc) - timedelta(days=END_DATE_OFFSET)
        start_date = end_date - timedelta(days=YEARS_BACK * 365 + 1)
        total_days = (end_date - start_date).days + 1

        os.makedirs(DAILY_DIR, exist_ok=True)

        # Checkpoint
        last_done_date = None
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r") as f:
                ds = f.read().strip()
                if ds:
                    try:
                        last_done_date = datetime.strptime(ds, "%Y-%m-%d").date()
                        logger.info(f"📌 Checkpoint: đã xong đến {last_done_date}")
                    except:
                        pass

        resume_date = start_date.date()
        if last_done_date:
            resume_date = last_done_date + timedelta(days=1)
            while True:
                daily_file = os.path.join(DAILY_DIR, resume_date.strftime("%Y-%m-%d") + ".csv")
                if os.path.exists(daily_file) and is_daily_file_valid(daily_file):
                    logger.info(f"⏩ Ngày {resume_date} đã có file hợp lệ, bỏ qua.")
                    last_done_date = resume_date
                    resume_date += timedelta(days=1)
                else:
                    if os.path.exists(daily_file):
                        logger.warning(f"File {resume_date} hỏng, sẽ tải lại.")
                    break
            if last_done_date:
                with open(CHECKPOINT_FILE, "w") as f:
                    f.write(last_done_date.strftime("%Y-%m-%d"))

        if resume_date > end_date.date():
            logger.info("✅ Tất cả ngày đã có. Tiến hành tính chỉ báo...")
            if not os.path.exists("completed.flag"):
                merge_daily_files_and_compute_indicators()
                with open("completed.flag", "w") as f:
                    f.write(f"Completed at {datetime.now()}\n")
            return

        # Giới hạn số ngày tải trong lần này (BATCH_SIZE)
        batch_end_date = min(
            end_date,
            datetime.combine(resume_date, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(days=BATCH_SIZE - 1)
        )
        dates_to_do = []
        d = datetime.combine(resume_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        while d <= batch_end_date:
            dates_to_do.append(d)
            d += timedelta(days=1)
        total_todo = len(dates_to_do)

        if total_todo == 0:
            logger.info("Không có ngày nào cần tải trong đợt này.")
            return

        logger.info(_("starting", total=total_todo, workers=MAX_WORKERS,
                      start=resume_date, end=batch_end_date.date()))

        # last_close từ ngày trước
        last_close = None
        if resume_date > start_date.date():
            prev_date = datetime.combine(resume_date, datetime.min.time()) - timedelta(days=1)
            prev_file = os.path.join(DAILY_DIR, prev_date.strftime("%Y-%m-%d") + ".csv")
            if os.path.exists(prev_file) and is_daily_file_valid(prev_file):
                try:
                    with open(prev_file, "r") as f:
                        last_line = deque(csv.reader(f), maxlen=1)
                        if last_line:
                            last_close = float(last_line[0][4])
                except Exception as e:
                    logger.warning(f"Không đọc được last_close từ {prev_file}: {e}")

        results = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_date = {}
            for dt in dates_to_do:
                future = executor.submit(download_and_process, dt.strftime("%Y-%m-%d"))
                future_to_date[future] = dt

            done = 0
            for future in as_completed(future_to_date):
                dt = future_to_date[future]
                try:
                    date_str, candles, err = future.result(timeout=WORKER_TIMEOUT)
                    results[dt] = (candles, err)
                except TimeoutError:
                    logger.error(_("worker_timeout", date=dt.date(), timeout=WORKER_TIMEOUT))
                    results[dt] = (None, f"Timeout sau {WORKER_TIMEOUT}s")
                except Exception as e:
                    logger.error(f"Worker ngày {dt.date()} gặp lỗi: {e}")
                    results[dt] = (None, str(e))
                done += 1
                if done % 10 == 0 or done == total_todo:
                    logger.info(_("progress", done=done, total=total_todo))

        for idx, dt in enumerate(dates_to_do):
            date_str = dt.strftime("%Y-%m-%d")
            daily_file = os.path.join(DAILY_DIR, date_str + ".csv")
            if os.path.exists(daily_file) and is_daily_file_valid(daily_file):
                try:
                    with open(daily_file, "r") as f:
                        last_line = deque(csv.reader(f), maxlen=1)
                        if last_line:
                            last_close = float(last_line[0][4])
                except:
                    pass
                continue
            candles, err = results.get(dt, (None, "missing"))
            last_close = save_daily(date_str, candles, last_close)
            remaining = total_todo - (idx + 1)
            logger.info(_("save_daily", date=date_str, remaining=remaining))

        logger.info(_("batch_done"))

        if batch_end_date >= end_date:
            merge_daily_files_and_compute_indicators()
            with open("completed.flag", "w") as f:
                f.write(f"Completed at {datetime.now()}\n")
            logger.info(_("completed_flag"))

    except Exception as e:
        logger.exception(_("error_fatal", error=e))
        sys.exit(1)

if __name__ == "__main__":
    main()
