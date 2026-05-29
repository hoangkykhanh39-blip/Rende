import csv, time, zipfile, io, os, logging, requests, sys, tempfile
from datetime import datetime, timedelta, timezone
from collections import deque
import multiprocessing as mp

# ===== CẤU HÌNH =====
SYMBOL = "BTCUSDT"
OUTPUT_ULTIMATE_FILE = "BTCUSDT_30s_5Y_Ultimate_Indicators.csv"
DAILY_DIR = "daily"
CHECKPOINT_FILE = "checkpoint.txt"
LOG_FILE = "download.log"
MAX_RETRIES = 3
YEARS_BACK = 3                  # ← ĐÃ SỬA TỪ 5 THÀNH 3 NĂM
END_DATE_OFFSET = 2
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
LOCALE = os.environ.get("LOCALE", "vi")
PROCESS_TIMEOUT = 300   # 5 phút cho mỗi ngày
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

# Hàm này sẽ chạy trong tiến trình con
def process_day(date_str, result_queue):
    try:
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
                        result_queue.put((date_str, None, "404"))
                        return
                    elif resp.status_code == 429:
                        wait = min(60, 15 * attempt)
                        time.sleep(wait)
                    else:
                        time.sleep(3)
                except requests.RequestException as e:
                    time.sleep(5 * attempt)
        if raw_zip is None:
            result_queue.put((date_str, None, f"Thất bại sau {MAX_RETRIES} lần thử"))
            return

        # Giải nén & xử lý
        candles = {}
        try:
            with zipfile.ZipFile(io.BytesIO(raw_zip)) as z:
                file_list = z.namelist()
                if not file_list:
                    result_queue.put((date_str, None, "ZIP rỗng"))
                    return
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
            result_queue.put((date_str, candles, None))
        except zipfile.BadZipFile:
            result_queue.put((date_str, None, "ZIP hỏng"))
        except Exception as e:
            result_queue.put((date_str, None, str(e)))
    except Exception as e:
        result_queue.put((date_str, None, f"Lỗi tiến trình: {e}"))

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

        # Chuẩn bị danh sách tất cả ngày còn thiếu
        dates_to_do = []
        d = datetime.combine(resume_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        while d <= end_date:
            dates_to_do.append(d)
            d += timedelta(days=1)
        total_todo = len(dates_to_do)

        logger.info(_("starting", total=total_todo, start=resume_date, end=end_date.date()))

        # Lấy last_close từ ngày trước đó
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

        # Xử lý từng ngày một bằng Process riêng biệt
        for idx, dt in enumerate(dates_to_do):
            date_str = dt.strftime("%Y-%m-%d")
            daily_file = os.path.join(DAILY_DIR, date_str + ".csv")
            if os.path.exists(daily_file) and is_daily_file_valid(daily_file):
                # Cập nhật last_close từ file này
                try:
                    with open(daily_file, "r") as f:
                        last_line = deque(csv.reader(f), maxlen=1)
                        if last_line:
                            last_close = float(last_line[0][4])
                except:
                    pass
                continue

            # Tạo queue để nhận kết quả từ tiến trình con
            result_queue = mp.Queue()
            p = mp.Process(target=process_day, args=(date_str, result_queue))
            p.start()
            p.join(PROCESS_TIMEOUT)

            if p.is_alive():
                # Timeout
                p.terminate()
                p.join()
                logger.error(_("process_timeout", date=date_str))
                # Coi như lỗi, fill forward
                last_close = save_daily(date_str, None, last_close)
            else:
                # Đọc kết quả từ queue
                try:
                    _, candles, err = result_queue.get_nowait()
                except:
                    candles, err = None, "Lỗi không xác định"
                last_close = save_daily(date_str, candles, last_close)
                if err:
                    logger.warning(f"   {date_str} lỗi: {err}")

            remaining = total_todo - (idx + 1)
            logger.info(_("progress", date=date_str, done=idx+1, total=total_todo, remaining=remaining))

        # Sau khi đã xử lý hết các ngày còn lại
        merge_daily_files_and_compute_indicators()
        with open("completed.flag", "w") as f:
            f.write(f"Completed at {datetime.now()}\n")
        logger.info(_("completed_flag"))

        elapsed = time.time() - start_time
        logger.info(_("complete", file=OUTPUT_ULTIMATE_FILE, elapsed=elapsed))

    except Exception as e:
        logger.exception(_("error_fatal", error=e))
        sys.exit(1)

if __name__ == "__main__":
    main()
