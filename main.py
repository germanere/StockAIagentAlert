"""
HOSE Stock Tracker — Direct Google Sheets Writer
==================================================
- Sheet HOSE_Prices: cập nhật giá realtime mỗi 1 phút
- Sheet HOSE_Close: lưu giá đóng cửa mỗi ngày lúc 15:00 ICT
"""

import os, time, logging, json, threading
from datetime import datetime
import pytz
import requests
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.responses import JSONResponse

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

SPREADSHEET_ID      = os.getenv("SPREADSHEET_ID", "")
GOOGLE_CREDS_JSON   = os.getenv("GOOGLE_CREDS_JSON", "")
UPDATE_INTERVAL_MIN = int(os.getenv("UPDATE_INTERVAL_MIN", "1"))
SHEET_NAME          = os.getenv("SHEET_NAME", "HOSE_Prices")
CLOSE_SHEET_NAME    = "HOSE_Close"
ICT                 = pytz.timezone("Asia/Ho_Chi_Minh")

STOCK_SYMBOLS = os.getenv(
    "STOCK_SYMBOLS",
    "ACB,AGG,AGM,AGR,ANV,APC,APH,ASM,AST,BCI,BCM,BFC,BHN,BIC,BID,BMC,BMP,BRC,BSI,BTP,BVH,C4G,CAV,CCI,CDC,CDN,CEE,CHP,CIG,CII,CLC,CLG,CMG,CMX,CNG,CRC,CSM,CTD,CTG,CTI,CTP,DAH,DBC,DCL,DCM,DGC,DGW,DHC,DIC,DIG,DLG,DMC,DPM,DQC,DRC,DRH,DRI,DSN,DTA,DTL,DXG,DXS,EIB,ELC,EVE,EVF,EVG,FCN,FIR,FIT,FPT,FRT,FTS,GAB,GAS,GDT,GEG,GEX,GIL,GMD,GVR,HAG,HAH,HAP,HBC,HCM,HDC,HDG,HHV,HID,HII,HMC,HPG,HPX,HQC,HSG,HTG,HTI,HTL,HTV,HU1,HU3,HVN,HVX,IDC,IDI,IJC,IMP,IPA,ITC,ITD,KBC,KDC,KDH,KHG,KMR,KOS,KSB,LAF,LCG,LDG,LEC,LGL,LHG,LIX,LPB,LSS,MBB,MCH,MCP,MDG,MIG,MML,MSB,MSN,MST,MWG,NAF,NAV,NBB,NCT,NKG,NLG,NNT,NPT,NRC,NSC,NT2,NTL,NVB,NVL,NVT,OCB,OGC,OPC,PAC,PC1,PDN,PDR,PET,PGC,PGD,PGI,PGV,PHR,PIT,PLX,PMG,PNJ,POW,PPC,PPI,PSH,PTB,PTC,PTL,PVD,PVP,PVT,QBS,QCG,RCL,REE,ROS,SAB,SAF,SAM,SAV,SBT,SC5,SCD,SCR,SDN,SFC,SFG,SGN,SHB,SHI,SJS,SKG,SLS,SMB,SMC,SPC,SPM,SRC,SRF,SSB,SSC,SSI,ST8,STB,STG,STK,SVC,SVD,SVT,SZC,TAC,TCB,TCD,TCH,TCM,TDC,TDG,TDH,TDM,TDP,TDW,TGG,THG,TIP,TIX,TLD,TLG,TLH,TMP,TMT,TNA,TNH,TNI,TNT,TPC,TPB,TRC,TRS,TSC,TTA,TTB,TTC,TTF,TV2,TVB,TVS,TYA,UDC,UIC,VBH,VCB,VCF,VCI,VCR,VCS,VDS,VGC,VGI,VGS,VHC,VHM,VIC,VID,VIP,VIX,VJC,VKC,VLB,VMD,VNM,VNS,VOS,VPB,VPG,VPH,VPI,VPS,VRC,VRE,VSC,VSH,VSI,VTB,VTC,VTO,WHS,YEG"
).split(",")

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────

_client_cache = None

def get_client():
    global _client_cache
    if _client_cache:
        return _client_cache
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    _client_cache = gspread.authorize(creds)
    return _client_cache

def get_sheet(name: str):
    spreadsheet = get_client().open_by_key(SPREADSHEET_ID)
    try:
        return spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=name, rows=500, cols=100)
        log.info(f"Đã tạo sheet: {name}")
        return sheet

# ─────────────────────────────────────────────
# FETCH GIÁ — Yahoo Finance
# ─────────────────────────────────────────────

def fetch_yahoo_batch(symbols: list[str]) -> list[dict]:
    tickers = ",".join([f"{s}.VN" for s in symbols])
    url = (
        f"https://query1.finance.yahoo.com/v7/finance/quote"
        f"?symbols={tickers}&fields=symbol,regularMarketPrice,regularMarketOpen,"
        f"regularMarketDayHigh,regularMarketDayLow,regularMarketVolume"
    )
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=15)
        quotes = r.json().get("quoteResponse", {}).get("result", [])
        for q in quotes:
            symbol = q.get("symbol", "").replace(".VN", "")
            price  = q.get("regularMarketPrice", 0)
            if price and price > 0:
                results.append({
                    "symbol": symbol,
                    "price":  float(price),
                    "open":   float(q.get("regularMarketOpen", 0)),
                    "high":   float(q.get("regularMarketDayHigh", 0)),
                    "low":    float(q.get("regularMarketDayLow", 0)),
                    "volume": int(q.get("regularMarketVolume", 0)),
                })
    except Exception as e:
        log.error(f"Yahoo error: {e}")
    return results


def fetch_all() -> list[dict]:
    all_results = []
    for i in range(0, len(STOCK_SYMBOLS), 50):
        batch = STOCK_SYMBOLS[i:i+50]
        all_results.extend(fetch_yahoo_batch(batch))
        time.sleep(0.5)
    return all_results

# ─────────────────────────────────────────────
# GHI REALTIME SHEET
# ─────────────────────────────────────────────

HEADERS = ["symbol", "price", "open", "high", "low", "volume", "change_pct", "updated_at"]

def write_realtime(stocks: list[dict]):
    sheet = get_sheet(SHEET_NAME)
    now   = datetime.now(ICT).strftime("%d/%m/%Y %H:%M:%S")
    rows  = [HEADERS]
    for s in stocks:
        pct = ((s["price"] - s["open"]) / s["open"] * 100) if s["open"] else 0
        rows.append([
            s["symbol"], s["price"], s["open"],
            s["high"], s["low"], s["volume"],
            f"{pct:+.2f}%", now,
        ])
    sheet.clear()
    sheet.update("A1", rows)
    log.info(f"✅ Realtime: ghi {len(stocks)} mã lúc {now}")

# ─────────────────────────────────────────────
# GHI CLOSE PRICE SHEET
# Cấu trúc: cột A = mã CP, mỗi ngày thêm 1 cột mới
# Hàng 1 = header ngày (vd: 09/06/2025)
# ─────────────────────────────────────────────

def write_close_prices(stocks: list[dict]):
    sheet   = get_sheet(CLOSE_SHEET_NAME)
    today   = datetime.now(ICT).strftime("%d/%m/%Y")
    log.info(f"📌 Lưu giá đóng cửa ngày {today}...")

    # Đọc dữ liệu hiện tại
    existing = sheet.get_all_values()

    if not existing:
        # Sheet trống — tạo mới hoàn toàn
        header_row = ["Mã CP", today]
        symbol_map = {s["symbol"]: s["price"] for s in stocks}
        rows = [header_row]
        for symbol in STOCK_SYMBOLS:
            rows.append([symbol, symbol_map.get(symbol, "")])
        sheet.update("A1", rows)
        log.info(f"✅ Close: tạo mới với {len(stocks)} mã ngày {today}")
        return

    # Sheet đã có dữ liệu
    header_row = existing[0]  # ["Mã CP", "08/06/2025", "09/06/2025", ...]

    # Kiểm tra ngày hôm nay đã có chưa
    if today in header_row:
        log.info(f"Ngày {today} đã có trong Close sheet — bỏ qua.")
        return

    # Thêm cột mới cho ngày hôm nay
    new_col_index = len(header_row) + 1  # cột tiếp theo (1-indexed)
    col_letter    = col_num_to_letter(new_col_index)

    # Map symbol → price
    symbol_map = {s["symbol"]: s["price"] for s in stocks}

    # Ghi header ngày mới
    sheet.update(f"{col_letter}1", [[today]])

    # Ghi giá theo đúng thứ tự symbol ở cột A
    existing_symbols = [row[0] for row in existing[1:] if row]
    price_col = [[symbol_map.get(sym, "")] for sym in existing_symbols]
    if price_col:
        sheet.update(f"{col_letter}2", price_col)

    log.info(f"✅ Close: thêm cột {today} ({len(stocks)} mã)")


def col_num_to_letter(n: int) -> str:
    """Chuyển số cột sang chữ cái (1=A, 26=Z, 27=AA...)"""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

# ─────────────────────────────────────────────
# JOBS
# ─────────────────────────────────────────────

def job_realtime():
    log.info(f"⏱ Fetch {len(STOCK_SYMBOLS)} mã...")
    stocks = fetch_all()
    if stocks:
        write_realtime(stocks)
    else:
        log.warning("Không fetch được dữ liệu.")

def job_close():
    """Chạy lúc 15:05 ICT mỗi ngày T2-T6 — lưu giá đóng cửa."""
    now = datetime.now(ICT)
    if now.weekday() > 4:
        return
    log.info("📌 Bắt đầu lưu giá đóng cửa...")
    stocks = fetch_all()
    if stocks:
        write_close_prices(stocks)

# ─────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────

scheduler = BackgroundScheduler(timezone=ICT)
scheduler.add_job(job_realtime, "interval", minutes=UPDATE_INTERVAL_MIN, id="realtime")
scheduler.add_job(job_close,    "cron",     hour=15, minute=5, day_of_week="mon-fri", id="close")
scheduler.start()

# ─────────────────────────────────────────────
# FASTAPI
# ─────────────────────────────────────────────

app = FastAPI(title="HOSE Stock Direct Writer")

@app.get("/")
def root():
    return {
        "status": "running",
        "symbols": len(STOCK_SYMBOLS),
        "interval_min": UPDATE_INTERVAL_MIN,
        "close_price_time": "15:05 ICT mỗi ngày T2-T6",
    }

@app.get("/fetch")
def manual_fetch():
    threading.Thread(target=job_realtime).start()
    return JSONResponse({"status": "fetching", "count": len(STOCK_SYMBOLS)})

@app.get("/close")
def manual_close():
    """Test lưu giá đóng cửa thủ công."""
    threading.Thread(target=job_close).start()
    return JSONResponse({"status": "saving close prices"})

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(ICT).isoformat()}
