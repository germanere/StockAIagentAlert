"""
HOSE Stock Tracker — SSI API
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────

_client_cache = None

def get_client():
    global _client_cache
    if _client_cache:
        return _client_cache
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), scopes=scopes)
    _client_cache = gspread.authorize(creds)
    return _client_cache

def get_sheet(name: str):
    ss = get_client().open_by_key(SPREADSHEET_ID)
    try:
        return ss.worksheet(name)
    except gspread.WorksheetNotFound:
        s = ss.add_worksheet(title=name, rows=500, cols=100)
        log.info(f"Tạo sheet: {name}")
        return s

# ─────────────────────────────────────────────
# FETCH GIÁ — SSI API (không bị chặn IP)
# ─────────────────────────────────────────────

def fetch_ssi_batch(symbols: list[str]) -> list[dict]:
    """SSI Securities API — lấy giá theo batch, không bị chặn IP nước ngoài."""
    results = []
    # SSI API lấy toàn bộ mã HOSE trong 1 request
    url = "https://iboard-query.ssi.com.vn/v2/stock/exchange/HOSE/snapshot"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Origin": "https://iboard.ssi.com.vn",
        "Referer": "https://iboard.ssi.com.vn/",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        data = r.json()
        items = data.get("data", [])
        symbol_set = set(symbols)
        for item in items:
            sym = item.get("ticker", "")
            if sym not in symbol_set:
                continue
            price = float(item.get("lastPrice", 0) or 0)
            open_ = float(item.get("openPrice", 0) or 0)
            high  = float(item.get("highPrice", 0) or 0)
            low   = float(item.get("lowPrice", 0) or 0)
            vol   = int(item.get("totalVolume", 0) or 0)
            if price > 0:
                results.append({
                    "symbol": sym,
                    "price":  price,
                    "open":   open_,
                    "high":   high,
                    "low":    low,
                    "volume": vol,
                })
        log.info(f"SSI: fetch được {len(results)}/{len(symbols)} mã")
    except Exception as e:
        log.error(f"SSI error: {e}")
    return results


def fetch_all() -> list[dict]:
    # SSI trả về toàn bộ HOSE trong 1 request — không cần batch
    return fetch_ssi_batch(STOCK_SYMBOLS)

# ─────────────────────────────────────────────
# GHI REALTIME
# ─────────────────────────────────────────────

HEADERS = ["symbol", "price", "open", "high", "low", "volume", "change_pct", "updated_at"]

def write_realtime(stocks: list[dict]):
    sheet = get_sheet(SHEET_NAME)
    now   = datetime.now(ICT).strftime("%d/%m/%Y %H:%M:%S")
    rows  = [HEADERS]
    for s in stocks:
        pct = ((s["price"] - s["open"]) / s["open"] * 100) if s["open"] else 0
        rows.append([s["symbol"], s["price"], s["open"], s["high"], s["low"], s["volume"], f"{pct:+.2f}%", now])
    sheet.clear()
    sheet.update("A1", rows)
    log.info(f"✅ Realtime: ghi {len(stocks)} mã lúc {now}")

# ─────────────────────────────────────────────
# GHI GIÁ ĐÓNG CỬA
# ─────────────────────────────────────────────

def col_num_to_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result

def write_close_prices(stocks: list[dict]):
    sheet  = get_sheet(CLOSE_SHEET_NAME)
    today  = datetime.now(ICT).strftime("%d/%m/%Y")
    log.info(f"📌 Lưu giá đóng cửa ngày {today}...")
    existing = sheet.get_all_values()
    symbol_map = {s["symbol"]: s["price"] for s in stocks}

    if not existing:
        rows = [["Mã CP", today]]
        for sym in STOCK_SYMBOLS:
            rows.append([sym, symbol_map.get(sym, "")])
        sheet.update("A1", rows)
        log.info(f"✅ Close: tạo mới {len(stocks)} mã ngày {today}")
        return

    header_row = existing[0]
    if today in header_row:
        log.info(f"Ngày {today} đã có — bỏ qua.")
        return

    new_col = len(header_row) + 1
    col_letter = col_num_to_letter(new_col)
    sheet.update(f"{col_letter}1", [[today]])
    existing_symbols = [row[0] for row in existing[1:] if row]
    price_col = [[symbol_map.get(sym, "")] for sym in existing_symbols]
    if price_col:
        sheet.update(f"{col_letter}2", price_col)
    log.info(f"✅ Close: thêm cột {today} ({len(stocks)} mã)")

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
    if datetime.now(ICT).weekday() > 4:
        return
    stocks = fetch_all()
    if stocks:
        write_close_prices(stocks)

# ─────────────────────────────────────────────
# SCHEDULER + APP
# ─────────────────────────────────────────────

scheduler = BackgroundScheduler(timezone=ICT)
scheduler.add_job(job_realtime, "interval", minutes=UPDATE_INTERVAL_MIN, id="realtime")
scheduler.add_job(job_close, "cron", hour=15, minute=5, day_of_week="mon-fri", id="close")
scheduler.start()

app = FastAPI(title="HOSE Stock Direct Writer")

@app.get("/")
def root():
    return {"status": "running", "symbols": len(STOCK_SYMBOLS), "interval_min": UPDATE_INTERVAL_MIN}

@app.get("/fetch")
def manual_fetch():
    threading.Thread(target=job_realtime).start()
    return JSONResponse({"status": "fetching", "count": len(STOCK_SYMBOLS)})

@app.get("/close")
def manual_close():
    threading.Thread(target=job_close).start()
    return JSONResponse({"status": "saving close prices"})

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(ICT).isoformat()}
