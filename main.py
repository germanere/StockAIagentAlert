"""
HOSE Stock Tracker — Direct Google Sheets Writer
==================================================
Nguồn dữ liệu: Yahoo Finance (không chặn IP nước ngoài)
"""

import os, time, logging, json, threading
from datetime import datetime
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

_sheet_cache = None

def get_sheet():
    global _sheet_cache
    if _sheet_cache:
        return _sheet_cache
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=500, cols=10)
        log.info(f"Đã tạo sheet: {SHEET_NAME}")
    _sheet_cache = sheet
    return sheet

# ─────────────────────────────────────────────
# FETCH GIÁ — Yahoo Finance (không bị chặn IP)
# ─────────────────────────────────────────────

def fetch_yahoo_batch(symbols: list[str]) -> list[dict]:
    """Fetch nhiều mã cùng lúc — Yahoo Finance API, không bị chặn IP nước ngoài."""
    # Yahoo Finance dùng suffix .VN cho cổ phiếu Việt Nam
    tickers = ",".join([f"{s}.VN" for s in symbols])
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={tickers}&fields=symbol,regularMarketPrice,regularMarketOpen,regularMarketDayHigh,regularMarketDayLow,regularMarketVolume"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    results = []
    try:
        r = requests.get(url, headers=headers, timeout=15)
        data = r.json()
        quotes = data.get("quoteResponse", {}).get("result", [])
        for q in quotes:
            raw_symbol = q.get("symbol", "").replace(".VN", "")
            price = q.get("regularMarketPrice", 0)
            if price and price > 0:
                results.append({
                    "symbol": raw_symbol,
                    "price":  float(price),
                    "open":   float(q.get("regularMarketOpen", 0)),
                    "high":   float(q.get("regularMarketDayHigh", 0)),
                    "low":    float(q.get("regularMarketDayLow", 0)),
                    "volume": int(q.get("regularMarketVolume", 0)),
                })
        log.info(f"Yahoo: fetch được {len(results)}/{len(symbols)} mã")
    except Exception as e:
        log.error(f"Yahoo batch error: {e}")
    return results


def fetch_all() -> list[dict]:
    """Chia batch 50 mã/request để tránh quá dài URL."""
    all_results = []
    batch_size = 50
    for i in range(0, len(STOCK_SYMBOLS), batch_size):
        batch = STOCK_SYMBOLS[i:i+batch_size]
        results = fetch_yahoo_batch(batch)
        all_results.extend(results)
        time.sleep(0.5)
    return all_results

# ─────────────────────────────────────────────
# GHI GOOGLE SHEETS
# ─────────────────────────────────────────────

HEADERS = ["symbol", "price", "open", "high", "low", "volume", "change_pct", "updated_at"]

def write_to_sheet(stocks: list[dict]):
    sheet = get_sheet()
    now   = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    rows  = [HEADERS]
    for s in stocks:
        pct = ((s["price"] - s["open"]) / s["open"] * 100) if s["open"] else 0
        rows.append([
            s["symbol"],
            s["price"],
            s["open"],
            s["high"],
            s["low"],
            s["volume"],
            f"{pct:+.2f}%",
            now,
        ])
    sheet.clear()
    sheet.update("A1", rows)
    log.info(f"✅ Ghi {len(stocks)} mã vào Sheets lúc {now}")

# ─────────────────────────────────────────────
# KIỂM TRA GIỜ GIAO DỊCH
# ─────────────────────────────────────────────

def is_trading_hours() -> bool:
    now = datetime.now()
    if now.weekday() > 4:
        return False
    if now.hour < 2 or now.hour >= 8:   # UTC+0: 9:00-15:00 ICT = 2:00-8:00 UTC
        return False
    return True

# ─────────────────────────────────────────────
# JOB
# ─────────────────────────────────────────────

def run_job():
    if not is_trading_hours():
        log.info("Ngoài giờ giao dịch — bỏ qua.")
        return
    log.info(f"⏱ Fetch {len(STOCK_SYMBOLS)} mã...")
    stocks = fetch_all()
    if stocks:
        write_to_sheet(stocks)
    else:
        log.warning("Không fetch được dữ liệu.")

# ─────────────────────────────────────────────
# SCHEDULER + APP
# ─────────────────────────────────────────────

scheduler = BackgroundScheduler(timezone="UTC")
scheduler.add_job(run_job, "interval", minutes=UPDATE_INTERVAL_MIN)
scheduler.start()

app = FastAPI(title="HOSE Stock Direct Writer")

@app.get("/")
def root():
    return {"status": "running", "symbols": len(STOCK_SYMBOLS), "interval_min": UPDATE_INTERVAL_MIN}

@app.get("/fetch")
def manual_fetch():
    threading.Thread(target=run_job).start()
    return JSONResponse({"status": "fetching", "count": len(STOCK_SYMBOLS)})

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}
