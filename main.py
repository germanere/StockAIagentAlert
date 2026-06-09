"""
HOSE Stock Webhook Server
==========================
- Chạy trên Render / Railway (free tier)
- Cứ mỗi lần Zapier Schedule trigger → gọi GET /fetch
- Server lấy giá HOSE → gửi POST đến Zapier Webhook URL
"""

import os
import time
import logging
import requests
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler

# ─────────────────────────────────────────────
# CONFIG (set qua Environment Variables trên Render/Railway)
# ─────────────────────────────────────────────

ZAPIER_WEBHOOK_URL = os.getenv("ZAPIER_WEBHOOK_URL", "")   # paste URL từ Zapier
UPDATE_INTERVAL_MIN = int(os.getenv("UPDATE_INTERVAL_MIN", "5"))

STOCK_SYMBOLS = os.getenv(
    "STOCK_SYMBOLS",
    "VNM,VIC,VHM,VCB,BID,CTG,TCB,MWG,FPT,HPG,VRE,MSN,GAS,SAB,PLX"
).split(",")

# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────

app = FastAPI(title="HOSE Stock Webhook Server")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# FETCH GIÁ
# ─────────────────────────────────────────────

def fetch_price_tcbs(symbol: str) -> dict | None:
    """Lấy giá từ TCBS (nguồn chính, free, không cần key)."""
    url = (
        f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/"
        f"second-chart?ticker={symbol}&type=stock"
    )
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        data = r.json()
        if data.get("data"):
            d = data["data"][-1]
            return {
                "symbol": symbol,
                "price": d.get("close", 0),
                "open":  d.get("open", 0),
                "high":  d.get("high", 0),
                "low":   d.get("low", 0),
                "volume": d.get("volume", 0),
            }
    except Exception as e:
        log.warning(f"TCBS {symbol}: {e}")
    return None


def fetch_price_vndirect(symbol: str) -> dict | None:
    """Backup: VNDirect API."""
    url = f"https://finfo-api.vndirect.com.vn/v4/stocks?q=codeList:{symbol}&size=1"
    try:
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.vndirect.com.vn/"},
            timeout=8
        )
        items = r.json().get("data", [])
        if items:
            i = items[0]
            return {
                "symbol": symbol,
                "price":  float(i.get("matchPrice", 0)),
                "open":   float(i.get("openPrice", 0)),
                "high":   float(i.get("highPrice", 0)),
                "low":    float(i.get("lowPrice", 0)),
                "volume": int(i.get("nmTotalTradedQty", 0)),
            }
    except Exception as e:
        log.warning(f"VNDirect {symbol}: {e}")
    return None


def fetch_all() -> list[dict]:
    results = []
    for symbol in STOCK_SYMBOLS:
        data = fetch_price_tcbs(symbol) or fetch_price_vndirect(symbol)
        if data:
            results.append(data)
        time.sleep(0.3)
    return results


# ─────────────────────────────────────────────
# GỬI DATA ĐẾN ZAPIER
# ─────────────────────────────────────────────

def push_to_zapier(stocks: list[dict]):
    """Gửi từng mã CP thành 1 request riêng đến Zapier webhook."""
    if not ZAPIER_WEBHOOK_URL:
        log.error("ZAPIER_WEBHOOK_URL chưa được set!")
        return

    success = 0
    for stock in stocks:
        payload = {
            "symbol":    stock["symbol"],
            "price":     f"{stock['price']:,.0f}",
            "open":      f"{stock['open']:,.0f}",
            "high":      f"{stock['high']:,.0f}",
            "low":       f"{stock['low']:,.0f}",
            "volume":    f"{stock['volume']:,}",
            "timestamp": time.strftime("%d/%m/%Y %H:%M:%S"),
        }
        try:
            r = requests.post(ZAPIER_WEBHOOK_URL, json=payload, timeout=10)
            if r.status_code == 200:
                success += 1
            else:
                log.warning(f"Zapier {stock['symbol']}: HTTP {r.status_code}")
        except Exception as e:
            log.error(f"Push {stock['symbol']}: {e}")
        time.sleep(0.2)

    log.info(f"✅ Pushed {success}/{len(stocks)} symbols to Zapier")


# ─────────────────────────────────────────────
# JOB CHÍNH
# ─────────────────────────────────────────────

def run_job():
    log.info("⏱ Running scheduled stock fetch...")
    stocks = fetch_all()
    if stocks:
        push_to_zapier(stocks)
    else:
        log.warning("Không fetch được dữ liệu.")


# ─────────────────────────────────────────────
# SCHEDULER (tự chạy không cần Zapier trigger)
# ─────────────────────────────────────────────

scheduler = BackgroundScheduler()
scheduler.add_job(run_job, "interval", minutes=UPDATE_INTERVAL_MIN)
scheduler.start()


# ─────────────────────────────────────────────
# API ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "running", "symbols": STOCK_SYMBOLS}


@app.get("/fetch")
def manual_fetch(background_tasks: BackgroundTasks):
    """Trigger thủ công — Zapier Schedule cũng gọi endpoint này."""
    background_tasks.add_task(run_job)
    return JSONResponse({"status": "fetching", "symbols": len(STOCK_SYMBOLS)})


@app.get("/health")
def health():
    return {"status": "ok"}
