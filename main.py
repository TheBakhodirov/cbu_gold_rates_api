from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone, timedelta
import json
import os
import threading
from typing import Optional

app = FastAPI(title="CBU Gold Prices API")

CBU_URL = os.getenv("CBU_URL", "https://cbu.uz/uz/banknotes-coins/gold-bars/prices/")
REQUEST_TIMEOUT = 8  # seconds
CACHE_FILE = "cache.json"
CACHE_TTL = timedelta(hours=24)  # 24 hours cache
UZB_TZ = timezone(timedelta(hours=5))

cache_lock = threading.Lock()

def normalize_digits(s: str) -> int:
    digits = re.findall(r"\d+", s.replace("\u00a0", " "))
    return int("".join(digits)) if digits else 0

def parse_table(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("h1 ~ table.table-bordered") or soup.find("table", class_=lambda c: c and "table-bordered" in c)
    if table is None:
        return None, None

    date_pattern = re.compile(r"(\d{1,2}\.\d{1,2}\.\d{4})")
    last_updated = None
    for el in table.find_all(text=True):
        m = date_pattern.search(el)
        if m:
            last_updated = m.group(1)
            break

    rows = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        first_text = tds[0].get_text(" ", strip=True)
        if re.search(r"og'ir|ogʻir|ogir|sotish|narx", first_text, flags=re.I):
            continue

        wg_match = re.search(r"(\d+)", first_text)
        if not wg_match:
            continue
        weight_g = int(wg_match.group(1))
        price_text = tds[1].get_text(" ", strip=True) if len(tds) > 1 else ""
        price_uzs = normalize_digits(price_text)

        rows.append({
            "weight_g": weight_g,
            "price_uzs": price_uzs,
            "price_str": price_text
        })

    rows = sorted(rows, key=lambda r: r["weight_g"])
    return last_updated, rows

def fetch_and_parse():
    resp = requests.get(CBU_URL, timeout=REQUEST_TIMEOUT, headers={
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36"
    })
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Source returned status {resp.status_code}")
    last_updated, rows = parse_table(resp.text)
    if rows is None:
        raise HTTPException(status_code=500, detail="Could not find gold table on source page")

    now = datetime.now(UZB_TZ)
    return {
        "source": CBU_URL,
        "last_updated": last_updated or None,
        "retrieved_at": now.isoformat(),
        "prices": rows
    }

def load_cache():
    if not os.path.exists(CACHE_FILE):
        return None
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        expires_at = datetime.fromisoformat(data.get("expires_at"))
        if datetime.now(UZB_TZ) < expires_at:
            return data
        else:
            return None  # expired
    except Exception:
        return None

def save_cache(result: dict):
    expires_at = datetime.now(UZB_TZ) + CACHE_TTL
    data = {**result, "expires_at": expires_at.isoformat()}
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return expires_at

def clear_cache():
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)

@app.get("/gold", summary="CBU gold bars prices")
def get_gold_prices(force: bool = Query(False, description="Force re-scrape and refresh cache")):
    with cache_lock:
        if force:
            clear_cache()

        cached_data = load_cache()
        if cached_data:
            return JSONResponse(content={**cached_data, "cache": {"status": "hit", "expires_at": cached_data["expires_at"]}})

    try:
        result = fetch_and_parse()
    except Exception as e:
        with cache_lock:
            cached_data = load_cache()
            if cached_data:
                return JSONResponse(content={
                    **cached_data,
                    "warning": f"Failed to fetch fresh data: {str(e)}. Returning cached data.",
                    "cache": {"status": "stale", "expires_at": cached_data["expires_at"]}
                })
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=502, detail=f"Failed to fetch source: {e}")

    with cache_lock:
        expires_at = save_cache(result)

    return JSONResponse(content={**result, "cache": {"status": "stored", "expires_at": expires_at.isoformat()}})

@app.get("/")
def root():
    return {
        "message": "Welcome to the CBU Gold Prices API 🇺🇿",
        "routes": ["/gold", "/cache/status", "/health"],
        "source": CBU_URL
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/cache/status", summary="Check cache status")
def cache_status():
    with cache_lock:
        if not os.path.exists(CACHE_FILE):
            return {"cache": {"status": "missing", "message": "No cache file found."}}
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            expires_at = datetime.fromisoformat(data.get("expires_at"))
            now = datetime.now(UZB_TZ)
            if now >= expires_at:
                status = "expired"
            else:
                status = "valid"
            return {
                "cache": {
                    "status": status,
                    "expires_at": expires_at.isoformat(),
                    "age_remaining_sec": int((expires_at - now).total_seconds())
                }
            }
        except Exception as e:
            return {"cache": {"status": "error", "message": str(e)}}
