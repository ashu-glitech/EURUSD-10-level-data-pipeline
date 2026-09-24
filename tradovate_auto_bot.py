import os
import gc
import glob
import time
import json
import shutil
import zipfile
import threading
import websocket
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn
from huggingface_hub import HfApi, create_repo

# ==============================================================================
# 🚀 TRADOVATE 24/7 AUTO-LOGIN & ZERO-GAP CLOUD RECORDER (Nifty-Proven Formula)
# 🛡️ Bulletproof: Chunked saves + ZIP upload. Restart = Zero Data Loss.
# ==============================================================================

HF_TOKEN   = os.getenv("HF_TOKEN", "")
HF_REPO_ID = os.getenv("HF_REPO_ID", "ashutickdata/Euro-usd-tick-data")

DATA_DIR = "data"
BUFFER_LIMIT = 50
UPLOAD_INTERVAL_SECONDS = 1800

buffer_lock = threading.Lock()
tick_buffer = []
last_hf_upload_time = time.time()

live_state = {
    "status":         "ACTIVE 🟢",
    "hf_sync_status": "READY ☁️" if HF_TOKEN else "SET HF_TOKEN IN ENV",
    "last_update":    "N/A",
    "total_snapshots": 0,
    "current_price":  0.0,
}

# ==============================================================================
# 🗂️ HELPER: Get today's trading date string (IST, 3:30 AM rollover)
# ==============================================================================
def get_trading_date_str():
    ist_time = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    if ist_time.hour < 3 or (ist_time.hour == 3 and ist_time.minute < 30):
        trade_date = ist_time - timedelta(days=1)
    else:
        trade_date = ist_time
    return trade_date.strftime("%Y-%m-%d")

def get_today_data_dir():
    date_str = get_trading_date_str()
    path = os.path.join(DATA_DIR, date_str)
    os.makedirs(path, exist_ok=True)
    return path, date_str

# ==============================================================================
# 💾 SAVE: Nifty-style — each chunk = its own timestamped file (NEVER overwrites)
# ==============================================================================
def save_parquet_chunk():
    global tick_buffer
    with buffer_lock:
        if not tick_buffer:
            return
        df = pd.DataFrame(tick_buffer)
        tick_buffer = []
    try:
        date_dir, _ = get_today_data_dir()
        ts = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%Y%m%d_%H%M%S_%f")
        filename = os.path.join(date_dir, f"tradovate_6EZ6_chunk_{ts}.parquet")
        df.to_parquet(filename, engine='pyarrow', index=False)
        print(f"💾 Saved {len(df)} rows → {filename}", flush=True)
    except Exception as e:
        print(f"❌ Parquet save error: {e}", flush=True)
    finally:
        if 'df' in locals():
            del df
        gc.collect()
        pa.default_memory_pool().release_unused()

# ==============================================================================
# 📦 ZIP: Merge all today's chunks → single zip file
# ==============================================================================
def create_daily_zip(date_str=None):
    if date_str is None:
        date_str = get_trading_date_str()
    date_dir = os.path.join(DATA_DIR, date_str)
    if not os.path.exists(date_dir):
        return None
    chunk_files = sorted(glob.glob(os.path.join(date_dir, "*.parquet")))
    if not chunk_files:
        return None
    try:
        merged_parquet = os.path.join(date_dir, f"tradovate_6EZ6_live_data_{date_str}.parquet")
        tables = [pq.read_table(f) for f in chunk_files]
        merged = pa.concat_tables(tables)
        pq.write_table(merged, merged_parquet)
        zip_path = os.path.join(DATA_DIR, f"tradovate_6EZ6_live_data_{date_str}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(merged_parquet, arcname=os.path.basename(merged_parquet))
        print(f"📦 ZIP created: {zip_path} ({len(chunk_files)} chunks, {merged.num_rows} rows)", flush=True)
        return zip_path
    except Exception as e:
        print(f"❌ ZIP error: {e}", flush=True)
        return None

# ==============================================================================
# ☁️ UPLOAD: ZIP → Hugging Face (daily_vault/)
# ==============================================================================
def upload_zip_to_huggingface(zip_path=None, date_str=None):
    if not HF_TOKEN:
        live_state["hf_sync_status"] = "SET HF_TOKEN IN ENV ⚠️"
        return
    if zip_path is None:
        zip_path = create_daily_zip(date_str)
    if not zip_path or not os.path.exists(zip_path):
        return
    try:
        api = HfApi(token=HF_TOKEN)
        create_repo(repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN, private=True, exist_ok=True)
        zip_name = os.path.basename(zip_path)
        api.upload_file(
            path_or_fileobj=zip_path,
            path_in_repo=f"daily_vault/{zip_name}",
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            token=HF_TOKEN
        )
        ts = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%H:%M:%S")
        print(f"[{ts}] 🚀 [HF SYNC] ZIP uploaded: {zip_name}", flush=True)
        live_state["hf_sync_status"] = f"SYNCED ({ts}) 🟢"
    except Exception as e:
        print(f"⚠️ HF Upload Failed: {e}", flush=True)
        live_state["hf_sync_status"] = "ERROR ❌"

# ==============================================================================
# 🔄 RESUME: On startup, download today's ZIP from HF and extract chunks
# ==============================================================================
def resume_from_hf():
    if not HF_TOKEN or not HF_REPO_ID:
        print("ℹ️ HF_TOKEN not set — starting fresh.", flush=True)
        return
    date_str = get_trading_date_str()
    date_dir = os.path.join(DATA_DIR, date_str)
    os.makedirs(date_dir, exist_ok=True)

    existing_chunks = glob.glob(os.path.join(date_dir, "tradovate_6EZ6_chunk_*.parquet"))
    if existing_chunks:
        print(f"✅ {len(existing_chunks)} chunk(s) already on disk for {date_str}. Resuming!", flush=True)
        return

    zip_name = f"tradovate_6EZ6_live_data_{date_str}.zip"
    try:
        from huggingface_hub import hf_hub_download
        print(f"🔄 Checking HF for {zip_name}...", flush=True)
        zip_path_hf = hf_hub_download(
            repo_id=HF_REPO_ID,
            filename=f"daily_vault/{zip_name}",
            repo_type="dataset",
            token=HF_TOKEN
        )
        local_zip = os.path.join(DATA_DIR, zip_name)
        shutil.copy(zip_path_hf, local_zip)
        with zipfile.ZipFile(local_zip, 'r') as zf:
            zf.extractall(date_dir)
        extracted = glob.glob(os.path.join(date_dir, "*.parquet"))
        print(f"✅ Resumed from HF! Extracted {len(extracted)} file(s) for {date_str}", flush=True)
    except Exception as e:
        print(f"ℹ️ No ZIP on HF for {date_str}: {e}. Starting fresh.", flush=True)

# ==============================================================================
# 🔁 BACKGROUND MONITOR: Auto zip+upload every 30 mins
# ==============================================================================
def background_monitor():
    global last_hf_upload_time
    while True:
        time.sleep(60)
        try:
            if time.time() - last_hf_upload_time >= UPLOAD_INTERVAL_SECONDS:
                save_parquet_chunk()
                threading.Thread(target=upload_zip_to_huggingface, daemon=True).start()
                last_hf_upload_time = time.time()
        except Exception as e:
            print(f"⚠️ Monitor error: {e}", flush=True)

# ==============================================================================
# 📊 FASTAPI DASHBOARD
# ==============================================================================
app = FastAPI(title="EUR/USD Tradovate Live Collector")

def run_fastapi():
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)

def get_today_stats():
    date_str = get_trading_date_str()
    date_dir = os.path.join(DATA_DIR, date_str)
    chunks = glob.glob(os.path.join(date_dir, "tradovate_6EZ6_chunk_*.parquet")) if os.path.exists(date_dir) else []
    total_rows = 0
    for c in chunks:
        try:
            total_rows += pq.read_metadata(c).num_rows
        except:
            pass
    return date_str, len(chunks), total_rows

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def dashboard():
    date_str, num_chunks, total_rows = get_today_stats()
    price = live_state.get("current_price", 0.0)
    price_str = f"{price:.5f}" if price else "Loading..."
    hf_status = live_state.get("hf_sync_status", "N/A")
    last_up = live_state.get("last_update", "N/A")
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
    <meta http-equiv="refresh" content="10">
    <title>EUR/USD Live</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;700;800&display=swap" rel="stylesheet">
    <style>*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:'Inter',sans-serif;background:#080c14;color:#c9d1d9;padding:20px}}.wrap{{max-width:900px;margin:0 auto}}h1{{font-size:26px;font-weight:800;color:#58a6ff;margin-bottom:4px}}.sub{{color:#8b949e;font-size:12px;margin-bottom:20px}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;margin-bottom:20px}}.card{{background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:16px}}.clabel{{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}}.cval{{font-size:26px;font-weight:700;color:#e6edf3}}.cval.g{{color:#3fb950}}.cval.b{{color:#58a6ff}}.bar{{background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:14px;font-size:12px;margin-bottom:12px;display:flex;gap:20px;flex-wrap:wrap;align-items:center}}</style></head>
    <body><div class="wrap">
    <h1>🚀 EUR/USD Tradovate Live Recorder</h1>
    <p class="sub">Bulletproof Pipeline · Chunked Saves · ZIP Backup · Zero Data Loss on Restart</p>
    <div class="cards">
    <div class="card"><div class="clabel">Live Price</div><div class="cval b">{price_str}</div></div>
    <div class="card"><div class="clabel">Trading Date</div><div class="cval">{date_str}</div></div>
    <div class="card"><div class="clabel">Chunk Files</div><div class="cval g">{num_chunks}</div></div>
    <div class="card"><div class="clabel">Total Rows</div><div class="cval g">{total_rows:,}</div></div>
    </div>
    <div class="bar">🟢 LIVE &nbsp;|&nbsp; HF: {hf_status} &nbsp;|&nbsp; Last Tick: {last_up}
    <a href="https://huggingface.co/datasets/{HF_REPO_ID}" target="_blank" style="color:#58a6ff;text-decoration:none;margin-left:auto;">☁️ HuggingFace Vault ↗</a>
    </div>
    <div class="bar" style="color:#8b949e;">💡 Each 50 ticks → unique chunk file · Every 30 min → ZIP+Upload to HF · On Restart → Auto Resume from ZIP</div>
    </div></body></html>"""
    return HTMLResponse(content=html)

@app.get("/status")
def status():
    date_str, num_chunks, total_rows = get_today_stats()
    return {"status": "ok", "date": date_str, "chunks": num_chunks, "total_rows": total_rows}

# ==============================================================================
# 🔐 TRADOVATE AUTH
# ==============================================================================
REST_URL = "https://live.tradovateapi.com/v1/auth/accesstokenrequest"
WS_URL   = "wss://md.tradovateapi.com/v1/websocket"

LOGIN_PAYLOAD = {
  "appId": "tradovate_trader(web)", "appVersion": "3.260911.0",
  "chl": "208634754931", "cid": "1",
  "deviceId": "6478b472-1342-8826-af96-5b685a8df5e4",
  "enc": True, "environment": "live",
  "name": "gavaliashutosh490@gmail.com",
  "password": "ViEqOXA3cm0mKkVQalNL",
  "sec": "f05206b8ac3ef4e527c9410e39fb2a4da1a801e19f51576d54ae1d398b578f8d"
}
HEADERS = {
    "Accept": "application/json", "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Origin": "https://trader.tradovate.com", "Referer": "https://trader.tradovate.com/"
}

has_subscribed = False
CURRENT_TOKEN  = ""
global_ws      = None

def get_fresh_token():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔐 Requesting fresh token...", flush=True)
    try:
        res = requests.post(REST_URL, headers=HEADERS, json=LOGIN_PAYLOAD)
        if res.status_code == 200:
            data = res.json()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Token acquired! (Expires: {data.get('expirationTime')})", flush=True)
            return data.get("mdAccessToken")
        print(f"❌ Token failed: {res.status_code}", flush=True)
        return None
    except Exception as e:
        print(f"❌ Token error: {e}", flush=True)
        return None

def proactive_token_refresher():
    global CURRENT_TOKEN, global_ws
    while True:
        time.sleep(50 * 60)
        new_token = get_fresh_token()
        if new_token and global_ws:
            CURRENT_TOKEN = new_token
            global_ws.send(f"authorize\n999\n\n{CURRENT_TOKEN}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔥 Zero-gap token refresh done!", flush=True)

# ==============================================================================
# 📡 WEBSOCKET HANDLERS
# ==============================================================================
def on_message(ws, message):
    global has_subscribed
    if message == 'h':
        ws.send('[]'); return
    if message == 'o':
        print("🟢 WS Opened! Authorizing...", flush=True)
        ws.send(f"authorize\n2\n\n{CURRENT_TOKEN}"); return
    if not has_subscribed and message.startswith('a[{"s":200'):
        print("✅ Authorized! Subscribing to EUR/USD DOM...", flush=True)
        ws.send('md/subscribeDom\n3\n\n{"symbol":3267437}')
        ws.send('md/subscribeQuote\n4\n\n{"symbol":3267437}')
        has_subscribed = True; return
    if message.startswith('a['):
        try:
            for item in json.loads(message[1:]):
                if 'e' in item and item['e'] == 'md':
                    process_market_data(item['d'])
        except: pass

def process_market_data(md_data):
    try:
        ts  = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S.%f")
        row = {"timestamp": ts, "raw_payload": json.dumps(md_data)}
        try:
            if "quotes" in md_data:
                for q in md_data["quotes"]:
                    if "entries" in q:
                        e = q["entries"]
                        if "Trade" in e:
                            live_state["current_price"] = e["Trade"].get("price", live_state["current_price"])
                        elif "Bid" in e:
                            live_state["current_price"] = e["Bid"].get("price", live_state["current_price"])
        except: pass
        live_state["total_snapshots"] += 1
        live_state["last_update"] = ts
        with buffer_lock:
            tick_buffer.append(row)
            if len(tick_buffer) >= BUFFER_LIMIT:
                threading.Thread(target=save_parquet_chunk, daemon=True).start()
    except: pass

def on_error(ws, error):
    print(f"❌ WS Error: {error}", flush=True)

def on_close(ws, code, msg):
    print("🔴 WS Closed — flushing buffer...", flush=True)
    save_parquet_chunk()

def on_open(ws):
    print("🚀 Connecting to Tradovate...", flush=True)

# ==============================================================================
# 🎬 MAIN
# ==============================================================================
def start_recorder():
    global has_subscribed, CURRENT_TOKEN, global_ws

    # Step 1: Resume any existing data from HuggingFace ZIP
    resume_from_hf()

    ws_headers = [
        "Origin: https://trader.tradovate.com",
        "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    ]

    # Step 2: Start background threads
    threading.Thread(target=proactive_token_refresher, daemon=True).start()
    threading.Thread(target=run_fastapi, daemon=True).start()
    threading.Thread(target=background_monitor, daemon=True).start()

    # Step 3: Main reconnect loop
    while True:
        CURRENT_TOKEN = get_fresh_token()
        if not CURRENT_TOKEN:
            time.sleep(10)
            continue
        try:
            ws = websocket.WebSocketApp(
                WS_URL, header=ws_headers,
                on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close
            )
            global_ws = ws
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"⚠️ Reconnection Error: {e}", flush=True)
        print("🔄 Connection lost. Reconnecting in 5s...", flush=True)
        has_subscribed = False
        time.sleep(5)

if __name__ == "__main__":
    start_recorder()
