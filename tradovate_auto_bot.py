import os
import time
import json
import websocket
import threading
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
import uvicorn
import zipfile
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, create_repo

# ==============================================================================
# 🚀 TRADOVATE 24/7 AUTO-LOGIN & ZERO-GAP CLOUD RECORDER (Pure Python)
# 🛡️ Includes Uptime Robot Health Check + Hugging Face Auto-Sync
# ==============================================================================

HF_TOKEN = os.getenv("HF_TOKEN", "") # Add this in Render Environment Variables!
HF_REPO_ID = os.getenv("HF_REPO_ID", "ashutickdata/Euro-usd-tick-data")
UPLOAD_INTERVAL_SECONDS = 300 # Upload to HF every 5 mins
last_hf_upload_time = time.time()

live_state = {
    "status": "ACTIVE 🟢",
    "hf_sync_status": "READY ☁️" if HF_TOKEN else "SET HF_TOKEN IN ENV",
    "last_update": "N/A",
    "total_snapshots": 0,
    "current_price": 0.0,
    "current_file": ""
}

app = FastAPI(title="EUR/USD Tradovate Live Collector")

def run_fastapi():
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)

@app.get("/", response_class=HTMLResponse)
def dashboard():
    files = [f for f in os.listdir(".") if f.endswith(".parquet") and "tradovate_" in f]
    files_html = "".join([f'<li><a href="/download/{f}" style="color:#00e676; text-decoration:none;">📥 {f}</a></li>' for f in files]) or "<li>No files yet...</li>"

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>🚀 EUR/USD Quant Engine</title>
        <meta http-equiv="refresh" content="2">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0b0e14; color: #fff; text-align: center; padding: 40px; }}
            .card {{ background: #151922; border: 1px solid #232936; border-radius: 16px; padding: 25px; max-width: 650px; margin: 0 auto; box-shadow: 0 8px 32px rgba(0,0,0,0.5); }}
            h1 {{ color: #00e676; font-size: 24px; margin-bottom: 5px; }}
            .stat {{ font-size: 32px; font-weight: bold; margin: 15px 0; color: #fff; }}
            .hf-badge {{ display: inline-block; padding: 4px 14px; border-radius: 12px; background: #1c2738; font-size: 12px; color: #58a6ff; margin-bottom: 15px; border: 1px solid #58a6ff; }}
            .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; text-align: left; margin: 20px 0; }}
            .box {{ background: #1a202c; padding: 15px; border-radius: 10px; }}
            .label {{ color: #8b949e; font-size: 12px; }}
            .val {{ font-size: 18px; font-weight: bold; margin-top: 5px; color: #00e676; }}
            .files {{ text-align: left; margin-top: 25px; background: #1a202c; padding: 15px; border-radius: 10px; }}
            .btn-zip {{ display: inline-block; background: #ff9800; color: #000; padding: 8px 16px; border-radius: 8px; font-weight: bold; text-decoration: none; margin-top: 10px; }}
            ul {{ list-style-type: none; padding: 0; }}
            li {{ padding: 8px 0; border-bottom: 1px solid #2d3748; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>🇪🇺🇺🇸 EUR/USD (6EZ6) Quant Engine</h1>
            <div class="hf-badge">Hugging Face Vault: {live_state["hf_sync_status"]}</div><br>
            
            <div class="stat">${live_state["current_price"]}</div>
            
            <div class="grid">
                <div class="box">
                    <div class="label">Total Raw Payloads Recorded</div>
                    <div class="val">{live_state["total_snapshots"]:,} Active Ticks</div>
                </div>
                <div class="box">
                    <div class="label">Target Data Type</div>
                    <div class="val">Parquet Vault</div>
                </div>
            </div>

            <div class="files">
                <div class="label" style="font-weight:bold; margin-bottom: 8px; color: #fff;">💾 Download Daily Parquet Files (1 File / Day):</div>
                <ul>{files_html}</ul>
                <a href="/download-zip" class="btn-zip">📦 Download ALL Days (ZIP)</a>
                <a href="https://huggingface.co/datasets/{HF_REPO_ID}" target="_blank" style="display:inline-block; margin-left: 10px; color:#58a6ff; text-decoration:none; font-size:12px;">☁️ Open HuggingFace Vault ↗</a>
            </div>
            <p style="color: #6e7681; font-size: 11px; margin-top: 15px;">Last Update: {live_state["last_update"]}</p>
        </div>
    </body>
    </html>
    """
    return html

@app.get("/download-zip")
def download_zip():
    zip_path = "tradovate_all_days.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for f in os.listdir("."):
            if f.endswith(".parquet") and "tradovate_" in f:
                zipf.write(f, arcname=f)
    return FileResponse(zip_path, media_type="application/zip", filename=zip_path)

@app.get("/download/{filename}")
def download_file(filename: str):
    if os.path.exists(filename) and filename.endswith(".parquet"):
        return FileResponse(filename, media_type="application/octet-stream", filename=filename)
    return {"error": "File not found"}

def sync_to_huggingface():
    try:
        api = HfApi(token=HF_TOKEN)
        create_repo(repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN, private=True, exist_ok=True)
        current_file = get_daily_parquet_filename()
        api.upload_file(
            path_or_fileobj=current_file,
            path_in_repo=f"daily_vault/{current_file}",
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            token=HF_TOKEN
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 [HF SYNC] File successfully uploaded to Hugging Face!", flush=True)
        live_state["hf_sync_status"] = f"SYNCED ({datetime.now().strftime('%H:%M:%S')}) 🟢"
    except Exception as e:
        print(f"⚠️ HF Sync Failed: {e}", flush=True)
        live_state["hf_sync_status"] = f"ERROR ❌"

REST_URL = "https://live.tradovateapi.com/v1/auth/accesstokenrequest"
WS_URL = "wss://md.tradovateapi.com/v1/websocket" # LIVE MARKET DATA!

LOGIN_PAYLOAD = {
  "appId": "tradovate_trader(web)",
  "appVersion": "3.260911.0",
  "chl": "208634754931",
  "cid": "1",
  "deviceId": "6478b472-1342-8826-af96-5b685a8df5e4",
  "enc": True,
  "environment": "live", 
  "name": "gavaliashutosh490@gmail.com",
  "password": "ViEqOXA3cm0mKkVQalNL",
  "sec": "f05206b8ac3ef4e527c9410e39fb2a4da1a801e19f51576d54ae1d398b578f8d"
}

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
    "Origin": "https://trader.tradovate.com",
    "Referer": "https://trader.tradovate.com/"
}

SYMBOL = "6EZ6"

def get_daily_parquet_filename():
    # Convert UTC to IST (+5:30)
    ist_time = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    
    # Trading day rolls over at 3:30 AM IST. 
    # If before 03:30 AM, it belongs to yesterday's trading session.
    if ist_time.hour < 3 or (ist_time.hour == 3 and ist_time.minute < 30):
        trade_date = ist_time - timedelta(days=1)
    else:
        trade_date = ist_time
        
    today_str = trade_date.strftime("%Y-%m-%d")
    return f"tradovate_{SYMBOL}_live_data_{today_str}.parquet"

collected_data = []
has_subscribed = False
CURRENT_TOKEN = ""
global_ws = None  # Reference to the active websocket

def get_fresh_token():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔐 Requesting fresh token from REST API...")
    try:
        res = requests.post(REST_URL, headers=HEADERS, json=LOGIN_PAYLOAD)
        if res.status_code == 200:
            data = res.json()
            exp = data.get('expirationTime')
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Token Acquired Successfully! (Expires at: {exp})")
            return data.get("mdAccessToken") 
        else:
            print(f"❌ Failed to get token. Status: {res.status_code}, Response: {res.text}")
            return None
    except Exception as e:
        print(f"❌ Error during login: {e}")
        return None

def proactive_token_refresher():
    """Runs in background: Refreshes token every 50 minutes and injects it into live WS (ZERO GAP)"""
    global CURRENT_TOKEN, global_ws
    while True:
        # Sleep for 50 minutes (Tokens usually last 1h 20m)
        time.sleep(50 * 60)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 PROACTIVE REFRESH: Getting new token before old one expires...")
        new_token = get_fresh_token()
        if new_token and global_ws:
            CURRENT_TOKEN = new_token
            # Send the new token over the EXISTING websocket to keep it alive!
            auth_frame = f"authorize\n999\n\n{CURRENT_TOKEN}"
            global_ws.send(auth_frame)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔥 ZERO-GAP REFRESH SUCCESSFUL! Injected new token into live stream.")

def on_message(ws, message):
    global has_subscribed
    if message == 'h':
        ws.send('[]')
        return
        
    if message == 'o':
        print("🟢 Connection Opened! Sending Authorization...")
        auth_frame = f"authorize\n2\n\n{CURRENT_TOKEN}"
        ws.send(auth_frame)
        return

    if not has_subscribed and message.startswith('a[{"s":200'):
        print("✅ Authorization Successful!")
        sub_frame_dom = f'md/subscribeDom\n3\n\n{{"symbol":3267437}}'
        sub_frame_quote = f'md/subscribeQuote\n4\n\n{{"symbol":3267437}}'
        ws.send(sub_frame_dom)
        ws.send(sub_frame_quote)
        has_subscribed = True
        print(f"📡 Subscribed to Live Orderbook (DOM) for Symbol ID: 3267437...")
        return

    if message.startswith('a['):
        try:
            json_str = message[1:]
            data_list = json.loads(json_str)
            for item in data_list:
                if 'e' in item and item['e'] == 'md':
                    process_market_data(item['d'])
        except Exception as e:
            pass

def process_market_data(md_data):
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        row = {"timestamp": ts, "raw_payload": json.dumps(md_data)}
        collected_data.append(row)
        
        # Parse price for UI
        try:
            if "quotes" in md_data:
                for q in md_data["quotes"]:
                    if "entries" in q and "Trade" in q["entries"]:
                        live_state["current_price"] = q["entries"]["Trade"].get("price", live_state["current_price"])
                    elif "entries" in q and "Bid" in q["entries"]:
                        live_state["current_price"] = q["entries"]["Bid"].get("price", live_state["current_price"])
        except:
            pass

        live_state["total_snapshots"] += 1
        live_state["last_update"] = ts
        
        # Save every 50 ticks to reduce file I/O overhead
        if len(collected_data) >= 50:
            df = pd.DataFrame(collected_data)
            current_file = get_daily_parquet_filename()
            table = pa.Table.from_pandas(df)
            if not os.path.exists(current_file):
                pq.write_table(table, current_file)
            else:
                existing = pq.read_table(current_file)
                pq.write_table(pa.concat_tables([existing, table]), current_file)
            collected_data.clear()
            print(f"[{ts}] 💾 [SAVED] 50 new ticks appended to {current_file}.", flush=True)
            
            # Auto-Sync to Hugging Face
            global last_hf_upload_time
            if time.time() - last_hf_upload_time > UPLOAD_INTERVAL_SECONDS:
                threading.Thread(target=sync_to_huggingface, daemon=True).start()
                last_hf_upload_time = time.time()
    except Exception as e:
        pass

def on_error(ws, error):
    print("❌ WebSocket Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("🔴 Connection Closed by Server")

def on_open(ws):
    print("🚀 Connecting to Tradovate Server...")

def start_recorder():
    global has_subscribed, CURRENT_TOKEN, global_ws
    ws_headers = [
        "Origin: https://trader.tradovate.com",
        "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ]
    
    # Start the proactive refresher thread
    threading.Thread(target=proactive_token_refresher, daemon=True).start()
    
    # Start the Uptime Robot Health Server thread
    threading.Thread(target=run_fastapi, daemon=True).start()
    
    while True:
        CURRENT_TOKEN = get_fresh_token()
        if not CURRENT_TOKEN:
            time.sleep(10)
            continue
            
        try:
            ws = websocket.WebSocketApp(WS_URL,
                                      header=ws_headers,
                                      on_open=on_open,
                                      on_message=on_message,
                                      on_error=on_error,
                                      on_close=on_close)
            global_ws = ws # Save reference for background refresher
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"⚠️ Reconnection Error: {e}")
            
        print("🔄 Connection lost. Re-authenticating and auto-reconnecting in 5 seconds...")
        has_subscribed = False
        time.sleep(5)

if __name__ == "__main__":
    start_recorder()
