import os
import time
import json
import websocket
import threading
import pandas as pd
from datetime import datetime
import requests
import http.server
import socketserver
from huggingface_hub import HfApi, create_repo

# ==============================================================================
# 🚀 TRADOVATE 24/7 AUTO-LOGIN & ZERO-GAP CLOUD RECORDER (Pure Python)
# 🛡️ Includes Uptime Robot Health Check + Hugging Face Auto-Sync
# ==============================================================================

HF_TOKEN = os.getenv("HF_TOKEN", "") # Add this in Render Environment Variables!
HF_REPO_ID = "gavali77/nifty-tradovate-live-data"
UPLOAD_INTERVAL_SECONDS = 300 # Upload to HF every 5 mins
last_hf_upload_time = time.time()

class HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is alive and running!")

def start_health_server():
    PORT = int(os.environ.get("PORT", 10000))
    with socketserver.TCPServer(("", PORT), HealthCheckHandler) as httpd:
        print(f"🟢 [UPTIME ROBOT] Health Server listening on port {PORT}...")
        httpd.serve_forever()

def sync_to_huggingface():
    try:
        api = HfApi(token=HF_TOKEN)
        create_repo(repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN, private=True, exist_ok=True)
        api.upload_file(
            path_or_fileobj=CSV_FILENAME,
            path_in_repo=f"live_data/{CSV_FILENAME}",
            repo_id=HF_REPO_ID,
            repo_type="dataset",
            token=HF_TOKEN
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 [HF SYNC] CSV successfully uploaded to Hugging Face!", flush=True)
    except Exception as e:
        print(f"⚠️ HF Sync Failed: {e}", flush=True)

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
CSV_FILENAME = f"tradovate_{SYMBOL}_live_data.csv"

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
        
        # Save every 50 ticks to reduce file I/O overhead
        if len(collected_data) >= 50:
            df = pd.DataFrame(collected_data)
            hdr = not os.path.exists(CSV_FILENAME)
            df.to_csv(CSV_FILENAME, mode='a', header=hdr, index=False)
            collected_data.clear()
            print(f"[{ts}] 💾 [SAVED] 50 new ticks appended to CSV.", flush=True)
            
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
    threading.Thread(target=start_health_server, daemon=True).start()
    
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
