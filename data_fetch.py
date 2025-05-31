import requests
from datetime import datetime, timezone
import pandas as pd


def contract_launch_time(symbol):
    response = requests.get(f"https://api.india.delta.exchange/v2/products/{symbol}")
    if response.status_code != 200:
        return None
    meta = response.json()['result']
    launch_time = datetime.fromisoformat(meta["launch_time"].replace("Z", "+00:00"))
    return launch_time



all_data = {}
def fetch_week_data(i,symbol):
    launch_time = contract_launch_time(symbol)

    start_unix = int(launch_time.timestamp())+i*604800
    end_unix = start_unix+(1+i)*604800

    url = "https://api.india.delta.exchange/v2/history/candles"
    params = {
        "resolution": "5m",
        "symbol": symbol,
        "start": start_unix,
        "end": end_unix,
    }
    headers = {"Accept": "application/json"}
    response = requests.get(url, params=params, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch candle data for {symbol}")
    else:
        candles = response.json().get("result", [])
        print(f"Retrieved {len(candles)} candles for {symbol}")
        all_data[symbol] = candles



url = "https://api.india.delta.exchange/v2/products"
headers = {"Accept": "application/json"}

resp = requests.get(url, headers=headers)
resp.raise_for_status()
products = resp.json()["result"]


for p in products:
    if (
        (p.get("contract_type") == "call_options" or p.get("contract_type") == "put_options") and
        p.get("settlement_time", "").startswith("2025-06")
    ):
        symbol = p.get("symbol")
        strike = p.get("strike_price")
        expiry = p.get("settlement_time")
        # print(f"sybmol: {symbol} etrike: {strike} expiry: {expiry}")
        fetch_week_data(0,symbol)


import os

os.makedirs("symbol_data", exist_ok=True)

for symbol, candles in all_data.items():
    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df["symbol"] = symbol

    file_name = f"symbol_data/{symbol}.csv"
    df.to_csv(file_name, index=False)
    print(f"Saved {symbol} to {file_name}")
