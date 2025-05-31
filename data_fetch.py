import requests
from datetime import datetime, timezone

def contract_launch_time(symbol):
    response = requests.get(f"https://api.india.delta.exchange/v2/products/{symbol}")
    if response.status_code != 200:
        return None
    meta = response.json()['result']
    launch_time = datetime.fromisoformat(meta["launch_time"].replace("Z", "+00:00"))
    return launch_time

symbol = "C-BTC-102000-130625"


def fetch_week_data(i):
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
        for candle in candles:
            print(candle)

fetch_week_data(0)