import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "LINKUSDT",
    "AVAXUSDT", "ONDOUSDT", "DOGEUSDT", "SHIBUSDT", "PEPEUSDT",
]

START_DATE = "2022-01-01"
INTERVAL = "1m"
LIMIT = 1000
SAVE_DIR = Path(__file__).resolve().parent / "data_raw" / "binance"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://api.binance.com/api/v3/klines"


def fetch_klines(symbol: str, start_ms: int, end_ms: int) -> list:
    rows = []
    cur = start_ms

    while cur < end_ms:
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": cur,
            "endTime": end_ms,
            "limit": LIMIT,
        }
        res = requests.get(BASE_URL, params=params, timeout=20)
        res.raise_for_status()
        data = res.json()

        if not data:
            break

        rows.extend(data)
        cur = data[-1][6] + 1
        time.sleep(0.1)

    return rows


def to_dataframe(raw: list) -> pd.DataFrame:
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "n_trades",
        "taker_buy_base", "taker_buy_quote", "ignore",
    ]
    df = pd.DataFrame(raw, columns=cols)
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("timestamp")[["open", "high", "low", "close", "volume"]]
    df = df.astype(float)
    return df[~df.index.duplicated(keep="first")]


def download_symbol(symbol: str) -> None:
    save_path = SAVE_DIR / f"{symbol}_1m.parquet"

    if save_path.exists():
        old = pd.read_parquet(save_path)
        start_ms = int(old.index[-1].timestamp() * 1000) + 60_000
        print(f"[{symbol}] resume from {old.index[-1]}")
    else:
        old = None
        start_ms = int(pd.Timestamp(START_DATE, tz="UTC").timestamp() * 1000)
        print(f"[{symbol}] start from {START_DATE}")

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    raw = fetch_klines(symbol, start_ms, end_ms)

    if not raw:
        print(f"[{symbol}] no new data")
        return

    new = to_dataframe(raw)

    if old is not None:
        df = pd.concat([old, new]).sort_index()
        df = df[~df.index.duplicated(keep="first")]
    else:
        df = new

    df.to_parquet(save_path)
    print(f"[{symbol}] saved: {len(df):,} rows")


if __name__ == "__main__":
    for symbol in SYMBOLS:
        try:
            download_symbol(symbol)
        except Exception as e:
            print(f"[{symbol}] error: {e}")

    print("Done")
