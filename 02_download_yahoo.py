import time
from pathlib import Path

import pandas as pd
import yfinance as yf

EQUITY_ETF = ["SPY", "QQQ", "IWM", "DIA", "VUG", "VTV", "XLK", "XLF", "XLE", "XLV"]
EQUITY_STK = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "TSLA", "JPM", "XOM", "JNJ", "PG"]
COMMODITY = ["GLD", "SLV", "USO", "UNG", "DBC", "DBA", "CORN", "WEAT", "SOYB", "PPLT"]
ALL_TICKERS = EQUITY_ETF + EQUITY_STK + COMMODITY

BASE_DIR = Path(__file__).resolve().parent / "data_raw" / "yahoo"

INTERVALS = {
    "5m": {"period": "60d", "subdir": "5m"},
    "15m": {"period": "60d", "subdir": "15m"},
    "30m": {"period": "60d", "subdir": "30m"},
    "1h": {"period": "730d", "subdir": "1h"},
    "1d": {"period": "20y", "subdir": "1d"},
    "1wk": {"period": "20y", "subdir": "1wk"},
    "1mo": {"period": "20y", "subdir": "1mo"},
}

MAX_RETRY = 3
SLEEP_SEC = 1.2


def normalize_df(df) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower() for col in df.columns]
    else:
        df.columns = [col.lower() for col in df.columns]

    keep = [col for col in ["open", "high", "low", "close", "volume"] if col in df.columns]
    if not keep:
        return None

    df = df[keep].copy()

    if df.index.tz is None:
        df.index = df.index.tz_localize("America/New_York").tz_convert("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    df.index.name = "timestamp"
    df = df[~df.index.duplicated(keep="first")].sort_index()

    return df if not df.empty else None


def download_ticker(ticker: str, interval: str, period: str, save_dir: Path) -> None:
    save_path = save_dir / f"{ticker}_{interval}.parquet"

    for attempt in range(1, MAX_RETRY + 1):
        try:
            df = normalize_df(
                yf.Ticker(ticker).history(
                    period=period,
                    interval=interval,
                    auto_adjust=True,
                    raise_errors=False,
                )
            )

            if df is None:
                df = normalize_df(
                    yf.download(
                        ticker,
                        period=period,
                        interval=interval,
                        auto_adjust=True,
                        progress=False,
                        multi_level_index=False,
                    )
                )

            if df is None:
                print(f"[{ticker}] no data for {interval}")
                return

            df.to_parquet(save_path)
            print(f"[{ticker}] saved {interval}: {len(df):,} rows")
            return

        except KeyboardInterrupt:
            raise
        except Exception as e:
            wait = 3 * attempt
            print(f"[{ticker}] try {attempt}/{MAX_RETRY} failed: {e}")
            time.sleep(wait)

    print(f"[{ticker}] failed for {interval}")


if __name__ == "__main__":
    print(f"yfinance version: {yf.__version__}")

    for cfg in INTERVALS.values():
        (BASE_DIR / cfg["subdir"]).mkdir(parents=True, exist_ok=True)

    for interval, cfg in INTERVALS.items():
        save_dir = BASE_DIR / cfg["subdir"]
        print(f"\n=== {interval} ({cfg['period']}) ===")

        for ticker in ALL_TICKERS:
            download_ticker(ticker, interval, cfg["period"], save_dir)
            time.sleep(SLEEP_SEC)

    print("Done")
