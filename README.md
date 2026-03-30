# MRTS: Mean Reversion Prediction with Shortcut Decomposition

This repository provides the data download pipeline used in the paper:

**"Mean Reversion Prediction with Shortcut Decomposition"**

---

## Overview

This project studies whether mean reversion is truly predictable, or whether model performance is driven by deviation-based shortcut features.

---

## Data Sources

### Crypto (Binance)

- Source: Binance public REST API
- Assets:
  - BTC, ETH, SOL, XRP, LINK, AVAX, ONDO, DOGE, SHIB, PEPE
- Frequency:
  - 1-minute OHLCV (raw data)
- Sample period:
  - From **2022-01-01** to the download time

---

### Equity, ETF, Commodity (Yahoo Finance)

- Source: Yahoo Finance via `yfinance`
- Assets:
  - Equity ETFs (10)
  - Individual stocks (10)
  - Commodities (10)

- Frequencies:
  - 5m (last 60 days)
  - 1h (last 730 days)
  - 1d (max available history, ~20 years)
  - 1wk (max available history, ~20 years)

---

## Important Note

Due to data availability constraints:

- Crypto data span a longer time period
- Yahoo Finance data have **frequency-dependent sample windows**

---

## Data Download

Run the following scripts:

```bash
python download_binance.py
python download_yahoo.py
