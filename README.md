# MRTS Data

This repository provides the data download scripts used in the MRTS project.

## Sources

- **Binance**: Crypto (BTC, ETH, SOL, XRP, LINK, AVAX, ONDO, DOGE, SHIB, PEPE)  
  - 1-minute OHLCV  
  - From 2022-01-01 to present  

- **Yahoo Finance**: ETF, stocks, commodities  
  - 5m (60 days), 1h (730 days), 1d / 1wk (max history)  

## Usage

```bash
python download_binance.py
python download_yahoo.py
