Downloader

Downloads daily time-series data for all tickers listed in Nasdaq. Stores these in the ES container.

How to run?
- `python downloader/downloader.py` to start a cycle of updates.
  
- `python downloader/downloader.py FB` to update a given ticker.

Requirements:

- DB Container must be running and can be spun off using `docker-compose`.
