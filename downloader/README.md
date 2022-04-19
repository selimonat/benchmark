Downloader

Downloads daily time-series data for all tickers listed in Nasdaq. Stores these in the ES container.

How to run?
Either from withing `benchmark` top-level directory:
- `python downloader` to start a cycle of updates.
- `python downloader FB` to update a given ticker.

Or by calling the `benchmark` package as:
- `python benchmark db-update --ticker GOOG`
- `python benchmark db-update`

Requirements:

- DB Container must be running and can be spun off using `docker-compose`.
