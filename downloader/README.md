Downloader

- Downloads daily time-series data for all tickers listed in Nasdaq. 
- Stores these in the ES container.

How to run?
- Run the `main.py` locally after building the parent poetry environment, or run it as a container after building 
  the downloader service. The former is easier to debug.
  
Issues:

- This service is supposed to ran once a day. The first day it saves all the available data for a ticker to the 
  database. However the second time it is ran, it should only download and store the missing data points. Currently 
  it doesn't consider what has already been stored in the database.