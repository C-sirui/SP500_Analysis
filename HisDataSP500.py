import lxml
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import mysql.connector
firmCode = "AAPL"
from yahoofinance import HistoricalPrices
import pandas as 



req = HistoricalPrices('AAPL',"2020-01-01", "2022-11-11") //daily
print(req)
