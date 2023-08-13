#!/usr/bin/python
# -*- coding: utf-8 -*-
# insert_symbols.py
from __future__ import print_function
import datetime
from math import ceil
import bs4
import MySQLdb as mdb
import requests
import time
import os

def format_dates(date_string):
    mapping = {'Jan': 'January', 'Feb': 'February', 'Mar': 'March', 'Apr': 'April',
                'May': 'May', 'Jun': 'June', 'Jul': 'July', 'Aug': 'August',
                'Sep': 'September', 'Oct': 'October', 'Nov': 'November', 'Dec': 'December'}
    date_split = date_string.split(' ')
    date_split[0] = mapping[date_split[0]]
    return ' '.join(date_split)

def get_daily_historic_data_yahoo(
    ticker, start_date=(2000,1,1),
    end_date=datetime.date.today().timetuple()[0:3]
    ):
    """
    Obtains data from Yahoo Finance returns and a list of tuples.
    ticker: Yahoo Finance ticker symbol, e.g. "GOOG" for Google, Inc.
    start_date: Start date in (YYYY, M, D) format
    end_date: End date in (YYYY, M, D) format
    """
    print('ticker', ticker)
    # Construct the Yahoo URL with the correct integer query parameters
    # for start and end dates. Note that some parameters are zero-based!
    start_datetime = datetime.datetime(start_date[0], start_date[1], start_date[2])
    end_datetime = datetime.datetime(end_date[0], end_date[1], end_date[2])
    start_unix = str(int((time.mktime(start_datetime.timetuple()))))
    end_unix = str(int((time.mktime(end_datetime.timetuple()))))
    ticker_tup = (
    ticker, start_unix, end_unix
    )
    yahoo_url = "https://finance.yahoo.com/quote/"
    yahoo_url += "%s/history?period1=%s&period2=%s&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"
    yahoo_url = yahoo_url % ticker_tup
    print('url', yahoo_url)
    # Try connecting to Yahoo Finance and obtaining the data
    # On failure, print an error message.
    try:
        header = {'Connection': 'keep-alive',
                'Expires': '-1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15'
                }
        yf_data = requests.get(yahoo_url, headers=header)
        prices = []
        soup = bs4.BeautifulSoup(yf_data.text, features="lxml")
        # This selects the first table, using CSS Selector syntax
        # and then ignores the header row ([1:])
        historical_list = soup.select('table')[0].select('tr')[1:]
        for history in historical_list[:2]:
            date = datetime.datetime.strptime(format_dates(
                    history.select('td')[0].text), "%B %d, %Y")
            open_price = history.select('td')[1].text
            high_price = history.select('td')[2].text
            low_price = history.select('td')[3].text
            close_price = history.select('td')[4].text
            adj_close_price = history.select('td')[5].text
            volume = history.select('td')[6].text
            prices.append(
            (date,
            open_price, high_price, low_price, close_price, volume, adj_close_price, ticker)
            )
    except Exception as e:
        print("Could not download Yahoo data: %s" % e)
    return prices

def obtain_parse_wiki_snp500():
    """
    Download and parse the Wikipedia list of S&P500
    constituents using requests and BeautifulSoup.
    Returns a list of tuples for to add to MySQL.
    """
    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()
    # Use requests and BeautifulSoup to download the
    # list of S&P500 companies and obtain the symbol table
    header = {'Connection': 'keep-alive',
                'Expires': '-1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15'
                }
    response = requests.get("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=header)
    soup = bs4.BeautifulSoup(response.text, features="lxml")
    # This selects the first table, using CSS Selector syntax
    # and then ignores the header row ([1:])
    symbolslist = soup.select('table')[0].select('tr')[1:]
    # Obtain the symbol information for each
    # row in the S&P500 constituent table
    symbols = []
    for i, symbol in enumerate(symbolslist[:2]):
        tds = symbol.select('td')
        symbols.append((
        '1', '1', now, # Ticker
        now,
        now, # Name
        420.9999, # Sector
        420.9999, 420.9999, 420.9999, 420.9999, 500
        ))
    return symbols

def insert_snp500_symbols(symbols):
    """
    Insert the S&P500 symbols into the MySQL database.
    """
    print('symns', symbols)
    # Connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'sec_user'
    db_pass = os.environ["sql_pwd"]
    db_name = 'securities_master'
    con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
    # Create the insert strings
    column_str = """data_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price,
    high_price, low_price, close_price, adj_close_price, volume
    """
    insert_str = ("%s, " * 11)[:-2]
    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % \
    (column_str, insert_str)
    # Using the MySQL connection, carry out
    # an INSERT INTO for every symbol
    with con:
        cur = con.cursor()
        cur.executemany(final_str, symbols)
        con.commit()

if __name__ == "__main__":
    symbols = obtain_parse_wiki_snp500()
    insert_snp500_symbols(symbols)
    print("%s symbols were successfully added." % len(symbols))
