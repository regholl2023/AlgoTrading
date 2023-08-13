# Create a table with all historical data from Yahoo Finance for each ticker

#!/usr/bin/python
# -*- coding: utf-8 -*-
# price_retrieval.py
from __future__ import print_function
import datetime
import warnings
import MySQLdb as mdb
import requests
import bs4
import time
import random
import os
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
# Obtain a database connection to the MySQL instance
db_host = 'localhost'
db_user = 'sec_user'
db_pass = os.environ["sql_pwd"]
db_name = 'securities_master'
con = mdb.connect(db_host, db_user, db_pass, db_name)

def random_agent():
    user_agent_list = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        ]
    return random.choice(user_agent_list)

def obtain_list_of_db_tickers():
    """
    Obtains a list of the ticker symbols in the database.
    """
    with con:
        cur = con.cursor()
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1]) for d in data]

def format_dates(date_string):
    mapping = {'Jan': 'January', 'Feb': 'February', 'Mar': 'March', 'Apr': 'April',
                'May': 'May', 'Jun': 'June', 'Jul': 'July', 'Aug': 'August',
                'Sep': 'September', 'Oct': 'October', 'Nov': 'November', 'Dec': 'December'}
    date_split = date_string.split(' ')
    date_split[0] = mapping[date_split[0]]
    return ' '.join(date_split)

# Get historical data for given ticker. Default start_date if Jan 1, 2000
# and default end_date is today
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
    # Construct the Yahoo URL with the correct integer query parameters
    # for start and end dates. Note that some parameters are zero-based!
    start_datetime = datetime.datetime(start_date[0], start_date[1], start_date[2])
    end_datetime = datetime.datetime(end_date[0], end_date[1], end_date[2])
    start_unix = str(int((time.mktime(start_datetime.timetuple()))))
    end_unix = str(int((time.mktime(end_datetime.timetuple()))))
    ticker_tup = (
    ticker, start_unix, end_unix
    )
    # Example URL we want to build (this one is for MMM ticker)
    # https://finance.yahoo.com/quote/MMM/history?period1=946684800&period2=1691884800&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true
    yahoo_url = "https://finance.yahoo.com/quote/"
    yahoo_url += "%s/history?period1=%s&period2=%s&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"
    yahoo_url = yahoo_url % ticker_tup
    # Try connecting to Yahoo Finance and obtaining the data
    # On failure, print an error message.
    # Using selenium to open webpage and automate scrolling to bottom of
    # page which loads all HTML/data
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(yahoo_url)
    time.sleep(1.5)
    button = driver.find_element(By.NAME, 'agree')
    button.click()
    time.sleep(1) # You can set your own pause time. My laptop is a bit slow so I use 1 sec
    screen_height = driver.execute_script("return window.screen.height;")   # get the screen height of the web
    i = 0
    # Should be enough scrolls to reach bottom of screen
    num_scrolls = 300
    while True:
        # scroll one screen height each time
        driver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=screen_height, i=i))  
        i += 1
        # Break the loop when scrolled num_scroll times
        if i > num_scrolls:
            break 
    time.sleep(0.4)
    try:
        prices = []
        events = []
        soup = bs4.BeautifulSoup(driver.page_source, features='lxml')
        # This selects the first table, using CSS Selector syntax
        # and then ignores the header row ([1:])
        historical_list = soup.select('table')[0].select('tr')[1:]
        for history in historical_list:
            # Select the td HTML element (table cell) from the table
            selected = history.select('td')
            # Accounting for text at very bottom of table, which breaks the code
            if ('Close price adjusted for splits' in history.text):
                continue
            date = datetime.datetime.strptime(format_dates(
                    selected[0].text), "%B %d, %Y")
            open_price = selected[1].text
            # For all numerical data, want to remove commas for parsing (same as below)
            open_price = open_price.replace(',','')
            # Events like stock split and dividends will show up in the 2nd column
            # Checking for these events and adding them to separate `events` MySQL table
            if ('Dividend' in open_price or 'Stock Split' in open_price):
                # open_price will contain an event
                events.append((date, open_price, ticker))
                continue
            high_price = selected[2].text
            high_price = high_price.replace(',','')
            # Some data might not be available (represented by something like a '-')
            # For each of the numerical values, try convering to a float to see if casting
            # throws and error. If it throws and error, the data probably isn't numerical
            # so we just replace that value in the table with None (NULL in SQL). 
            # Not clean but IDGAF
            try:
                test = float(high_price)
            except:
                high_price = None
            low_price = selected[3].text
            low_price = low_price.replace(',','')
            try:
                test = float(low_price)
            except:
                low_price = None
            close_price = selected[4].text
            close_price = close_price.replace(',','')
            try:
                test = float(close_price)
            except:
                close_price = None
            adj_close_price = selected[5].text
            adj_close_price = adj_close_price.replace(',','')
            try:
                test = float(adj_close_price)
            except:
                adj_close_price = None
            volume = selected[6].text
            volume = volume.replace(',','')
            try:
                test = float(volume)
            except:
                volume = None
            # Add to list of all tuple information, which gets returned and used in insert_daily_data_into_db()
            prices.append(
            (date,
            open_price, high_price, low_price, close_price, volume, adj_close_price, ticker)
            )
    except Exception as e:
        print("Could not download Yahoo data: %s" % e)
    return prices, events

def insert_daily_data_into_db(
data_vendor_id, symbol_id, daily_data, event_data
):
    """
    Takes a list of tuples of daily data and adds it to the
    MySQL database. Appends the vendor ID and symbol ID to the data.
    daily_data: List of tuples of the OHLC data (with
    adj_close and volume)
    """
    # Connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'sec_user'
    db_pass = os.environ["sql_pwd"]
    db_name = 'securities_master'
    con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
    # Create the time now
    now = datetime.datetime.utcnow()
    # Amend the data which was returned from get_daily_historic_data_yahoo() 
    # to include the vendor ID and symbol ID
    daily_data = [
    (data_vendor_id, symbol_id, d[0], now, now,
    d[1], d[2], d[3], d[4], d[5], d[6], d[7])
    for d in daily_data
    ]
    # Amend the other data (events data) which was returned from get_daily_historic_data_yahoo() 
    # to include the vendor ID and symbol ID
    event_data = [
        (data_vendor_id, symbol_id, now, e[0],
        e[1], e[2])
        for e in event_data
    ]
    # Create the insert strings
    column_str = """data_vendor_id, symbol_id, price_date, created_date,
    last_updated_date, open_price, high_price, low_price,
    close_price, volume, adj_close_price, ticker"""
    insert_str = ("%s, " * 12)[:-2]
    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % \
    (column_str, insert_str)

    event_column_str = """data_vendor_id, symbol_id, update_date, 
                        event_date, event, ticker"""
    event_insert_str = ("%s, " * 6)[:-2]
    event_final_str = "INSERT INTO events (%s) VALUES (%s)" % \
    (event_column_str, event_insert_str)
    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    with con:
        cur = con.cursor()
        # Execute the insert statements which actually insert into the MySQL table
        cur.executemany(final_str, daily_data)
        cur.executemany(event_final_str, event_data)
        # Need commit to save data to table
        con.commit()
        
if __name__ == "__main__":
    # This ignores the warnings regarding Data Truncation
    # from the Yahoo precision to Decimal(19,4) datatypes
    warnings.filterwarnings('ignore')
    # Loop over the tickers and insert the daily historical
    # data into the database
    tickers = obtain_list_of_db_tickers()[148:]
    lentickers = len(tickers)
    for i, t in enumerate(tickers):
        print("Adding data for %s: %s out of %s" %
        (t[1], i+1, lentickers)
        )
        # Get all ticker data
        yf_data, event_data = get_daily_historic_data_yahoo(t[1])
        # Insert all ticker data
        insert_daily_data_into_db('1', t[0], yf_data, event_data)
        time.sleep(0.25)
    print("Successfully added Yahoo Finance pricing data to DB.")
