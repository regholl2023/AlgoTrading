# Create a table with all S&P 500 tickers and id/information

#!/usr/bin/python
# -*- coding: utf-8 -*-
# insert_symbols.py
from __future__ import print_function
import datetime
from math import ceil
import bs4
import MySQLdb as mdb
import requests
import random
import os

def random_agent():
    user_agent_list = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        ]
    return random.choice(user_agent_list)

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
                'User-Agent': random_agent(),
                }
    response = requests.get("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=header)
    soup = bs4.BeautifulSoup(response.text, features="lxml")
    # This selects the first table, using CSS Selector syntax
    # and then ignores the header row ([1:])
    symbolslist = soup.select('table')[0].select('tr')[1:]
    # Obtain the symbol information for each
    # row in the S&P500 constituent table
    symbols = []
    for i, symbol in enumerate(symbolslist):
        tds = symbol.select('td')
        symbols.append((
        tds[0].select('a')[0].text, # Ticker
        'stock',
        tds[1].select('a')[0].text, # Name
        tds[3].text, # Sector
        'USD', now, now
        ))
    return symbols

def insert_snp500_symbols(symbols):
    """
    Insert the S&P500 symbols into the MySQL database.
    """
    sql_pwd = os.environ["sql_pwd"]
    # Connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'sec_user'
    db_pass = sql_pwd
    db_name = 'securities_master'
    con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
    # Create the insert strings
    column_str = """ticker, instrument, name, sector,
    currency, created_date, last_updated_date
    """
    insert_str = ("%s, " * 7)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % \
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
