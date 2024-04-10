import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import os

def pull_current_list(conn):
    #Get list of tickers being shown as currently part of the S&P 500 in the database
    engine = create_engine(conn)
    query = r'SELECT ticker FROM "NASDAQ".s_and_p_500_history WHERE currently_listed = true;'
    current_tickers = pd.read_sql_query(query, conn)
    current_tickers = list(current_tickers['ticker'])
    return(current_tickers)

def get_content():
    #Check source to verify current list
    wiki_url = r'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    payload = requests.get(wiki_url)
    soup = BeautifulSoup(payload.content, 'html.parser')
    return(soup)

def get_constituents(soup):
    #format html into pandas df of current S&P 500 constituents
    table = soup.find('table', id='constituents')
    header = []
    rows = []
    for i, row in enumerate(table.find_all('tr')):
        if i == 0:
            header = [el.text.strip() for el in row.find_all('th')]
        else:
            rows.append([el.text.strip() for el in row.find_all('td')])
    
    df = pd.DataFrame(rows, columns = header)
    df = df[['Symbol', 'Date added']].rename(columns ={ 'Symbol':'ticker', 'Date added':'date_added'}).sort_values(by = 'ticker').reset_index(drop = True)
    df['date_added'] = pd.to_datetime(df['date_added'])
    df['currently_listed'] = True
    return(df)

def get_removed_tickers(soup):
    rows = []
    change_html = soup.find('table', id='changes')
    for i, row in enumerate(change_html.find_all('tr')):
        if i > 1:
            rows.append([el.text.strip() for el in row.find_all('td')])
            
    df = pd.DataFrame(rows, columns = ['Date', 'ADD_Symbol', 'ADD_Security', 'RMV_Symbol', 'RMV_Security', 'Reason'])       
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.replace('', None)

    #Get Most recent remove dates per ticker
    rmv_dates = df.loc[df['RMV_Symbol'].isnull() == False][['Date', 'RMV_Symbol']].\
        groupby('RMV_Symbol').agg({'Date':'max'}).reset_index().\
        rename(columns = {'RMV_Symbol':'ticker', 'Date':'date_removed'})
    
    rmv_dates['date_removed'] = pd.to_datetime(rmv_dates['date_removed'])

    return(rmv_dates)




