import os
from nasdaq_api_call import daily_wiki_api_call, data_to_landing
import pandas as pd
from sqlalchemy import create_engine

#Create DB Connection & Get current S&P 500 Tickers
conn = os.getenv('STOCK_DB_CONN')
engine = create_engine(conn)
query = 'SELECT ticker from "NASDAQ".s_and_p_500_history WHERE currently_listed = true ;'
df = pd.read_sql_query(query, engine)
#list of tickers to get daily price updates
tickers = list(df['ticker'])

today = pd.Timestamp('today')
today = str(today)[0:10]
#Check get wiki price data for each ticker
for ticker in tickers:
    t_df = daily_wiki_api_call(ticker, today)
    #if data returns send it to the LANDING schema in the database
    if len(t_df) > 0:
        data_to_landing(t_df, 'wiki_prices', conn)
    else:
        pass

#Close the SQL engine connection
engine.dispose()



