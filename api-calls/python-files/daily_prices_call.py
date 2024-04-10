import os
from nasdaq_api_call import daily_wiki_api_call, data_to_landing
import pandas as pd
from sqlalchemy import create_engine

#Create DB Connection & Get current S&P 500 Tickers
conn = os.getenv('STOCK_DB_CONN')
engine = create_engine(conn)
query = 'SELECT ticker from "NASDAQ".s_and_p_500_history WHERE currently_listed = true ;'
df = pd.read_sql_query(query, engine)
tickers = list(df['ticker'])

today = pd.Timestamp('today')
yesterday = today - pd.Timedelta(days = 1)
yesterday = str(yesterday)[0:10]
for t in tickers:
    t_df = daily_wiki_api_call(t, yesterday)
    if len(t_df) > 0:
        data_to_landing(t_df, 'wiki_prices', conn)
        print(f'{t} Succes {yesterday}!')
    else:
        print(f'No {t} data for {yesterday}')



