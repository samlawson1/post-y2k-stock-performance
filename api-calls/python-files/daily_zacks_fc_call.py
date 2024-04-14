import os
from nasdaq_api_call import zacks_fc_api_call, data_to_landing
import pandas as pd
from sqlalchemy import create_engine

#Create DB Connection & Get current S&P 500 Tickers
conn = os.getenv('STOCK_DB_CONN')
engine = create_engine(conn)
query = 'SELECT ticker from "NASDAQ".s_and_p_500_history WHERE currently_listed = true ;'
df = pd.read_sql_query(query, engine)
#list of tickers to check for updated ZACKS/FC data
tickers = list(df['ticker'])

today = pd.Timestamp('today')
today = str(today)[0:10]

#columns to include in response from ZACKS/FC api call
zacks_columns = [
    'm_ticker', #primary_key
    'ticker', 'comp_name', 'currency_code', 'per_end_date', 'per_fisc_year', 'per_cal_year', 'filing_date',
    'zacks_sector_code', 'zacks_x_ind_code', 
    'bus_city', 'bus_state_name', 'bus_post_code', 'country_name',
    'tot_revnu', #millions
    'gross_profit', #millions
    'tot_oper_exp', #millions
    'basic_net_eps', 
    'comm_shares_out', 'comm_stock_div_paid',
    'incr_decr_cash', 
]
#Get ZACKS/FC data for each ticker
for ticker in tickers[0:10]:
    t_df = zacks_fc_api_call(ticker, today, today, columns = zacks_columns)
    #If there's data that comes back send it to the database
    if len(t_df) > 0:
        data_to_landing(t_df, 'zacks_fc', conn)
    else:
        pass
#close the sql engine
conn.dispose()
