import pandas as pd
import os
from sqlalchemy import create_engine
from wiki_scrape_functions import *

#Get list of tickers being shown as currently part of the S&P 500 in the database
conn = os.getenv('STOCK_DB_CONN')
db_tickers = pull_current_list(conn)
wiki_content = get_content()
wiki_tickers = get_constituents(wiki_content)
new_tickers = [t for t in wiki_tickers['ticker'] if t not in db_tickers]

#If there are new additions
if len(new_tickers)  > 0:
    today = pd.Timestamp('today')
    last_week_date = str(today - pd.Timedelta(weeks = 1))
    #Get the removed tickers from the last week
    removed_tickers = get_removed_tickers(wiki_content)
    #removed tickers within the past week / run
    removed_tickers = removed_tickers.loc[(removed_tickers['ticker'].isin(db_tickers)) & (removed_tickers['date_removed'] > last_week_date)]
    #update internal data to reflect that it was removed and on what date it was removed
    for t, d in list(zip(removed_tickers['ticker'], removed_tickers['date_removed'])):
        update_query = f'UPDATE "NASDAQ".s_and_p_500_history \
                        SET currently_listed = false and date_removed = {d} \
                        WHERE ticker = {t} ;'
        with engine.connect() as db_conn:
            db_conn.execute(update_query)
            db_conn.commit()
            db_conn.dispose()

    i = get_max_db_id(conn) + 1
    ticker_updates = wiki_tickers.loc[wiki_tickers['ticker'].isin(new_tickers)].sort_values(by = ['date_added']).reset_index(drop = True)
    ticker_updates['date_removed'] = pd.NaT
    ticker_updates['ticker_id'] = ticker_updates.index + i
    ticker_updates['currently_listed'] = True
    col_order = ['ticker_id', 'ticker', 'date_added', 'date_removed', 'currently_listed']
    ticker_updates = ticker_updates[col_order]
    update_table(ticker_updates, conn)
