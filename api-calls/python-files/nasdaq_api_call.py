import os
import nasdaqdatalink
import pandas as pd

#Function to get ticker prices from the Nasdaq WIKI/PRICES table
def wiki_api_call(ticker_symbol, start_date, end_date):
    #API call will return a pandas dataframe
    df = nasdaqdatalink.get_table('WIKI/PRICES', 
                                ticker = [ticker_symbol], 
                                #gte = greater than or equal to (>=) date in YYYY-MM-DD format
                                #lte = less than or equal to (<=) date in YYYY-MM-DD format
                                date = { 'gte': start_date, 'lte': end_date},
                                #Nasdaq documentation recommends paginate always be set to True
                                paginate = True)
    return(df)

#Function to get Zack's Fundamental's Condensed data
#Description: https://data.nasdaq.com/databases/ZFA#anchor-fundamentals-condensed-zacks-fc-
def zacks_fc_api_call(ticker_symbol, start_date, end_date, **kwargs):
    # kwargs = columns to filter on as a list
    # EX columns = ['ticker', 'per_end_date]
    #If there are no columns
    if len(kwargs) == 0:
            df = nasdaqdatalink.get_table('ZACKS/FC', ticker=[ticker_symbol],
                                  #gte = greater than or equal to (>=) date in YYYY-MM-DD format\
                                  #lte = less than or equal to (<=) date in YYYY-MM-DD format
                                  per_end_date={'gte': start_date, 'lte':end_date}, paginate = True
                                  )
    else:
         cols = kwargs['columns']
         df = nasdaqdatalink.get_table('ZACKS/FC',
                                       ticker=[ticker_symbol],
                        #gte = greater than or equal to (>=) date in YYYY-MM-DD format\
                        #lte = less than or equal to (<=) date in YYYY-MM-DD format
                        per_end_date={'gte': start_date, 'lte':end_date},
                        qopts = cols,
                        paginate = True
                        )
    return(df)

