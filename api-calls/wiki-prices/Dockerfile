FROM python:3.10
COPY ["requirements.txt", "daily_prices_call.py", "nasdaq_api_call.py", "./"]
RUN pip install -r requirements.txt
#Argument for user to input API key needed to use the nasdaq-data-link api
ARG API_KEY=USER_API_KEY
ENV NASDAQ_DATA_LINK_API_KEY=${API_KEY}
#Input database username
ARG USER=DB_USER
#Input user's password
ARG PASSWORD=DB_PASS
#Create environment variable for postgres db connection string on localhost
ENV STOCK_DB_CONN="postgresql+psycopg2://${USER}:${PASSWORD}@localhost/StockDB"
#Run the daily_prices_call.py file to get updated wiki prices
CMD ["python", "daily_prices_call.py"]