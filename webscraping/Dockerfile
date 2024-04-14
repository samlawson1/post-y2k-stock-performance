FROM python:3.10
COPY ["requirements.txt", "wiki_scrape_functions.py", "verify_current_s_and_p_500.py", "./"]
RUN pip install -r requirements.txt
#Input database username
ARG USER=DB_USER
#Input user's password
ARG PASSWORD=DB_PASS
#Create environment variable for postgres db connection string on localhost
ENV STOCK_DB_CONN="postgresql+psycopg2://${USER}:${PASSWORD}@localhost/StockDB"
#Run the verify_current_s_and_p_500.py file to make sure internal data is up to date
CMD ["python", "verify_current_s_and_p_500.py"]