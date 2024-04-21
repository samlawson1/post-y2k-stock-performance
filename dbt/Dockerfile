FROM python:3.10
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir /stocks_dbt_model
COPY ["commands.sh", "stocks_dbt_model/*", "stocks_dbt_model/"]
WORKDIR /stocks_dbt_model
CMD ["commands.sh"]