FROM python:3.10 AS pybuild
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir -p /{python-files, source_data}
COPY python-files\* python-files\*
COPY source-data\* source-data\*
COPY format-source-data.ipynb historical-nasdaq-api-call.ipynb ./
ARG API_KEY=USER_API_KEY
ENV NASDAQ_DATA_LINK_API_KEY=${API_KEY}
RUN echo "THE NASDAQ_DATA_LINK_API_KEY is ${API_KEY}"