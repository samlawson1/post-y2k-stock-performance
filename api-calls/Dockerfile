FROM python:3.10
RUN python -m env .
RUN pip install -r requirements.txt
RUN MKDIR -p {python-files, source_data}
COPY python-files\* python-files\*
COPY source-data\* source-data\*
COPY format-source-data.ipynb historical-nasdaq-api-call.ipynb ./
ENV NASDAQ_DATA_LINK_API_KEY=${API_KEY}