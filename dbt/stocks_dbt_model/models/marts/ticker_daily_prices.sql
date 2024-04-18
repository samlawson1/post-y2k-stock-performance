{{
    config(
        materialized='incremental',
        unique_key='historical_id'
    )
}}

SELECT
-- Create primary key for table
CASE
    WHEN (SELECT MAX(historical_id) FROM {{source('nasdaq', 'ticker_daily_prices')}}) IS NOT NULL
    THEN ROW_NUMBER() OVER(ORDER BY p.ticker, p.date) + (SELECT MAX(historical_id) FROM {{source('nasdaq', 'ticker_daily_prices')}})
    ELSE ROW_NUMBER() OVER(ORDER BY p.ticker, p.date) END AS historical_id
,
h.ticker_id,
p.date,
p.open,
p.high,
p.low,
p.close,
p.volume,
p."ex-dividend" AS ex_dividend,
p.split_ratio,
p.adj_open,
p.adj_low,
p.adj_close,
p.adj_volume
FROM {{source('nasdaq', 's_and_p_500_history')}} AS h
INNER JOIN 
{{source('landing', 'wiki_prices_api')}} AS p
ON h.ticker = p.ticker