{{
    config(
        materialized='incremental',
        unique_key = dbt_utils.generate_surrogate_key(['ticker_id', 'date']) 
    )
}}

SELECT
-- Create surrogate key for table
{{ dbt_utils.generate_surrogate_key(['ticker_id', 'date'])
  }} AS historical_id
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
p.adj_volume,
CURRENT_DATE AS insert_date
FROM {{source('nasdaq', 's_and_p_500_history')}} AS h
INNER JOIN 
{{source('landing', 'wiki_prices_api')}} AS p
ON h.ticker = p.ticker

LIMIT 100