--TEST to make sure there aren't any tickers that aren't historically a part
--of the S&P 500

SELECT ticker
FROM {{ref('ticker_profiles')}}
WHERE ticker NOT IN (
    SELECT DISTINCT ticker
    FROM {{source('nasdaq', 's_and_p_500_history')}}
)




