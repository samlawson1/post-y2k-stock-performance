SELECT ticker_id
FROM {{ref('ticker_daily_prices')}}
WHERE ticker_id NOT IN (
    SELECT ticker_id
    FROM {{source('nasdaq', 's_and_p_500_history')}}
)




