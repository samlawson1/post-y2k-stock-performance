{{
    config(
        materialized='incremental',
        unique_key = 'report_id'
    )
}}

SELECT 

report_id,
h.ticker_id,
per_end_date AS period_end_date,
per_fisc_year AS fiscal_year,
per_cal_year AS calendar_year,
tot_revnu AS total_revenue,
tot_oper_exp AS operating_expense,
gross_profit,
CASE WHEN gross_profit > 0 THEN TRUE ELSE FALSE END AS is_profitable,
basic_net_eps AS earnings_per_share,
comm_shares_out AS shares_outstanding,
comm_stock_div_paid AS dividends_paid,
incr_decr_cash,
CASE WHEN incr_decr_cash > 1 THEN TRUE ELSE FALSE END AS cash_increase,
CASE WHEN incr_decr_cash < 0 THEN TRUE ELSE FALSE END AS cash_decrease
FROM {{ref('zacks_fc_api')}} AS z
INNER JOIN
{{source('nasdaq', 's_and_p_500_history')}} AS h 
ON z.ticker = h.ticker


