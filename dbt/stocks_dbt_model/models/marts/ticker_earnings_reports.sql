{{
    config(
        materialized='incremental',
        unique_key='report_id'
    )
}}

SELECT
-- Create primary key for table
CASE
    WHEN (SELECT MAX(report_id) FROM {{source('nasdaq', 'ticker_earnings_reports')}}) IS NOT NULL
    THEN ROW_NUMBER() OVER(ORDER BY z.ticker, z.per_end_date) + (SELECT MAX(report_id) FROM {{source('nasdaq', 'ticker_earnings_reports')}})
    ELSE ROW_NUMBER() OVER(ORDER BY z.ticker, z.per_end_date) END AS report_id
,
h.ticker_id,
z.per_end_date AS quarter_end_date,
z.per_fisc_year AS fiscal_year,
z.per_cal_year AS calendar_year,
z.tot_revnu AS total_revenue,
z.gross_profit,
CASE WHEN z.gross_profit < 0 THEN FALSE ELSE TRUE END AS is_profitable,
z.tot_oper_exp AS total_operating_expense,
z.basic_net_eps AS earnings_per_share,
z.comm_shares_out AS shares_outstanding

FROM {{source('landing', 'zacks_fc_api')}} AS z 
INNER JOIN {{source('nasdaq', 's_and_p_500_history')}} AS h
ON z.ticker = h.ticker
