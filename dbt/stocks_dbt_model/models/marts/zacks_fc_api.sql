{{
    config(
        materialized='incremental',
        unique_key = dbt_utils.generate_surrogate_key(['m_ticker', 'per_end_date']) 
    )
}}

SELECT 
-- Create Primary Key for report_id
{{ dbt_utils.generate_surrogate_key(['m_ticker', 'per_end_date'])
  }} AS report_id
,
*
,
CURRENT_DATE AS insert_date
FROM
{{source('landing', 'zacks_fc_api')}}


LIMIT 100