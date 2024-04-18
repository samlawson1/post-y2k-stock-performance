{{
    config(
        materialized='incremental',
        unique_key='ticker_id'
    )
}}

WITH zacks_profile_info AS(
	SELECT DISTINCT
    z1.ticker,
    comp_name,
    zacks_sector_code AS sector,
    zacks_x_ind_code AS expanded_industry,
    bus_city AS city,
    bus_state_name AS state,
    bus_post_code AS zip_code,
    country_name AS country
    FROM {{ref('zacks_fc_api')}} AS z1
    INNER JOIN 
	(
		SELECT ticker, MAX(per_end_date) AS per_end_date
		FROM {{ref('zacks_fc_api')}}
		GROUP BY ticker
	) AS z2

ON z1.ticker = z2.ticker AND z1.per_end_date = z2.per_end_date
WHERE z1.ticker NOT IN(
    SELECT ticker FROM "NASDAQ".ticker_profiles
    )
)

SELECT 
h.ticker_id,
z.ticker,
z.comp_name,
z.sector,
z.expanded_industry,
z.city,
z.state,
z.zip_code,
z.country
FROM 
{{source('nasdaq', 's_and_p_500_history')}} AS h
INNER JOIN zacks_profile_info z
ON h.ticker = z.ticker
ORDER BY ticker_id


