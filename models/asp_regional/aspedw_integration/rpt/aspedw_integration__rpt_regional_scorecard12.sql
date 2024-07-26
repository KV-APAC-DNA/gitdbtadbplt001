{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'MARKET SHARE'
        {% endif %}"
    )
}}
with edw_market_share_qsd as(
    select * from aspedw_integration.edw_market_share_qsd
),
itg_mds_ap_sales_ops_map as(
    select * from snapaspitg_integration.itg_mds_ap_sales_ops_map
),
final as(
SELECT cy.datasource,
cy.fisc_yr_per,
cy.period_type,
cy.cluster,
cy.market,
SUM(cy.market_share_jnj) AS "market_share_jnj",
SUM(cy.market_share_total) AS "market_share_total",
SUM(py.market_share_jnj) AS "market_share_jnj_prev_yr",
SUM(py.market_share_total) AS "market_share_total_prev_yr"
FROM(
(SELECT 'MARKET SHARE' AS "datasource",
period_type,
(LEFT(mkt_share.period_date,4) + '0' + LEFT(RIGHT(mkt_share.period_date,5),2))::INT AS "fisc_yr_per",
destination_cluster AS "cluster",
destination_market AS "market",
SUM(CASE WHEN UPPER(brand_manufacturer_flg) = 'J&J' THEN value ELSE 0 END) AS "market_share_jnj",
SUM(value) AS "market_share_total"
FROM edw_market_share_qsd mkt_share
JOIN itg_mds_ap_sales_ops_map ctry_map
ON ctry_map.dataset = 'Market Share QSD' AND ctry_map.source_cluster = mkt_share.cluster AND ctry_map.source_market = mkt_share.country_geo AND mkt_share.cluster != 'Total APac'
WHERE region = 'APAC' AND period_type = 'Continuous' AND type = 'Value'
AND LEFT(mkt_share.period_date,4)::INT > EXTRACT(YEAR FROM convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - 2
GROUP BY 1,2,3,4,5) cy
LEFT JOIN
(SELECT 'MARKET SHARE' AS "datasource",
period_type,
((LEFT(mkt_share.period_date,4)::INT + 1)::character varying + '0' + LEFT(RIGHT(mkt_share.period_date,5),2))::INT AS "fisc_yr_per",
destination_cluster AS "cluster",
destination_market AS "market",
SUM(CASE WHEN UPPER(brand_manufacturer_flg) = 'J&J' THEN value ELSE 0 END) AS "market_share_jnj",
SUM(value) AS "market_share_total"
FROM edw_market_share_qsd mkt_share
JOIN itg_mds_ap_sales_ops_map ctry_map
ON ctry_map.dataset = 'Market Share QSD' AND ctry_map.source_cluster = mkt_share.cluster AND ctry_map.source_market = mkt_share.country_geo AND mkt_share.cluster != 'Total APac'
WHERE region = 'APAC' AND period_type = 'Continuous' AND type = 'Value'
AND LEFT(mkt_share.period_date,4)::INT > EXTRACT(YEAR FROM convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - 3
GROUP BY 1,2,3,4,5) py
ON cy.fisc_yr_per = py.fisc_yr_per AND cy.cluster = py.cluster AND cy.market = py.market
) GROUP BY 1,2,3,4,5
)
select * from final