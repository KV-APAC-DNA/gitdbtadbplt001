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
    select *
    from aspedw_integration.edw_market_share_qsd
),
itg_mds_ap_sales_ops_map as(
    select *
    from snapaspitg_integration.itg_mds_ap_sales_ops_map
),
final as(
    SELECT cy."datasource" as datasource,
        cy."market" as ctry_nm,
        cy."cluster" as cluster,
        cy."fisc_yr_per" as fisc_yr_per,
        null::VARCHAR(50) as PARENT_CUSTOMER,
        null::VARCHAR(100) as MEGA_BRAND,
        null::NUMBER(38, 15) as COPA_NTS_USD,
        null::NUMBER(38, 15) as COPA_NTS_LCY,
        null::NUMBER(38, 15) as COPA_TOP30_NTS_USD,
        null::NUMBER(38, 15) as COPA_TOP30_NTS_LCY,
        null::NUMBER(38, 15) as ECOMM_NTS_USD,
        null::NUMBER(38, 15) as ECOMM_NTS_LCY,
        null::NUMBER(38, 15) as CIW_GTS_LCY,
        null::NUMBER(38, 15) as CIW_GTS_USD,
        null::NUMBER(38, 15) as CIW_LCY,
        null::NUMBER(38, 15) as CIW_USD,
        null::NUMBER(18, 0) as PREV_YR_MNTH,
        null::NUMBER(38, 15) as NTS_PREV_YR_MNTH,
        null::NUMBER(38, 15) as NTS_LCY_PREV_YR_MNTH,
        null::NUMBER(15, 5) as TOP30_GROWTH,
        null::NUMBER(38, 15) as ECOMM_NTS_PREV_YR_MNTH,
        null::NUMBER(38, 15) as ECOMM_NTS_LCY_PREV_YR_MNTH,
        null::NUMBER(15, 5) as ECOMM_NTS_GROWTH,
        null::NUMBER(38, 15) as CIW_GTS_PREV_YR_MNTH,
        null::NUMBER(38, 15) as CIW_PREV_YR_MNTH,
        null::NUMBER(38, 15) as CIW_GTS_LCY_PREV_YR_MNTH,
        null::NUMBER(38, 15) as CIW_LCY_PREV_YR_MNTH,
        null::NUMBER(38, 15) as GROWTH_CIW_GTS,
        null::NUMBER(38, 15) as GROWTH_CIW,
        null::VARCHAR(50) as KPI,
        null::NUMBER(38, 15) as MSL_COMPLAINCE_NUMERATOR,
        null::NUMBER(38, 15) as MSL_COMPLAINCE_DENOMINATOR,
        null::NUMBER(38, 15) as MSL_COMPLAINCE_DENOMINATOR_WT,
        null::NUMBER(38, 15) as OSA_COMPLAINCE_NUMERATOR,
        null::NUMBER(38, 15) as OSA_COMPLAINCE_DENOMINATOR,
        null::NUMBER(38, 15) as OSA_COMPLAINCE_DENOMINATOR_WT,
        null::NUMBER(38, 15) as PROMO_COMPLAINCE_NUMERATOR,
        null::NUMBER(38, 15) as PROMO_COMPLAINCE_DENOMINATOR,
        null::NUMBER(38, 15) as PROMO_COMPLAINCE_DENOMINATOR_WT,
        null::NUMBER(38, 15) as DISPLAY_COMPLAINCE_NUMERATOR,
        null::NUMBER(38, 15) as DISPLAY_COMPLAINCE_DENOMINATOR,
        null::NUMBER(38, 15) as DISPLAY_COMPLAINCE_DENOMINATOR_WT,
        null::NUMBER(38, 15) as PLANOGRAM_COMPLAINCE_NUMERATOR,
        null::NUMBER(38, 15) as PLANOGRAM_COMPLAINCE_DENOMINATOR,
        null::NUMBER(38, 15) as PLANOGRAM_COMPLAINCE_DENOMINATOR_WT,
        null::NUMBER(38, 15) as SOS_COMPLAINCE_NUMERATOR,
        null::NUMBER(38, 15) as SOS_COMPLAINCE_DENOMINATOR,
        null::NUMBER(38, 15) as SOS_COMPLAINCE_DENOMINATOR_WT,
        null::NUMBER(38, 15) as SOA_COMPLAINCE_NUMERATOR,
        null::NUMBER(38, 15) as SOA_COMPLAINCE_DENOMINATOR,
        null::NUMBER(38, 15) as SOA_COMPLAINCE_DENOMINATOR_WT,
        null as Healthy_Inventory_usd,
        null as Total_inventory_usd,
        null as last_12_months_so_value_usd,
        null as weeks_cover,
        null::DATE as PERFECTSTORE_LATESTDATE,
        null::NUMBER(31, 2) as DSO_GTS,
        null::NUMBER(31, 2) as DSO_GROSS_ACCOUNT_RECEIVABLE,
        null::NUMBER(31, 0) as DSO_JNJ_DAYS,
        cy.period_type::VARCHAR(20) as MARKET_SHARE_PERIOD_TYPE,
        SUM(cy."market_share_total") AS market_share_total,
        SUM(cy."market_share_jnj") AS market_share_jnj,
        null::NUMBER(31, 2) as GROSS_PROFIT,
        null::NUMBER(31, 2) as FINANCE_NTS,
        SUM(py."market_share_total") AS market_share_total_prev_yr,
        SUM(py."market_share_jnj") AS market_share_jnj_prev_yr,
        null::NUMBER(31, 2) as DSO_GTS_PREV_YR,
        null::NUMBER(31, 2) as DSO_GROSS_ACCOUNT_RECEIVABLE_PREV_YR,
        null:: NUMBER(31,0) as DSO_JNJ_DAYS_PREV_YR,
null:: NUMBER(31,2) as GROSS_PROFIT_PREV_YR,
null:: NUMBER(31,2) as FINANCE_NTS_PREV_YR,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_USD,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_LCY,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_USD_PREV_YR_MNTH,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_LCY_PREV_YR_MNTH,
null:: NUMBER(31,2) as NUMERATOR,
null:: NUMBER(31,2) as DENOMINATOR,
null:: NUMBER(31,2) as NUMERATOR_PREV_YR,
null:: NUMBER(31,2) as DENOMINATOR_PREV_YR,
null:: VARCHAR(20) as CUSTOMER_SEGMENT,
null:: NUMBER(38,15) as CY_SEGMENT_NTS_USD,
null:: NUMBER(38,15) as PY_SEGMENT_NTS_USD,
null:: NUMBER(38,15) as CY_SEGMENT_NTS_LCY,
null:: NUMBER(38,15) as PY_SEGMENT_NTS_LCY
    FROM(
            (
                SELECT 'MARKET SHARE' AS "datasource",
                    period_type,
                    (
                        LEFT(mkt_share.period_date, 4) + '0' + LEFT(RIGHT(mkt_share.period_date, 5), 2)
                    )::INT AS "fisc_yr_per",
                    destination_cluster AS "cluster",
                    destination_market AS "market",
                    SUM(
                        CASE
                            WHEN UPPER(brand_manufacturer_flg) = 'J&J' THEN value
                            ELSE 0
                        END
                    ) AS "market_share_jnj",
                    SUM(value) AS "market_share_total"
                FROM edw_market_share_qsd mkt_share
                    JOIN itg_mds_ap_sales_ops_map ctry_map ON ctry_map.dataset = 'Market Share QSD'
                    AND ctry_map.source_cluster = mkt_share.cluster
                    AND ctry_map.source_market = mkt_share.country_geo
                    AND mkt_share.cluster != 'Total APac'
                WHERE region = 'APAC'
                    AND period_type = 'Continuous'
                    AND type = 'Value'
                    AND LEFT(mkt_share.period_date, 4)::INT > EXTRACT(
                        YEAR
                        FROM convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)
                    ) - 2
                GROUP BY 1,
                    2,
                    3,
                    4,
                    5
            ) cy
            LEFT JOIN (
                SELECT 'MARKET SHARE' AS "datasource",
                    period_type,
                    (
                        (LEFT(mkt_share.period_date, 4)::INT + 1)::character varying + '0' + LEFT(RIGHT(mkt_share.period_date, 5), 2)
                    )::INT AS "fisc_yr_per",
                    destination_cluster AS "cluster",
                    destination_market AS "market",
                    SUM(
                        CASE
                            WHEN UPPER(brand_manufacturer_flg) = 'J&J' THEN value
                            ELSE 0
                        END
                    ) AS "market_share_jnj",
                    SUM(value) AS "market_share_total"
                FROM edw_market_share_qsd mkt_share
                    JOIN itg_mds_ap_sales_ops_map ctry_map ON ctry_map.dataset = 'Market Share QSD'
                    AND ctry_map.source_cluster = mkt_share.cluster
                    AND ctry_map.source_market = mkt_share.country_geo
                    AND mkt_share.cluster != 'Total APac'
                WHERE region = 'APAC'
                    AND period_type = 'Continuous'
                    AND type = 'Value'
                    AND LEFT(mkt_share.period_date, 4)::INT > EXTRACT(
                        YEAR
                        FROM convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)
                    ) - 3
                GROUP BY 1,
                    2,
                    3,
                    4,
                    5
            ) py ON cy."fisc_yr_per" = py."fisc_yr_per"
            AND cy."cluster" = py."cluster"
            AND cy."market" = py."market"
        )
    GROUP BY 1,
        2,
        3,
        4,
        cy.period_type
        
)
select * from final