{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'INVENTORY_COVER'
        {% endif %}"
    )
}}

with edw_reg_inventory_health_analysis_propagation as(
    select * from {{ ref('aspedw_integration__edw_reg_inventory_health_analysis_propagation') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
transformed as 
(    
    select 
        'INVENTORY_COVER' as datasource,
        country_name,
        cd."cluster",
        cast (month_year as integer) as fisc_yr_per,
        sum(Healthy_Inventory) as Healthy_Inventory,
        sum(inventory_value) as Total_inventory,
        sum(last_12_months_so_value) as so_value,
        case
            when sum(last_12_months_so_value) / 52 != 0 
            then sum(inventory_value) / (sum(last_12_months_so_value) / 52)
            else 0
        end as weeks_cover
    from 
        (
            select month_year,
                country_name,
                sap_prnt_cust_desc,
                brand,
                segment,
                prod_category,
                variant,
                put_up_description,
                inventory_value,
                last_12_months_so_value,
                weeks_cover_prod,
                case
                    when country_name != 'China FTZ'
                    and weeks_cover_prod <= 13 then inventory_value
                    when country_name = 'China FTZ'
                    and weeks_cover_prod <= 26 then inventory_value
                    else 0
                end as Healthy_Inventory
            from (
                    select month_year,
                        country_name,
                        sap_prnt_cust_desc,
                        brand,
                        segment,
                        prod_category,
                        variant,
                        put_up_description,
                        sum(inventory_val_usd) as inventory_value,
                        sum(last_12months_so_val_usd) as last_12_months_so_value,
                        case 
                            when sum(last_12months_so_val_usd)>0  and (sum(last_12months_so_val_usd)/52) != 0 
                            then sum(inventory_val_usd)/ (sum(last_12months_so_val_usd)/52)
                            else 0 
                        end as weeks_cover_prod -- ,last_12months_so_val_usd
                        -- ,inventory_val_usd
                        -- ,
                    from edw_reg_inventory_health_analysis_propagation -- where country_name = 'Indonesia'
                        --  and  month_year = 202012
                    group by month_year,
                        country_name,
                        sap_prnt_cust_desc,
                        brand,
                        segment,
                        prod_category,
                        variant,
                        put_up_description
                )
        ) inv
        LEFT JOIN (
            select distinct ctry_group,
                "cluster"
            from edw_company_dim
        ) cd ON case
            when (
                inv.country_name = 'China FTZ'
                or inv.country_name = 'China Domestic'
            ) then 'China Selfcare'
            else inv.country_name
        end = cd.ctry_group
    group by month_year,
        country_name,
        "cluster"
),
final as(
    select 
    datasource,
country_name as ctry_nm,
"cluster" as cluster,
 fisc_yr_per,
null:: VARCHAR(50) as PARENT_CUSTOMER,
null:: VARCHAR(100) as MEGA_BRAND,
null:: NUMBER(38,15) as COPA_NTS_USD,
null:: NUMBER(38,15) as COPA_NTS_LCY,
null:: NUMBER(38,15) as COPA_TOP30_NTS_USD,
null:: NUMBER(38,15) as COPA_TOP30_NTS_LCY,
null:: NUMBER(38,15) as ECOMM_NTS_USD,
null:: NUMBER(38,15) as ECOMM_NTS_LCY,
null:: NUMBER(38,15) as CIW_GTS_LCY,
null:: NUMBER(38,15) as CIW_GTS_USD,
null:: NUMBER(38,15) as CIW_LCY,
null:: NUMBER(38,15) as CIW_USD,
null:: NUMBER(18,0) as PREV_YR_MNTH,
null:: NUMBER(38,15) as NTS_PREV_YR_MNTH,
null:: NUMBER(38,15) as NTS_LCY_PREV_YR_MNTH,
null:: NUMBER(15,5) as TOP30_GROWTH,
null:: NUMBER(38,15) as ECOMM_NTS_PREV_YR_MNTH,
null:: NUMBER(38,15) as ECOMM_NTS_LCY_PREV_YR_MNTH,
null:: NUMBER(15,5) as ECOMM_NTS_GROWTH,
null:: NUMBER(38,15) as CIW_GTS_PREV_YR_MNTH,
null:: NUMBER(38,15) as CIW_PREV_YR_MNTH,
null:: NUMBER(38,15) as CIW_GTS_LCY_PREV_YR_MNTH,
null:: NUMBER(38,15) as CIW_LCY_PREV_YR_MNTH,
null:: NUMBER(38,15) as GROWTH_CIW_GTS,
null:: NUMBER(38,15) as GROWTH_CIW,
null::VARCHAR(50) as KPI,
null::NUMBER(38,15) as MSL_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as MSL_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as MSL_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as OSA_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as OSA_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as OSA_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as PROMO_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as PROMO_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as PROMO_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as DISPLAY_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as DISPLAY_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as DISPLAY_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as SOS_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as SOS_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15)  as SOS_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as SOA_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as SOA_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as SOA_COMPLAINCE_DENOMINATOR_WT,
Healthy_Inventory as Healthy_Inventory_usd,
Total_inventory as Total_inventory_usd,
so_value as last_12_months_so_value_usd,
weeks_cover as weeks_cover,
null::DATE as PERFECTSTORE_LATESTDATE,
null::NUMBER(31,2) as DSO_GTS,
null::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE,
null::NUMBER(31,0) as DSO_JNJ_DAYS,
null::VARCHAR(20) as MARKET_SHARE_PERIOD_TYPE,
null::NUMBER(31,2) as MARKET_SHARE_TOTAL,
null::NUMBER(31,2) as MARKET_SHARE_JNJ,
null::NUMBER(31,2) as GROSS_PROFIT,
null::NUMBER(31,2) as FINANCE_NTS,
null::NUMBER(31,2) as MARKET_SHARE_TOTAL_PREV_YR,
null::NUMBER(31,2) as MARKET_SHARE_JNJ_PREV_YR,
null::NUMBER(31,2) as DSO_GTS_PREV_YR,
null::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE_PREV_YR,
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
    from transformed
)
select * from final