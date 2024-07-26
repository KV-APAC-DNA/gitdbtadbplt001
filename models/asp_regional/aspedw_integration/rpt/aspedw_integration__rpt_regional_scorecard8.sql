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
    select * from snapaspedw_integration.edw_reg_inventory_health_analysis_propagation
),
edw_company_dim as(
    select * from snapaspedw_integration.edw_company_dim
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
    select datasource,
        country_name as ctry_nm,
        "cluster" as cluster,
        fisc_yr_per,
        Healthy_Inventory as Healthy_Inventory_usd,
        Total_inventory as Total_inventory_usd,
        so_value as last_12_months_so_value_usd,
        weeks_cover as weeks_cover
    from transformed
)
select * from final