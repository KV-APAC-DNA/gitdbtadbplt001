{{
    config(
        sql_header = "alter session set week_start= 7;"
    )
}}
with pacific_inventory_health_analysis_propagation as (
    select * from {{ ref('pcfwks_integration__pacific_inventory_health_analysis_propagation') }}
),
wks_pacific_sellout_for_inv_analysis as (
    select * from {{ ref('pcfwks_integration__wks_pacific_sellout_for_inv_analysis') }}
),
wks_pacific_inventory_healthy_unhealthy_analysis as (
    select * from {{ ref('pcfwks_integration__wks_pacific_inventory_healthy_unhealthy_analysis') }}
),
final as (
    SELECT 
        inv.*,
        healthy_inv.healthy_inventory,
        wkly_avg.min_date,
        datediff (week, wkly_avg.min_date, last_day(to_date(left(mnth_id, 4) || right(mnth_id, 2), 'yyyymm'))) diff_weeks,
        case
            when least(diff_weeks, 52) <= 0 then 1
            else least(diff_weeks, 52)
        end as l12m_weeks,
        case
            when least(diff_weeks, 26) <= 0 then 1
            else least(diff_weeks, 26)
        end as l6m_weeks,
        case
            when least(diff_weeks, 13) <= 0 then 1
            else least(diff_weeks, 13)
        end as l3m_weeks,
        inv.last_12months_so_val / l12m_weeks as l12m_weeks_avg_sales,
        inv.last_6months_so_val / l6m_weeks as l6m_weeks_avg_sales,
        inv.last_3months_so_val / l3m_weeks as l3m_weeks_avg_sales,
        last_12months_so_val_usd / l12m_weeks as l12m_weeks_avg_sales_usd,
        last_6months_so_val_usd / l6m_weeks as l6m_weeks_avg_sales_usd,
        last_3months_so_val_usd / l3m_weeks as l3m_weeks_avg_sales_usd,
        last_12months_so_qty / l12m_weeks as l12m_weeks_avg_sales_qty,
        last_6months_so_qty / l6m_weeks as l6m_weeks_avg_sales_qty,
        last_3months_so_qty / l3m_weeks as l3m_weeks_avg_sales_qty
    FROM pacific_inventory_health_analysis_propagation inv
    LEFT JOIN wks_pacific_sellout_for_inv_analysis wkly_avg 
        ON inv.sap_prnt_cust_key = wkly_avg.sap_prnt_cust_key
        and inv.global_put_up_desc = wkly_avg.pka_size_desc
        and inv.global_prod_brand = wkly_avg.global_prod_brand
        and inv.global_prod_variant = wkly_avg.global_prod_variant
        and inv.global_prod_segment = wkly_avg.global_prod_segment
        and inv.global_prod_category = wkly_avg.global_prod_category
        and inv.pka_product_key = wkly_avg.pka_product_key
    LEFT JOIN wks_pacific_inventory_healthy_unhealthy_analysis healthy_inv 
        ON mnth_id = healthy_inv.month
        and inv.sap_prnt_cust_key = healthy_inv.sap_prnt_cust_key
        and inv.global_put_up_desc = healthy_inv.pka_size_desc
        and inv.global_prod_brand = healthy_inv.global_prod_brand
        and inv.global_prod_variant = healthy_inv.global_prod_variant
        and inv.global_prod_segment = healthy_inv.global_prod_segment
        and inv.global_prod_category = healthy_inv.global_prod_category
        and inv.pka_product_key = healthy_inv.pka_product_key
)
select * from final