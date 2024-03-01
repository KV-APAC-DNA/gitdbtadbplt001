{{
    config(
        sql_header = "alter session set week_start= 7;"
    )
}}

with wks_my_inventory_health_analysis_propagation as
(
    select * from {{ ref('aspwks_integration__wks_my_inventory_health_analysis_propagation') }}
),
wkly_avg as
(
    select * from {{ ref('myswks_integration__wks_my_sellout_for_inv_analysis') }}
    where to_ccy = 'MYR'
),
healthy_inv as
(
  select * from {{ ref('myswks_integration__wks_my_inventory_healthy_unhealthy_analysis') }}
),

transformed as
(
    select
    inv.*,
    healthy_inv.healthy_inventory,
    wkly_avg.min_date,
    datediff ( week,min_date, last_day(to_date(left(month_year,4)||right(month_year,2),'yyyymm')) )as diff_weeks,
    case when least(diff_weeks, 52) <= 0 then 1 
         when diff_weeks is NULL then 52
         else least(diff_weeks, 52) 
    end as l12m_weeks,
    case when least(diff_weeks, 26) <= 0 then 1 
         when diff_weeks is NULL then 26
         else least(diff_weeks, 26) 
    end as l6m_weeks,
    case when least(diff_weeks, 13) <= 0 then 1 
         when diff_weeks is NULL then 13
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
from wks_my_inventory_health_analysis_propagation as inv
left join wkly_avg
  on inv.dstrbtr_grp_cd = wkly_avg.dstrbtr_grp_cd
  and inv.distributor_id_name = wkly_avg.dstrbtr_lvl3
  and inv.sap_prnt_cust_key = wkly_avg.sap_prnt_cust_key
  and inv.put_up_description = wkly_avg.pka_size_desc
  and inv.brand = wkly_avg.global_prod_brand
  and inv.variant = wkly_avg.global_prod_variant
  and inv.segment = wkly_avg.global_prod_segment
  and inv.prod_category = wkly_avg.global_prod_category
  and inv.pka_product_key = wkly_avg.pka_product_key
left join  healthy_inv
  on month_year = healthy_inv.month
  and inv.dstrbtr_grp_cd = healthy_inv.dstrbtr_grp_cd
  and inv.distributor_id_name = healthy_inv.distributor_id_name
  and inv.sap_prnt_cust_key = healthy_inv.sap_parent_customer_key
  and inv.put_up_description = healthy_inv.pka_size_desc
  and inv.brand = healthy_inv.global_prod_brand
  and inv.variant = healthy_inv.global_prod_variant
  and inv.segment = healthy_inv.global_prod_segment
  and inv.prod_category = healthy_inv.global_prod_category
  and inv.pka_product_key = healthy_inv.pka_product_key
),
final as
(
    select 
        year::number(18,0) as year,
        year_quarter::varchar(14) as year_quarter,
        month_year::varchar(21) as month_year,
        month_number::number(18,0) as month_number,
        country_name::varchar(8) as country_name,
        dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
        distributor_id_name::varchar(40) as distributor_id_name,
        franchise::varchar(30) as franchise,
        brand::varchar(30) as brand,
        prod_sub_brand::varchar(100) as prod_sub_brand,
        variant::varchar(100) as variant,
        segment::varchar(50) as segment,
        prod_subsegment::varchar(100) as prod_subsegment,
        prod_category::varchar(50) as prod_category,
        prod_subcategory::varchar(50) as prod_subcategory,
        put_up_description::varchar(30) as put_up_description,
        sku_cd::varchar(100) as sku_cd,
        sku_description::varchar(100) as sku_description,
        pka_product_key::varchar(68) as pka_product_key,
        pka_product_key_description::varchar(255) as pka_product_key_description,
        product_key::varchar(68) as product_key,
        product_key_description::varchar(255) as product_key_description,
        from_ccy::varchar(3) as from_ccy,
        to_ccy::varchar(3) as to_ccy,
        exch_rate::number(15,5) as exch_rate,
        sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
        sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
        sap_cust_chnl_key::varchar(12) as sap_cust_chnl_key,
        sap_cust_chnl_desc::varchar(50) as sap_cust_chnl_desc,
        sap_cust_sub_chnl_key::varchar(12) as sap_cust_sub_chnl_key,
        sap_sub_chnl_desc::varchar(50) as sap_sub_chnl_desc,
        sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
        sap_go_to_mdl_desc::varchar(50) as sap_go_to_mdl_desc,
        sap_bnr_key::varchar(12) as sap_bnr_key,
        sap_bnr_desc::varchar(50) as sap_bnr_desc,
        sap_bnr_frmt_key::varchar(12) as sap_bnr_frmt_key,
        sap_bnr_frmt_desc::varchar(50) as sap_bnr_frmt_desc,
        retail_env::varchar(50) as retail_env,
        region::varchar(61) as region,
        zone_or_area::varchar(80) as zone_or_area,
        si_sls_qty::number(38,5) as si_sls_qty,
        si_gts_val::number(38,5) as si_gts_val,
        si_gts_val_usd::number(38,5) as si_gts_val_usd,
        inventory_quantity::number(38,5) as inventory_quantity,
        inventory_val::number(38,5) as inventory_val,
        inventory_val_usd::number(38,5) as inventory_val_usd,
        so_sls_qty::number(38,5) as so_sls_qty,
        so_grs_trd_sls::number(38,5) as so_grs_trd_sls,
        so_grs_trd_sls_usd::number(17,0) as so_grs_trd_sls_usd,
        si_all_db_val::number(38,5) as si_all_db_val,
        si_all_db_val_usd::number(38,5) as si_all_db_val_usd,
        si_inv_db_val::number(38,5) as si_inv_db_val,
        si_inv_db_val_usd::number(38,5) as si_inv_db_val_usd,
        last_3months_so_qty::number(38,6) as last_3months_so_qty,
        last_6months_so_qty::number(38,6) as last_6months_so_qty,
        last_12months_so_qty::number(38,6) as last_12months_so_qty,
        last_3months_so_val::number(38,17) as last_3months_so_val,
        last_3months_so_val_usd::number(38,5) as last_3months_so_val_usd,
        last_6months_so_val::number(38,17) as last_6months_so_val,
        last_6months_so_val_usd::number(38,5) as last_6months_so_val_usd,
        last_12months_so_val::number(38,17) as last_12months_so_val,
        last_12months_so_val_usd::number(38,5) as last_12months_so_val_usd,
        propagate_flag::varchar(1) as propagate_flag,
        propagate_from::number(18,0) as propagate_from,
        reason::varchar(100) as reason,
        last_36months_so_val::number(38,17) as last_36months_so_val,
        healthy_inventory::varchar(1) as healthy_inventory,
        min_date::date as min_date,
        diff_weeks::number(38,0) as diff_weeks,
        l12m_weeks::number(38,0) as l12m_weeks,
        l6m_weeks::number(38,0) as l6m_weeks,
        l3m_weeks::number(38,0) as l3m_weeks,
        l12m_weeks_avg_sales::number(38,17) as l12m_weeks_avg_sales,
        l6m_weeks_avg_sales::number(38,17) as l6m_weeks_avg_sales,
        l3m_weeks_avg_sales::number(38,17) as l3m_weeks_avg_sales,
        l12m_weeks_avg_sales_usd::number(38,5) as l12m_weeks_avg_sales_usd,
        l6m_weeks_avg_sales_usd::number(38,5) as l6m_weeks_avg_sales_usd,
        l3m_weeks_avg_sales_usd::number(38,5) as l3m_weeks_avg_sales_usd,
        l12m_weeks_avg_sales_qty::number(38,6) as l12m_weeks_avg_sales_qty,
        l6m_weeks_avg_sales_qty::number(38,6) as l6m_weeks_avg_sales_qty,
        l3m_weeks_avg_sales_qty::number(38,6) as l3m_weeks_avg_sales_qty
    from transformed
)
select * from final