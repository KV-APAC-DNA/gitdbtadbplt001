
with transformed as (
    select * from {{ source('aspwks_integration', 'wks_my_inventory_health_analysis_propagation_final') }}
    union all 
    select 
        year,
        year_quarter,
        month_year,
        mnth_no,
        country_name,
        dstrbtr_grp_cd,
        dstrbtr_grp_cd_name,
        global_prod_franchise,
        global_prod_brand,
        global_prod_sub_brand,
        global_prod_variant,
        global_prod_segment,
        global_prod_subsegment,
        global_prod_category,
        global_prod_subcategory,
        global_put_up_desc,
        sku_cd,
        sku_description,

        --greenlight_sku_flag,
        pka_product_key,
        pka_product_key_description,
        product_key,
        product_key_description,
        from_ccy,
        to_ccy,
        exch_rate,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_cust_chnl_key,
        sap_cust_chnl_desc,
        sap_cust_sub_chnl_key,
        sap_sub_chnl_desc,
        sap_go_to_mdl_key,
        sap_go_to_mdl_desc,
        sap_bnr_key,
        sap_bnr_desc,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env,
        region,
        zone_or_area,
        si_sls_qty,
        si_gts_val,
        si_gts_val_usd,
        inventory_quantity,
        inventory_val,
        inventory_val_usd,
        so_sls_qty,
        so_trd_sls,
        so_trd_sls_usd,
        si_all_db_val,
        si_all_db_val_usd,
        si_inv_db_val,
        si_inv_db_val_usd,
        last_3months_so_qty,
        last_6months_so_qty,
        last_12months_so_qty,
        last_3months_so_val,
        last_3months_so_val_usd,
        last_6months_so_val,
        last_6months_so_val_usd,
        last_12months_so_val,
        last_12months_so_val_usd,
        propagate_flag,
        propagate_from,
        reason,
        last_36months_so_val,
        healthy_inventory,
        min_date,
        diff_weeks,
        l12m_weeks,
        l6m_weeks,
        l3m_weeks,
        l12m_weeks_avg_sales,
        l6m_weeks_avg_sales,
        l3m_weeks_avg_sales,
        l12m_weeks_avg_sales_usd,
        l6m_weeks_avg_sales_usd,
        l3m_weeks_avg_sales_usd,
        l12m_weeks_avg_sales_qty,
        l6m_weeks_avg_sales_qty,
        l3m_weeks_avg_sales_qty
    from {{ source('aspwks_integration', 'wks_china_reg_inventory_health_analysis_propagation_final') }}

    union all 
    select * from {{ source('aspwks_integration', 'wks_hk_inventory_health_analysis_propagation_final') }}

    union all   
    select * from {{ source('aspwks_integration', 'wks_india_inventory_health_analysis_propagation_final') }}

    union all   
    select * from {{ source('aspwks_integration', 'wks_indonesia_inventory_health_analysis_propagation_final') }}

    union all 
    select * from {{ source('aspwks_integration', 'wks_kr_inventory_health_analysis_propagation_final') }}

    union all 
    select * from {{ source('aspwks_integration', 'wks_ph_inventory_health_analysis_propagation_final') }}

    union all 
    select * from {{ source('aspwks_integration', 'wks_tw_inventory_health_analysis_propagation_final') }}

    union all 
    select  
        *
        ,NULL as last_36months_so_val
        ,'Y' as healthy_inventory
        ,NULL as min_date
        ,NULL as diff_weeks
        ,NULL as l12m_weeks
        ,NULL as l6m_weeks 
        ,NULL as l3m_weeks
        ,last_12months_so_val /52 as l12m_weeks_avg_sales  
        ,last_6months_so_val /26 as l6m_weeks_avg_sales
        ,last_3months_so_val /13 as l3m_weeks_avg_sales
        ,last_12months_so_val_usd /52 as l12m_weeks_avg_sales_usd  
        ,last_6months_so_val_usd /26 as l6m_weeks_avg_sales_usd
        ,last_3months_so_val_usd /13 as l3m_weeks_avg_sales_usd
        ,last_12months_so_qty /52 as l12m_weeks_avg_sales_qty  
        ,last_6months_so_qty /26 as l6m_weeks_avg_sales_qty
        ,last_3months_so_qty /13 as l3m_weeks_avg_sales_qty
    from {{ source('jpnwks_integration', 'wks_japan_inventory_health_analysis_propagation') }}

    union all 
    select * from {{ source('pcfwks_integration', 'pacific_inventory_health_analysis_propagation_final') }}

    union all 
    select * from {{ ref('thawks_integration__wks_thailand_inventory_health_analysis_propagation_final') }}

    union all 
    select * from {{ source('osewks_integration', 'wks_vietnam_inventory_health_analysis_propagation_final') }}
),
final as(
    select
        IFF(trim(year)='',null,year)::number(18,0) as year,
        IFF(trim(year_quarter)='',null,year_quarter)::number(18,0)::varchar(14) as year_quarter,
        IFF(trim(month_year)='',null,month_year)::number(18,0)::varchar(23) as month_year,
        IFF(trim(month_number)='',null,month_number)::number(18,0) as month_number,
        country_name::varchar(40) as country_name,
        dstrbtr_grp_cd::varchar(100) as distributor_id,
        distributor_id_name::varchar(250) as distributor_id_name,
        franchise::varchar(50) as franchise,
        brand::varchar(50) as brand,
        prod_sub_brand::varchar(100) as prod_sub_brand,
        variant::varchar(100) as variant,
        segment::varchar(50) as segment,
        prod_subsegment::varchar(100) as prod_subsegment,
        prod_category::varchar(50) as prod_category,
        prod_subcategory::varchar(50) as prod_subcategory,
        put_up_description::varchar(100) as put_up_description,
        sku_cd::varchar(255) as sku_cd,
        sku_description::varchar(100) as sku_description,
        pka_product_key::varchar(200) as pka_product_key,
        pka_product_key_description::varchar(500) as pka_product_key_description,
        product_key::varchar(200) as product_key,
        product_key_description::varchar(500) as product_key_description,
        from_ccy::varchar(5) as from_ccy,
        to_ccy::varchar(5) as to_ccy,
        exch_rate::number(15,5) as exch_rate,
        sap_prnt_cust_key::varchar(50) as sap_prnt_cust_key,
        sap_prnt_cust_desc::varchar(382) as sap_prnt_cust_desc,
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
        region::varchar(255) as region,
        zone_or_area::varchar(255) as zone_or_area,
        si_sls_qty::number(38,5) as si_sls_qty,
        si_gts_val::number(38,5) as si_gts_val,
        si_gts_val_usd::number(38,5) as si_gts_val_usd,
        inventory_quantity::number(38,5) as inventory_quantity,
        inventory_val::number(38,5) as inventory_val,
        inventory_val_usd::number(38,5) as inventory_val_usd,
        so_sls_qty::number(38,5) as so_sls_qty,
        so_grs_trd_sls::number(38,5) as so_grs_trd_sls,
        so_grs_trd_sls_usd::float as so_grs_trd_sls_usd,
        si_all_db_val::number(38,5) as si_all_db_val,
        si_all_db_val_usd::number(38,5) as si_all_db_val_usd,
        si_inv_db_val::number(38,5) as si_inv_db_val,
        si_inv_db_val_usd::number(38,5) as si_inv_db_val_usd,
        last_3months_so_qty::number(38,4) as last_3months_so_qty,
        last_6months_so_qty::number(38,4) as last_6months_so_qty,
        last_12months_so_qty::number(38,4) as last_12months_so_qty,
        last_3months_so_val::number(38,4) as last_3months_so_val,
        last_3months_so_val_usd::number(38,5) as last_3months_so_val_usd,
        last_6months_so_val::number(38,4) as last_6months_so_val,
        last_6months_so_val_usd::number(38,5) as last_6months_so_val_usd,
        last_12months_so_val::number(38,4) as last_12months_so_val,
        last_12months_so_val_usd::number(38,5) as last_12months_so_val_usd,
        propagate_flag::varchar(1) as propagate_flag,
        propagate_from::number(18,0) as propagate_from,
        reason::varchar(100) as reason,
        last_36months_so_val::number(38,4) as last_36months_so_val,
        healthy_inventory::varchar(1) as healthy_inventory,
        min_date::date as min_date,
        diff_weeks::number(18,0) as diff_weeks,
        l12m_weeks::number(18,0) as l12m_weeks,
        l6m_weeks::number(18,0) as l6m_weeks,
        l3m_weeks::number(18,0) as l3m_weeks,
        l12m_weeks_avg_sales::number(38,17) as l12m_weeks_avg_sales,
        l6m_weeks_avg_sales::number(38,17) as l6m_weeks_avg_sales,
        l3m_weeks_avg_sales::number(38,17) as l3m_weeks_avg_sales,
        l12m_weeks_avg_sales_usd::number(38,5) as l12m_weeks_avg_sales_usd,
        l6m_weeks_avg_sales_usd::number(38,5) as l6m_weeks_avg_sales_usd,
        l3m_weeks_avg_sales_usd::number(38,5) as l3m_weeks_avg_sales_usd,
        l12m_weeks_avg_sales_qty::number(38,5) as l12m_weeks_avg_sales_qty,
        l6m_weeks_avg_sales_qty::number(38,5) as l6m_weeks_avg_sales_qty,
        l3m_weeks_avg_sales_qty::number(38,5) as l3m_weeks_avg_sales_qty 
    from transformed
)
select * from final