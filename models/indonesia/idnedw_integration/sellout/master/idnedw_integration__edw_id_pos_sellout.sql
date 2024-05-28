with source as(
    select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_VW_ID_POS_SELLOUT 
),
final as(
    select 
    	sap_cntry_cd::varchar(2) as sap_cntry_cd,
        sap_cntry_nm::varchar(9) as sap_cntry_nm,
        dataset::varchar(7) as dataset,
        dstrbtr_grp_cd::varchar(25) as dstrbtr_grp_cd,
        year::varchar(10) as year,
        yearmonth::varchar(10) as yearmonth,
        customer_brnch_code::varchar(100) as customer_brnch_code,
        customer_brnch_name::varchar(100) as customer_brnch_name,
        customer_store_code::varchar(50) as customer_store_code,
        customer_store_name::varchar(50) as customer_store_name,
        customer_franchise::varchar(210) as customer_franchise,
        customer_brand::varchar(50) as customer_brand,
        customer_product_code::varchar(50) as customer_product_code,
        customer_product_desc::varchar(251) as customer_product_desc,
        jj_sap_prod_id::varchar(50) as jj_sap_prod_id,
        brand::varchar(50) as brand,
        brand2::varchar(50) as brand2,
        sku_sales_cube::varchar(200) as sku_sales_cube,
        customer_product_range::varchar(100) as customer_product_range,
        customer_product_group::varchar(110) as customer_product_group,
        customer_store_class::varchar(50) as customer_store_class,
        customer_store_channel::varchar(50) as customer_store_channel,
        sales_qty::number(18,2) as sales_qty,
        sales_value::number(37,2) as sales_value,
        service_level::number(10,2) as service_level,
        sales_order::number(18,2) as sales_order,
        share::number(10,2) as share,
        store_stock_qty::varchar(1) as store_stock_qty,
        store_stock_value::varchar(1) as store_stock_value,
        branch_stock_qty::varchar(1) as branch_stock_qty,
        branch_stock_value::varchar(1) as branch_stock_value,
        stock_uom::varchar(1) as stock_uom,
        stock_days::varchar(1) as stock_days,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm
    from source 
)
select * from final