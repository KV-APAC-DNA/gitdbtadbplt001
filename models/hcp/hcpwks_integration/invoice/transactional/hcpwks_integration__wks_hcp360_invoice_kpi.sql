with wks_hcp360_invoice_data as
(
    select * from DEV_DNA_CORE.SNAPINDWKS_INTEGRATION.WKS_HCP360_INVOICE_DATA
),
itg_mds_in_hcp_sales_hierarchy_mapping as
(
    select * from DEV_DNA_CORE.SNAPINDITG_INTEGRATION.ITG_MDS_IN_HCP_SALES_HIERARCHY_MAPPING
),
transformed as
(
    SELECT DISTINCT s.mth_mm,
    s.Brand,
    s.customer_code,
    s.customer_name,
    --case when d.channel_name = 'Wholesale' and d.retailer_category_cd = 'Pharma' then 'Wholesale-Pharma'
    --     when d.channel_name = 'Wholesale' and d.retailer_category_cd != 'Pharma' then 'Wholesale-Non-Pharma'
    --       when d.channel_name = 'GT'      and d.retailer_category_cd = 'CH' then 'GT-Chemist'     /*Updated as part of Ph-2 enhancement*/
    --       when d.channel_name = 'GT'      and d.retailer_category_cd != 'CH' then 'GT-Others'     /*Updated as part of Ph-2 enhancement*/
    --      else d.channel_name end as channel_name  ,
    m.region_code,
    m.zone_code,
    m.sales_area_code,
    sum(invoice_val) AS invoice_value
    --  s.invoice_val as invoice_value
    FROM wks_hcp360_invoice_data s
    LEFT JOIN itg_mds_in_hcp_sales_hierarchy_mapping m ON s.customer_code = m.rds_code::VARCHAR
    AND s.brand = m.brand_name_code
    --left join in_edw.edw_rpt_sales_details d
    --on s.customer_code = d.customer_code
    --and s.mth_mm = d.mth_mm
    --and upper(s.product_category_name) = upper(d.product_category_name)   ---Filter Added as a part of AEBU-8453 .sales_value count mismatch
    --and upper(s.variant_name) = upper(d.variant_name)					  ---Filter Added as a part of AEBU-8453 .sales_value count mismatch
    --and upper(s.region) = upper(d.region_name)							  ---Filter Added as a part of AEBU-8453 .sales_value count mismatch
    --and upper(s.zone) = upper(d.zone_name)								  ---Filter Added as a part of AEBU-8453 .sales_value count mismatch
    --and upper(s.territory) = upper(d.territory_name)					  ---Filter Added as a part of AEBU-8453 .sales_value count mismatch
    --and upper(s.franchise_name) = upper(d.franchise_name)
    AND m.validationstatus = 'Validation Succeeded' ---to remove duplicates from the sales_hierarchy_mapping with awaiting approval sales mapping
    GROUP BY 1,
    2,
    3,
    4,
    5,
    6,
    7
),
final as 
(   
    select
        mth_mm::number(18,0) as mth_mm,
        brand::varchar(5) as brand,
        customer_code::varchar(20) as customer_code,
        customer_name::varchar(150) as customer_name,
        region_code::varchar(500) as region_code,
        zone_code::varchar(500) as zone_code,
        sales_area_code::varchar(500) as sales_area_code,
        invoice_value::number(38,8) as invoice_value
    from transformed 
)
select * from final 
