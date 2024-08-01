with wks_hcp360_invoice_data as
(
    select * from {{ ref('hcpwks_integration__wks_hcp360_invoice_data') }}
),
itg_mds_in_hcp_sales_hierarchy_mapping as
(
    select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_hierarchy_mapping') }}
),
transformed as
(
    SELECT DISTINCT s.mth_mm,
    s.Brand,
    s.customer_code,
    s.customer_name,
    m.region_code,
    m.zone_code,
    m.sales_area_code,
    sum(invoice_val) AS invoice_value
    FROM wks_hcp360_invoice_data s
    LEFT JOIN itg_mds_in_hcp_sales_hierarchy_mapping m ON s.customer_code = m.rds_code::VARCHAR
    AND s.brand = m.brand_name_code
    AND m.validationstatus = 'Validation Succeeded'
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
