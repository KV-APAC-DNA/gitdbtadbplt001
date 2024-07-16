with wks_hcp_360_iqvia_brand_orsl_temp as
(
    select * from dev_dna_core.snapindwks_integration.wks_hcp_360_iqvia_brand_orsl_temp
),
transformed as
(
SELECT base.country,
    base.source_system,
    base.data_source,
    base.brand_category,
    base.report_brand_reference,
    base.iqvia_brand,
    base.region,
    base.Product_Description,
    base.Pack_Description,
    base.Brand,
    base.activty_date AS activity_till_date,
    base.activty_date - 360 AS activity_from_date,
    sum(derived.NoofPrescritions) AS MAT_NoofPrescritions,
    avg(derived.NoofPrescribers) AS MAT_NoofPrescribers
FROM wks_hcp_360_iqvia_brand_orsl_temp base,
    (
        SELECT country,
            source_system,
            data_source,
            brand_category,
            report_brand_reference,
            iqvia_brand,
            region,
            Product_Description,
            Pack_Description,
            Brand,
            activty_date,
            NoofPrescritions,
            NoofPrescribers
        FROM wks_hcp_360_iqvia_brand_orsl_temp
        ) derived
WHERE base.country = derived.country
    AND base.source_system = derived.source_system
    AND base.data_source = derived.data_source
    AND base.brand_category = derived.brand_category
    AND base.report_brand_reference = derived.report_brand_reference
    AND base.iqvia_brand = derived.iqvia_brand
    AND base.region = derived.region
    AND base.Product_Description = derived.Product_Description
    AND base.Pack_Description = derived.Pack_Description
    AND base.Brand = derived.Brand
    AND derived.activty_date BETWEEN activity_from_date
        AND activity_till_date
GROUP BY base.country,
    base.source_system,
    base.data_source,
    base.brand_category,
    base.report_brand_reference,
    base.iqvia_brand,
    base.region,
    base.Product_Description,
    base.Pack_Description,
    base.Brand,
    base.activty_date,
    base.activty_date - 360
),
final as 
(   
    select
    	country::varchar(2) as country,
    	source_system::varchar(5) as source_system,
    	data_source::varchar(20) as data_source,
    	brand_category::varchar(50) as brand_category,
    	report_brand_reference::varchar(200) as report_brand_reference,
    	iqvia_brand::varchar(10) as iqvia_brand,
    	region::varchar(20) as region,
    	product_description::varchar(50) as product_description,
    	pack_description::varchar(100) as pack_description,
    	brand::varchar(50) as brand,
    	activity_till_date::date as activity_till_date,
    	activity_from_date::date as activity_from_date,
    	mat_noofprescritions::number(38,5) as mat_noofprescritions,
    	mat_noofprescribers::number(38,5) as mat_noofprescribers
    from transformed 
)
select * from final
