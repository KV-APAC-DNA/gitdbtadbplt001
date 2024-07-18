with wks_hcp_360_iqvia_brand_jbaby_temp as 
(
    select * from {{ ref('hcpwks_integration__wks_hcp_360_iqvia_brand_jbaby_temp') }}
),
trans as 
(
    SELECT base.country,
    base.source_system,
    base.brand,
    base.report_brand_reference,
    base.iqvia_brand,
    base.product_description,
    base.pack_description,
    base.region,
    base.pack_volume,
    base.activty_date as activity_till_date,
    base.activty_date -360 as activity_from_date,
    sum(derived.NoofPrescritions) as MAT_NoofPrescritions,
    avg(derived.NoofPrescribers) as MAT_NoofPrescribers
FROM wks_hcp_360_iqvia_brand_jbaby_temp base,
    (
        SELECT country,
            source_system,
            brand,
            report_brand_reference,
            iqvia_brand,
            product_description,
            pack_description,
            region,
            pack_volume,
            activty_date,
            NoofPrescritions,
            NoofPrescribers
        FROM wks_hcp_360_iqvia_brand_jbaby_temp
    ) derived
WHERE base.country = derived.country
    AND base.source_system = derived.source_system
    AND base.brand = derived.brand
    AND base.report_brand_reference = derived.report_brand_reference
    AND base.iqvia_brand = derived.iqvia_brand
    AND base.region = derived.region
    AND base.Product_Description = derived.Product_Description
    AND base.Pack_Description = derived.Pack_Description
    AND base.pack_volume = derived.pack_volume
    AND derived.activty_date between activity_from_date and activity_till_date
GROUP BY base.country,
    base.source_system,
    base.brand,
    base.report_brand_reference,
    base.iqvia_brand,
    base.product_description,
    base.pack_description,
    base.region,
    base.pack_volume,
    base.activty_date,
    base.activty_date -360
),
final as 
(
    select 
    country::varchar(2) as country,
	source_system::varchar(5) as source_system,
	brand::varchar(5) as brand,
	report_brand_reference::varchar(200) as report_brand_reference,
	iqvia_brand::varchar(11) as iqvia_brand,
	product_description::varchar(50) as product_description,
	pack_description::varchar(100) as pack_description,
	region::varchar(20) as region,
	pack_volume::varchar(20) as pack_volume,
	activity_till_date::date as activity_till_date,
	activity_from_date::date as activity_from_date,
	mat_noofprescritions::number(38,5) as mat_noofprescritions,
	mat_noofprescribers::number(38,5) as mat_noofprescribers
    from 
    trans
)
select * from final