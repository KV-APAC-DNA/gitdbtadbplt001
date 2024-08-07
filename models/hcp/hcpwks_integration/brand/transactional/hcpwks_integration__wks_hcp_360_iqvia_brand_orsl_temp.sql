with itg_hcp360_in_iqvia_brand as(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_in_iqvia_brand') }}
),
itg_mds_hcp360_product_mapping as(
    select * from {{ ref('hcpitg_integration__itg_mds_hcp360_product_mapping') }}
),
final as(
select
	'IN' as country,
	'IQVIA' as source_system,
	iqvia.data_source as data_source,
	brand_category as brand_category,
	report_brand as report_brand_reference ,
	product_description,
	pack_description,
	brand,
	'ORSL Total' as iqvia_brand,
	zone as region,
	cast(year_month as date) as activty_date,
	no_of_prescriptions as noofprescritions,
	no_of_prescribers as noofprescribers
from
	(
		select
			nvl(m.brand, i.brand || '_COMP') as report_brand,
			i.*
		from
			itg_hcp360_in_iqvia_brand i,
			(
				select
					*
				from
					itg_mds_hcp360_product_mapping
				where
					brand = 'ORSL'
			) m
		where
			i.pack_description = m.iqvia(+)
			and i.data_source = 'ORSL'
	) iqvia
)
select * from final