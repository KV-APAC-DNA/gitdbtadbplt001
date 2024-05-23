with source as
(
    select * from {{ ref('idnitg_integration__itg_id_ps_pricing') }}
),
final as
(
    select
    outlet_id,
	outlet_name,
	province,
	city,
	channel,
	merchandiser_id,
	merchandiser_name,
	cust_group,
	input_date,
	day_name,
	franchise,
	put_up,
	competitor,
	price_type,
	price,
	crt_dttm
	from source
)
select * from final