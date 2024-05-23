with source as
(
    select * from {{ ref('idnitg_integration__itg_id_ps_promotion') }}
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
	promo_desc,
	photo_link,
	posm_execution_flag,
	crt_dttm
	from source
)
select * from final