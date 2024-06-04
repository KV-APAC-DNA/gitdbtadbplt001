with source as
(
    select * from {{ ref('idnitg_integration__itg_distributor_ivy_merchandising') }}
),
final as
(
    select 
    distributor_code::varchar(25) as distributor_code,
	distributor_name::varchar(100) as distributor_name,
	sales_repcode::varchar(50) as sales_repcode,
	sales_repname::varchar(100) as sales_repname,
	channel_name::varchar(100) as channel_name,
	sub_channel_name::varchar(100) as sub_channel_name,
	retailer_code::varchar(25) as retailer_code,
	retailer_name::varchar(100) as retailer_name,
	month::varchar(30) as month,
	to_date(surveydate) as surveydate,
	aq_name::varchar(500) as aq_name,
	srd_answer::varchar(100) as srd_answer,
	link::varchar(500) as link,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final