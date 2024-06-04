{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw','sdl_distributor_ivy_merchandising') }}
),
final as (
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
	surveydate::varchar(100) as surveydate,
	aq_name::varchar(500) as aq_name,
	srd_answer::varchar(100) as srd_answer,
	link::varchar(500) as link,
	cdl_dttm::varchar(50) as cdl_dttm,
	run_id::number(14,0) as run_id,
	source_file_name::varchar(256) as source_file_name
    from source
)

select * from final