{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (store_no,product_code,start_date) IN (SELECT DISTINCT store_no,product_code,start_date from {{ source('ntasdl_raw', 'sdl_tw_pos_watson_store') }});
        {% endif %}"
    )
}}
with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_watson_store') }}
),
final as 
(
    select 
    store_no::varchar(40) as store_no,
	store_name::varchar(40) as store_name,
	product_code::varchar(40) as product_code,
	product_description::varchar(500) as product_description,
	selling_qty::number(18,0) as selling_qty,
	selling_amt::number(16,5) as selling_amt,
	department::varchar(40) as department,
	start_date::date as start_date,
	end_date::date as end_date,
	filename::varchar(60) as filename,
	run_id::varchar(20) as run_id,
	current_timestamp::timestamp_ntz(9) as crt_dttm,
	current_timestamp::timestamp_ntz(9) as upd_dttm
    from source
)
select * from final