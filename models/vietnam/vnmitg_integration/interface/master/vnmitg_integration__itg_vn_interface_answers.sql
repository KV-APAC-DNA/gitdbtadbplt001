{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['ise_id','slsper_id','cust_code','ques_no','shop_code','answer_seq','createddate']
    )
}}

with source as 
(
    select *, dense_rank() over (partition by ise_id,slsper_id,cust_code,ques_no,shop_code,answer_seq,createddate order by filename desc) rnk
    from {{ source('vnmsdl_raw','sdl_vn_interface_answers') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_answers__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_answers__duplicate_test')}}
        ) qualify rnk = 1

),

final as
(
    select
    cust_code::varchar(255) as cust_code,
	slsper_id::varchar(255) as slsper_id,
	shop_code::varchar(255) as shop_code,
	ise_id::varchar(255) as ise_id,
	ques_no::number(10,0) as ques_no,
	answer_seq::number(20,0) as answer_seq,
	answer_value::number(20,0) as answer_value,
	score::number(20,0) as score,
	oos::number(20,0) as oos,
	createddate::varchar(255) as createddate,
	filename::varchar(255) as filename,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final