{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_emart_evydy_header') }}
),
final as(
    select 
    	mesg_name,
    	mesg_id,
    	mesg_from,
    	mesg_to,
    	sale_date,
    	mesg_date,
    	mesg_no,
    	mesg_code,
    	mesg_func_code,
    	send_date,
    	sale_date_type,
    	send_gln,
    	send_id,
    	send_name,
    	recv_gln,
    	recv_id,
    	recv_name,
    	sendyn,
    	reg_date,
    	upd_date,
        convert_timezone('UTC', current_timestamp()) as crt_dttm, 
        convert_timezone('UTC', current_timestamp()) as updt_dttm 
    from source
)

select * from final