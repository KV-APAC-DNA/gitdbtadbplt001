{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_emart_header') }}
),
final as(
    select 
        mesg_no,
        mesg_code,
        mesg_func_code,
        mesg_date,
        sale_date,
        sale_date_form,
        send_code,
        send_ean_code,
        send_name,
        recv_qual,
        recv_ean_code,
        recv_name,
        part_qual,
        part_ean_code,
        part_id,
        part_name,
        sender_id,
        recv_date,
        recv_time,
        file_size,
        file_path,
        lega_tran,
        regi_date,
        convert_timezone('UTC', current_timestamp()) as crt_dttm, 
        convert_timezone('UTC', current_timestamp()) as updt_dttm 
    from source
)

select * from final