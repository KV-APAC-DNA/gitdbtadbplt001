{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_emart_evydy_line') }}
),
final as(
    select 
        mesg_name,
        mesg_id,
        mesg_from,
        mesg_to,
        sale_date,
        mesg_no,
        line_no,
        sale_id,
        sale_id_map,
        sale_gln,
        sale_name,
        sale_date_type,
        cate_code,
        prod_code,
        prod_code_map,
        prod_name1,
        prod_name2,
        sale_amnt,
        sale_mon_amnt,
        unit_price,
        sale_qty,
        sale_unit,
        sale_mon_qty,
        sale_div,
        sale_type,
        online_sign,
        online_sale_qty,
        online_sale_amnt,
        disc_sign,
        disc_sale_amnt,
        reg_date,
        upd_date,
        convert_timezone('UTC', current_timestamp()) as crt_dttm, 
        convert_timezone('UTC', current_timestamp()) as updt_dttm 
    from source
)

select * from final