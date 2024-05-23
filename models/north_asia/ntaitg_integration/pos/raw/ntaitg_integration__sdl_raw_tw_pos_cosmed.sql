{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_cosmed') }}
),
final as
(
    select 
        product_code as product_code,
        product_name as product_name,
        wk1_strt_dt as wk1_strt_dt,
        wk1_end_dt as wk1_end_dt,
        wk1_qty as wk1_qty,
        wk2_strt_dt as wk2_strt_dt,
        wk2_end_dt as wk2_end_dt,
        wk2_qty as wk2_qty,
        wk3_strt_dt as wk3_strt_dt,
        wk3_end_dt as wk3_end_dt,
        wk3_qty as wk3_qty,
        wk4_strt_dt as wk4_strt_dt,
        wk4_end_dt as wk4_end_dt,
        wk4_qty as wk4_qty,
        crt_dttm as crt_dttm,
        upd_dttm as upd_dttm,
        null as filename,
        null as run_id
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final

