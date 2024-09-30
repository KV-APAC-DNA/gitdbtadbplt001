{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_symbion_dstr') }}
    where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_symbion_dstr__null_test')}}
    )
),
final as
(
    select 
        inv_dt,
        state,
        warehouse,
        warehouse_desc,
        symbion_product_no,
        symbion_product_desc,
        supplier_part_no,
        ean,
        global_std_cost,
        date_introduced,
        date_last_recived,
        oosr,
        soh_amt,
        soh_qty,
        on_order,
        back_order,
        reserved_amt_for_order,
        reserved_qty_for_order,
        reserved_amt_for_qa,
        reserved_qty_for_qa,
        available_amt,
        available_qty,
        mtd,
        month_01,
        month_02,
        month_03,
        month_04,
        month_05,
        month_06,
        month_07,
        month_08,
        month_09,
        month_10,
        month_11,
        month_12,
        month_13,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        file_name::varchar(255) as file_name
    from source
    -- {% if is_incremental() %}
    -- -- this filter will only be applied on an incremental run
    -- where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    -- {% endif %}
)
select * from final
