{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_dstr_coles_inv') }}
),
final as(
    select
        vendor,
        vendor_name,
        dc_state_shrt_desc,
        dc_state_desc,
        dc,
        dc_desc,
        category,
        category_desc,
        order_item,
        order_item_desc,
        ean,
        inv_date,
        closing_soh_nic,
        closing_soh_qty_ctns,
        closing_soh_qty_octns,
        closing_soh_qty_unit,
        dc_days_on_hand,
        current_timestamp()::timestamp_ntz(9) as crtd_dt
    from source
)
select * from final