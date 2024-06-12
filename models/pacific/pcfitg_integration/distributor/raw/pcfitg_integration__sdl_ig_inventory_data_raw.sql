{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_ig_inventory_data') }}
),
final as
(
    select 
        inv_dt,
        state,
        try_to_number(ware_house) as ware_house,
        try_to_number(pe_item_no) as pe_item_no,
        item_description,
        stock_details_promo_ind,
        try_to_number(stock_details_vendor) as stock_details_vendor,
        stock_details_vendor_description,
        try_to_number(stock_details_sub_vendor) as stock_details_sub_vendor,
        try_to_number(stock_details_lead_time) as stock_details_lead_time,
        stock_details_buyer_number,
        stock_details_buyer_name,
        stock_details_stock_control_email,
        try_to_number(stock_details_soh) as stock_details_soh,
        try_to_number(stock_details_soo) as stock_details_soo,
        try_to_number(stock_details_awd) as stock_details_awd,
        try_to_number(stock_details_weeks_cover) as stock_details_weeks_cover,
        try_to_number(inbound_ordered_cases) as inbound_ordered_cases,
        try_to_number(inbound_received_cases) as inbound_received_cases,
        inbound_po_number,
        try_to_number(item_details_item_sub_range_code) as item_details_item_sub_range_code,
        item_details_item_sub_range_description,
        try_to_number(item_details_pack_size) as item_details_pack_size,
        try_to_number(item_details_buying_master_pack) as item_details_buying_master_pack,
        try_to_number(item_details_retail_unit) as item_details_retail_unit,
        item_details_buy_in_pallet,
        try_to_number(item_details_ti) as item_details_ti,
        try_to_number(item_details_hi) as item_details_hi,
        try_to_number(item_details_pallet) as item_details_pallet,
        item_details_item_status,
        item_details_delete_code,
        item_details_deletion_date,
        item_details_metcash_item_type,
        item_details_ord_split_cat_code,
        item_details_ndc_item,
        item_details_imported_goods,
        item_details_code_date,
        item_details_packed_on_date,
        try_to_number(item_details_incremental_days) as item_details_incremental_days,
        try_to_number(item_details_max_shelf_days) as item_details_max_shelf_days,
        try_to_number(item_details_receiving_limit) as item_details_receiving_limit,
        try_to_number(item_details_dispatch_limit) as item_details_dispatch_limit,
        try_to_number(item_details_current_dd) as item_details_current_dd,
        item_details_date_added_to_og,
        try_to_number(item_details_consumer_gtin) as item_details_consumer_gtin,
        try_to_number(item_details_inner_gtin) as item_details_inner_gtin,
        try_to_number(item_details_outer_gtin) as item_details_outer_gtin,
        try_to_number(sales_week_6) as sales_week_6,
        try_to_number(sales_week_100) as sales_week_100,
        try_to_number(sales_week_4) as sales_week_4,
        try_to_number(sales_week_3) as sales_week_3,
        try_to_number(sales_week_2) as sales_week_2,
        try_to_number(sales_week_1) as sales_week_1,
        try_to_number(sales_this_week) as sales_this_week,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
