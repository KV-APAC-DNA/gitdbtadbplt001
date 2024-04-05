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
        ware_house,
        pe_item_no,
        item_description,
        stock_details_promo_ind,
        stock_details_vendor,
        stock_details_vendor_description,
        stock_details_sub_vendor,
        stock_details_lead_time,
        stock_details_buyer_number,
        stock_details_buyer_name,
        stock_details_stock_control_email,
        stock_details_soh,
        stock_details_soo,
        stock_details_awd,
        stock_details_weeks_cover,
        inbound_ordered_cases,
        inbound_received_cases,
        inbound_po_number,
        item_details_item_sub_range_code,
        item_details_item_sub_range_description,
        item_details_pack_size,
        item_details_buying_master_pack,
        item_details_retail_unit,
        item_details_buy_in_pallet,
        item_details_ti,
        item_details_hi,
        item_details_pallet,
        item_details_item_status,
        item_details_delete_code,
        item_details_deletion_date,
        item_details_metcash_item_type,
        item_details_ord_split_cat_code,
        item_details_ndc_item,
        item_details_imported_goods,
        item_details_code_date,
        item_details_packed_on_date,
        item_details_incremental_days,
        item_details_max_shelf_days,
        item_details_receiving_limit,
        item_details_dispatch_limit,
        item_details_current_dd,
        item_details_date_added_to_og,
        item_details_consumer_gtin,
        item_details_inner_gtin,
        item_details_outer_gtin,
        sales_week_6,
        sales_week_100,
        sales_week_4,
        sales_week_3,
        sales_week_2,
        sales_week_1,
        sales_this_week
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
