{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_sfmc_invoice_data') }}
),
final as
(
    select 
        purchase_date as purchase_date,
        channel as channel,
        product as product,
        status as status,
        created_date as created_date,
        completed_date as completed_date,
        subscriber_key as subscriber_key,
        points as points,
        show_record as show_record,
        qty as qty,
        invoice_type as invoice_type,
        seller_nm as seller_nm,
        product_category as product_category,
        website_unique_id as website_unique_id,
        invoice_num as invoice_num,
        epsilon_price_per_unit as epsilon_price_per_unit,
        epsilon_amount as epsilon_amount,
        epsilon_total_amount as epsilon_total_amount,
        file_name as file_name,
        crtd_dttm as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
