{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_sfmc_consumer_master') }}
),
final as
(
    select 
       first_name as first_name,
        last_name as last_name,
        mobile_num as mobile_num,
        mobile_cntry_cd as mobile_cntry_cd,
        birthday_mnth as birthday_mnth,
        birthday_year as birthday_year,
        address_1 as address_1,
        address_2 as address_2,
        address_city as address_city,
        address_zipcode as address_zipcode,
        subscriber_key as subscriber_key,
        website_unique_id as website_unique_id,
        source as source,
        medium as medium,
        brand as brand,
        address_cntry as address_cntry,
        campaign_id as campaign_id,
        created_date as created_date,
        updated_date as updated_date,
        unsubscribe_date as unsubscribe_date,
        email as email,
        full_name as full_name,
        last_logon_time as last_logon_time,
        remaining_points as remaining_points,
        redeemed_points as redeemed_points,
        total_points as total_points,
        gender as gender,
        line_id as line_id,
        line_name as line_name,
        line_email as line_email,
        line_channel_id as line_channel_id,
        address_region as address_region,
        tier as tier,
        opt_in_for_communication as opt_in_for_communication,
        file_name as file_name,
        crtd_dttm as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
