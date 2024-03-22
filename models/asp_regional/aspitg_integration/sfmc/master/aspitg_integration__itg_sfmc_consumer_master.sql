{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['cntry_cd']
    )
}}
with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master') }}
),

final as
(
    select
        'TH'::varchar(10) as cntry_cd,
        first_name::varchar(200) as first_name,
        last_name::varchar(200) as last_name,
        mobile_num::varchar(30) as mobile_num,
        mobile_cntry_cd ::varchar(10) as mobile_cntry_cd,
        birthday_mnth::varchar(10) as birthday_mnth,
        birthday_year::varchar(10) as birthday_year,
        address_1::varchar(255) as address_1,
        address_2::varchar(255) as address_2,
        address_city::varchar(100) as address_city,
        address_zipcode::varchar(30) as address_zipcode,
        subscriber_key::varchar(100) as subscriber_key,
        website_unique_id::varchar(150) as website_unique_id,
        source::varchar(100) as source,
        medium::varchar(100) as medium,
        brand::varchar(200) as brand,
        address_cntry::varchar(100) as address_cntry,
        campaign_id::varchar(100) as campaign_id,
        created_date::timestamp_ntz(9) as created_date,
        updated_date::timestamp_ntz(9) as updated_date,
        unsubscribe_date::timestamp_ntz(9) as unsubscribe_date,
        email::varchar(100) as email,
        full_name::varchar(200) as full_name,
        last_logon_time::timestamp_ntz(9) as last_logon_time,
        remaining_points::number(10,4) as remaining_points,
        redeemed_points::number(10,4) as redeemed_points,
        total_points::number(10,4) as total_points,
        gender::varchar(20) as gender,
        line_id::varchar(50) as line_id,
        line_name::varchar(200) as line_name,
        line_email::varchar(100) as line_email,
        line_channel_id::varchar(50) as line_channel_id,
        address_region::varchar(100) as address_region,
        tier::varchar(100) as tier,
        opt_in_for_communication::varchar(100) as opt_in_for_communication,
        have_kid::varchar(20) as have_kid,
        age::number(18,0) as age,
        file_name::varchar(255) as file_name,
        null::varchar(10) as delete_flag, 
        null::varchar(100) as subscriber_status, 
        null::varchar(100) as opt_in_for_jnj_communication, 
        null::varchar(100) as opt_in_for_campaign ,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        current_timestamp()::timestamp_ntz(9) as valid_from,
        '31-dec-9999'::timestamp_ntz(9) as valid_to
    from source
)

select * from final