{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if var('job_to_execute') == 'th_crm_files' %}
                    delete from {{this}} where cntry_cd='TH' and crtd_dttm < (select min(crtd_dttm) from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master') }});
                    {% elif var('job_to_execute') == 'ph_crm_files' %}
                    delete from {{this}} where cntry_cd='PH';
                    {% endif %}
                "
    )
}}
with
source as
(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master') }}
),
wks_itg_sfmc_consumer_master as
(
    select * from phlwks_integration__wks_itg_sfmc_consumer_master
)
{% if var("job_to_execute") == 'th_crm_files' %}
,
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
    where rnk=1
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        and source.crtd_dttm > (select max(crtd_dttm) from {{ this }})
    {% endif %}
)

select * from final
{% elif var("job_to_execute") == 'ph_crm_files' %}
,

final as
(
    select
        cntry_cd::varchar(10) as cntry_cd,
        first_name::varchar(200) as first_name,
        last_name::varchar(200) as last_name,
        mobile_num::varchar(30) as mobile_num,
        mobile_cntry_cd::varchar(10) as mobile_cntry_cd,
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
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        have_kid::varchar(20) as have_kid,
        age::number(18,0) as age,
        valid_from::timestamp_ntz(9) as valid_from,
        valid_to::timestamp_ntz(9) as valid_to,
        delete_flag::varchar(10) as delete_flag,
        subscriber_status::varchar(100) as subscriber_status,
        opt_in_for_jnj_communication::varchar(100) as opt_in_for_jnj_communication,
        opt_in_for_campaign::varchar(100) as opt_in_for_campaign
    from wks_itg_sfmc_consumer_master
)
select * from final

{% endif %}