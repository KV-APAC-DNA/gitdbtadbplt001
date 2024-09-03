{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}} itg
        USING {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_hcp_details') }} sdl
        WHERE sdl.SUBSCRIBER_KEY=itg.SUBSCRIBER_KEY;
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_hcp_details') }}
),
final as
(
    SELECT *,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    FROM source
)
select first_name::varchar(100) as first_name,
    last_name::varchar(100) as last_name,
    mobile_number::varchar(20) as mobile_number,
    mobile_country_code::varchar(5) as mobile_country_code,
    birthday_month::number(10,0) as birthday_month,
    birthday_year::number(18,0) as birthday_year,
    address_1::varchar(100) as address_1,
    address_2::varchar(100) as address_2,
    address_city::varchar(20) as address_city,
    address_zipcode::number(10,0) as address_zipcode,
    subscriber_key::varchar(100) as subscriber_key,
    website_unique_id::varchar(50) as website_unique_id,
    fa_response_id::varchar(20) as fa_response_id,
    source::varchar(20) as source,
    medium::varchar(20) as medium,
    brand::varchar(50) as brand,
    country::varchar(20) as country,
    campaign_id::varchar(50) as campaign_id,
    opt_in_for_campaign::varchar(10) as opt_in_for_campaign,
    opt_in_for_communication::varchar(10) as opt_in_for_communication,
    opt_in_for_jnj_communication::varchar(10) as opt_in_for_jnj_communication,
    created_date::timestamp_ntz(9) as created_date,
    updated_date::timestamp_ntz(9) as updated_date,
    unsubscribe_date::timestamp_ntz(9) as unsubscribe_date,
    are_you_board_certified_professional::varchar(10) as are_you_board_certified_professional,
    status::varchar(10) as status,
    profession::varchar(50) as profession,
    specialty::varchar(50) as specialty,
    license_number::varchar(20) as license_number,
    licensing_city::varchar(20) as licensing_city,
    graduation_year::number(18,0) as graduation_year,
    practice_name::varchar(20) as practice_name,
    hospital_affiliation::varchar(30) as hospital_affiliation,
    promo_code::varchar(20) as promo_code,
    reason_code::varchar(20) as reason_code,
    secondary_mobile_number::varchar(20) as secondary_mobile_number,
    ip_address::varchar(20) as ip_address,
    is_confirmation_email_sent::varchar(10) as is_confirmation_email_sent,
    salutation::varchar(10) as salutation,
    territory_name::varchar(30) as territory_name,
    position::varchar(20) as position,
    primary_parent::varchar(20) as primary_parent,
    date_of_sample_order::timestamp_ntz(9) as date_of_sample_order,
    email::varchar(50) as email,
    device_id::varchar(20) as device_id,
    how_did_you_hear_about_us::varchar(20) as how_did_you_hear_about_us,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name
    from final