{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where cntry_cd='KR' and file_name in (select distinct file_name from {{ source('ntasdl_raw','sdl_kr_sfmc_naver_data') }});
        {% endif %}",
        post_hook="delete from {{this}} where naver_id not in (select naver_id from {{ source('ntasdl_raw','sdl_kr_sfmc_naver_data') }} where cntry_cd='KR' and file_name in(select distinct file_name from {{ source('ntasdl_raw','sdl_kr_sfmc_naver_data') }})) and cntry_cd='KR';"
)}}

with source as (
    select * from {{ source('ntasdl_raw','sdl_kr_sfmc_naver_data') }}
),
final as (
    select
        'KR'::varchar(100) as cntry_cd,
        naver_id::varchar(50) as naver_id,
        birth_year::varchar(4) as birth_year,
        gender::varchar(50) as gender,
        total_purchase_amount::varchar(50) as total_purchase_amount,
        total_number_of_purchases::varchar(50) as total_number_of_purchases,
        membership_grade_level::varchar(50) as membership_grade_level,
        marketing_message_viewing_receiving::varchar(50) as marketing_message_viewing_receiving,
        coupon_usage_issuance::varchar(50) as coupon_usage_issuance,
        number_of_reviews::varchar(50) as number_of_reviews,
        number_of_comments::varchar(50) as number_of_comments,
        number_of_attendances::varchar(50) as number_of_attendances,
        opt_in_for_jnj_communication::varchar(50) as opt_in_for_jnj_communication,
        notification_subscription::varchar(50) as notification_subscription,
        updated_date::varchar(50) as updated_date,
        membership_registration_date::varchar(50) as membership_registration_date,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final