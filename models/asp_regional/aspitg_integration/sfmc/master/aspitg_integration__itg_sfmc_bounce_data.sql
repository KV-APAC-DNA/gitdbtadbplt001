{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if var('sfmc_job_to_execute') == 'th_sfmc_files' %}
                    delete from {{this}} where event_date >= (select min(event_date) from {{ source('thasdl_raw','sdl_th_sfmc_bounce_data') }}) and cntry_cd = 'TH';
                    {% elif var('sfmc_job_to_execute') == 'ph_sfmc_files' %}
                    delete from {{this}} where event_date >= (select min(event_date) from {{ source('thasdl_raw','sdl_th_sfmc_bounce_data') }}) and cntry_cd = 'PH';
                     {% elif var('sfmc_job_to_execute') == 'tw_sfmc_files' %}
                    delete from {{this}} where event_date >= (select min(event_date) from {{ source('ntasdl_raw', 'sdl_tw_sfmc_bounce_data') }}) and cntry_cd = 'TW';
                    {% endif %}"
    )
}}
with source as(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('thasdl_raw', 'sdl_th_sfmc_bounce_data') }}
),
source2 as(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('phlsdl_raw', 'sdl_ph_sfmc_bounce_data') }}
),
source3 as(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('ntasdl_raw', 'sdl_tw_sfmc_bounce_data') }}
),
{% if var("sfmc_job_to_execute") == 'th_sfmc_files' %}

final as
(
    select
        'TH'::varchar(10) as cntry_cd,
        oyb_account_id::varchar(20) as oyb_account_id,
        job_id::varchar(20) as job_id,
        list_id::varchar(10) as list_id,
        batch_id::varchar(10) as batch_id,
        subscriber_id::varchar(20) as subscriber_id,
        subscriber_key::varchar(100) as subscriber_key,
        event_date::timestamp_ntz(9) as event_date,
        is_unique::varchar(10) as is_unique,
        domain::varchar(50) as domain,
        bounce_category_id::varchar(10) as bounce_category_id,
        bounce_category::varchar(30) as bounce_category,
        bounce_subcategory_id::varchar(10) as bounce_subcategory_id,
        bounce_subcategory::varchar(30) as bounce_subcategory,
        bounce_type_id::varchar(10) as bounce_type_id,
        bounce_type::varchar(30) as bounce_type,
        smtp_bounce_reason::varchar(1000) as smtp_bounce_reason,
        smtp_message::varchar(200) as smtp_message,
        smtp_code::varchar(10) as smtp_code,
        triggerer_send_definition_object_id::varchar(50) as triggerer_send_definition_object_id,
        triggered_send_customer_key::varchar(10) as triggered_send_customer_key,
        email_subject::varchar(200) as email_subject,
        bcc_email::varchar(50) as bcc_email,
        email_name::varchar(100) as email_name,
        email_id::varchar(20) as email_id,
        email_address::varchar(100) as email_address,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where rnk=1
)
select * from final

{% elif var("sfmc_job_to_execute") == 'ph_sfmc_files' %}

final as
(
    select
        'PH'::varchar(10) as cntry_cd,
        oyb_account_id::varchar(20) as oyb_account_id,
        job_id::varchar(20) as job_id,
        list_id::varchar(10) as list_id,
        batch_id::varchar(10) as batch_id,
        subscriber_id::varchar(20) as subscriber_id,
        subscriber_key::varchar(100) as subscriber_key,
        event_date::timestamp_ntz(9) as event_date,
        is_unique::varchar(10) as is_unique,
        domain::varchar(50) as domain,
        bounce_category_id::varchar(10) as bounce_category_id,
        bounce_category::varchar(30) as bounce_category,
        bounce_subcategory_id::varchar(10) as bounce_subcategory_id,
        bounce_subcategory::varchar(30) as bounce_subcategory,
        bounce_type_id::varchar(10) as bounce_type_id,
        bounce_type::varchar(30) as bounce_type,
        smtp_bounce_reason::varchar(1000) as smtp_bounce_reason,
        smtp_message::varchar(200) as smtp_message,
        smtp_code::varchar(10) as smtp_code,
        triggerer_send_definition_object_id::varchar(50) as triggerer_send_definition_object_id,
        triggered_send_customer_key::varchar(10) as triggered_send_customer_key,
        email_subject::varchar(200) as email_subject,
        bcc_email::varchar(50) as bcc_email,
        email_name::varchar(100) as email_name,
        email_id::varchar(20) as email_id,
        email_address::varchar(100) as email_address,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source2
    where rnk=1
)
select * from final

{% elif var("sfmc_job_to_execute") == 'tw_sfmc_files' %}

final as
(
    select
        'TW'::varchar(10) as cntry_cd,
        oyb_account_id::varchar(20) as oyb_account_id,
        job_id::varchar(20) as job_id,
        list_id::varchar(10) as list_id,
        batch_id::varchar(10) as batch_id,
        subscriber_id::varchar(20) as subscriber_id,
        subscriber_key::varchar(100) as subscriber_key,
        event_date::timestamp_ntz(9) as event_date,
        is_unique::varchar(10) as is_unique,
        domain::varchar(50) as domain,
        bounce_category_id::varchar(10) as bounce_category_id,
        bounce_category::varchar(30) as bounce_category,
        bounce_subcategory_id::varchar(10) as bounce_subcategory_id,
        bounce_subcategory::varchar(30) as bounce_subcategory,
        bounce_type_id::varchar(10) as bounce_type_id,
        bounce_type::varchar(30) as bounce_type,
        smtp_bounce_reason::varchar(1000) as smtp_bounce_reason,
        smtp_message::varchar(200) as smtp_message,
        smtp_code::varchar(10) as smtp_code,
        triggerer_send_definition_object_id::varchar(50) as triggerer_send_definition_object_id,
        triggered_send_customer_key::varchar(10) as triggered_send_customer_key,
        email_subject::varchar(200) as email_subject,
        bcc_email::varchar(50) as bcc_email,
        email_name::varchar(100) as email_name,
        email_id::varchar(20) as email_id,
        email_address::varchar(100) as email_address,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source3
    where rnk=1
)
select * from final

{% endif %}