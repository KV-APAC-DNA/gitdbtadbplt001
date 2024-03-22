{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_click_data') }}
),
final as(
    select   
        oyb_account_id as oyb_account_id,
        job_id as job_id,
        list_id as list_id,
        batch_id as batch_id,
        subscriber_id as subscriber_id,
        subscriber_key as subscriber_key,
        event_date as event_date,
        domain as domain,
        url as url,
        link_name as link_name,
        link_content as link_content,
        is_unique as is_unique,
        email_name as email_name,
        TRIM(email_subject) as email_subject,
        file_name as file_name,
        crtd_dttm as crtd_dttm
   from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %}
)
select * from final