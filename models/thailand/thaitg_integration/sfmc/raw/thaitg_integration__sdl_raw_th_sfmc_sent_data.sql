{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_sent_data') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_sent_data__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_sent_data__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_sent_data__lookup_test') }}
    )

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
        triggerer_send_definition_object_id as triggerer_send_definition_object_id,
        triggered_send_customer_key as triggered_send_customer_key,
        email_name as email_name,
        TRIM(email_subject) as email_subject,
        email_id as email_id,
        file_name as file_name,
        crtd_dttm as crtd_dttm
   from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %}
)
select * from final