{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_sfmc_bounce_data') }}
),
final as
(
    select 
        oyb_account_id as oyb_account_id,
        job_id as job_id,
        list_id as list_id,
        batch_id as batch_id,
        subscriber_id as subscriber_id,
        subscriber_key as subscriber_key,
        event_date as event_date,
        is_unique as is_unique,
        domain as domain,
        bounce_category_id as bounce_category_id,
        bounce_category as bounce_category,
        bounce_subcategory_id as bounce_subcategory_id,
        bounce_subcategory as bounce_subcategory,
        bounce_type_id as bounce_type_id,
        bounce_type as bounce_type,
        smtp_bounce_reason as smtp_bounce_reason,
        smtp_message as smtp_message,
        smtp_code as smtp_code,
        triggerer_send_definition_object_id as triggerer_send_definition_object_id,
        triggered_send_customer_key as triggered_send_customer_key,
        email_subject as email_subject,
        bcc_email as bcc_email,
        email_name as email_name,
        email_id as email_id,
        email_address as email_address,
        file_name as file_name,
        crt_dttm as crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final

