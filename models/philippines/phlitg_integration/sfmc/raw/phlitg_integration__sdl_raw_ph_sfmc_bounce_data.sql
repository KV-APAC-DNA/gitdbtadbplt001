{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_sfmc_bounce_data') }}
),

final as
(
    select 
        oyb_account_id,
        job_id,
        list_id,
        batch_id,
        subscriber_id,
        subscriber_key,
        event_date,
        is_unique,
        domain,
        bounce_category_id,
        bounce_category,
        bounce_subcategory_id,
        bounce_subcategory,
        bounce_type_id,
        bounce_type,
        smtp_bounce_reason,
        smtp_message,
        smtp_code,
        triggerer_send_definition_object_id,
        triggered_send_customer_key,
        email_subject,
        bcc_email,
        email_name,
        email_id,
        email_address,
        crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
{% endif %} 
)

select * from final