with source as (
    select * from {{ ref('aspitg_integration__itg_sfmc_sent_data') }}
),
final as (
    select
        cntry_cd as "cntry_cd", 
        oyb_account_id as "oyb_account_id", 
        job_id as "job_id", 
        list_id as "list_id", 
        batch_id as "batch_id", 
        subscriber_id as "subscriber_id", 
        subscriber_key as "subscriber_key", 
        event_date as "event_date", 
        domain as "domain", 
        triggerer_send_definition_object_id as "triggerer_send_definition_object_id", 
        triggered_send_customer_key as "triggered_send_customer_key", 
        email_name as "email_name", 
        email_subject as "email_subject", 
        email_id as "email_id", 
        file_name as "file_name", 
        crtd_dttm as "crtd_dttm", 
        updt_dttm as "updt_dttm"
    from source
)

select * from final