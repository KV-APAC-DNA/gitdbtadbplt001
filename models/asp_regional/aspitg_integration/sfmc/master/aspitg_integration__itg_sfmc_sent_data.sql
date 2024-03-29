{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["event_date"],
        pre_hook= "delete from {{this}} where event_date >= (select min(event_date) from {{ source('thasdl_raw','sdl_th_sfmc_sent_data') }}) and cntry_cd = 'TH'"
    )
}}


with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_sent_data') }}
),
final as(
    select
        'TH'::varchar(10) as cntry_cd,
        oyb_account_id::varchar(50) as oyb_account_id,
        job_id::varchar(50) as job_id,
        list_id::varchar(30) as list_id,
        batch_id::varchar(30) as batch_id,
        subscriber_id::varchar(50) as subscriber_id,
        subscriber_key::varchar(100) as subscriber_key,
        event_date::timestamp_ntz(9) as event_date,
        domain::varchar(50) as domain,
        triggerer_send_definition_object_id::varchar(50) as triggerer_send_definition_object_id,
        triggered_send_customer_key::varchar(10) as triggered_send_customer_key,
        email_name::varchar(100) as email_name,
        trim(email_subject)::varchar(200) as email_subject,
        email_id::varchar(30) as email_id,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm 
    from source
)
select * from final