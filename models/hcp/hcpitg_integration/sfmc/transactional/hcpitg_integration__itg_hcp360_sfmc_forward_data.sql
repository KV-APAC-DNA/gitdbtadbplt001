{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} as itg_hcp360_sfmc_forward_data using {{ source('hcpsdl_raw','sdl_hcp360_in_sfmc_forward_data') }} as sdl_hcp360_in_sfmc_forward_data where sdl_hcp360_in_sfmc_forward_data.job_id = itg_hcp360_sfmc_forward_data.job_id
        and sdl_hcp360_in_sfmc_forward_data.batch_id = itg_hcp360_sfmc_forward_data.batch_id
        and sdl_hcp360_in_sfmc_forward_data.subscriber_id = itg_hcp360_sfmc_forward_data.subscriber_id
        and sdl_hcp360_in_sfmc_forward_data.subscriber_key = itg_hcp360_sfmc_forward_data.subscriber_key
        and sdl_hcp360_in_sfmc_forward_data.event_date = itg_hcp360_sfmc_forward_data.event_date
        and sdl_hcp360_in_sfmc_forward_data.email_id = itg_hcp360_sfmc_forward_data.email_id;
        {% endif %}"
    )
}}
with sdl_hcp360_in_sfmc_forward_data as (
    select * from {{ source('hcpsdl_raw','sdl_hcp360_in_sfmc_forward_data') }}
),
final as 
(
    select
    'IN'::varchar(10) as cntry_cd,
	oyb_account_id::varchar(20) as oyb_account_id,
	job_id::varchar(20) as job_id,
	list_id::varchar(10) as list_id,
	batch_id::varchar(10) as batch_id,
	subscriber_id::varchar(20) as subscriber_id,
	subscriber_key::varchar(100) as subscriber_key,
	event_date::timestamp_ntz(9) as event_date,
	domain::varchar(50) as domain,
	triggerer_send_definition_object_id::varchar(50) as triggerer_send_definition_object_id,
	triggered_send_customer_key::varchar(10) as triggered_send_customer_key,
	email_name::varchar(100) as email_name,
	email_subject::varchar(200) as email_subject,
	email_id::varchar(20) as email_id,
	transactiontime::timestamp_ntz(9) as transactiontime,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name
    from sdl_hcp360_in_sfmc_forward_data
)
select * from final