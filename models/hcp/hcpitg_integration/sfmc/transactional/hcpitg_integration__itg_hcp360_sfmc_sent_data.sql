{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}} as ITG_HCP360_SFMC_SENT_DATA USING {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_sent_data') }} as SDL_HCP360_IN_SFMC_SENT_DATA
        WHERE SDL_HCP360_IN_SFMC_SENT_DATA.JOB_ID = ITG_HCP360_SFMC_SENT_DATA.JOB_ID
        AND SDL_HCP360_IN_SFMC_SENT_DATA.BATCH_ID = ITG_HCP360_SFMC_SENT_DATA.BATCH_ID
        AND SDL_HCP360_IN_SFMC_SENT_DATA.SUBSCRIBER_ID = ITG_HCP360_SFMC_SENT_DATA.SUBSCRIBER_ID
        AND SDL_HCP360_IN_SFMC_SENT_DATA.SUBSCRIBER_KEY = ITG_HCP360_SFMC_SENT_DATA.SUBSCRIBER_KEY
        AND SDL_HCP360_IN_SFMC_SENT_DATA.EVENT_DATE = ITG_HCP360_SFMC_SENT_DATA.EVENT_DATE
        AND SDL_HCP360_IN_SFMC_SENT_DATA.EMAIL_ID = ITG_HCP360_SFMC_SENT_DATA.EMAIL_ID;
        {% endif %}"
    )
}}
WITH sdl_hcp360_in_sfmc_sent_data AS (
    select *, dense_rank() over(partition by JOB_ID, BATCH_ID, SUBSCRIBER_ID, SUBSCRIBER_KEY, EVENT_DATE, EMAIL_ID order by file_name desc) as rnk 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_sent_data') }}
    qualify rnk=1
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
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name
    from sdl_hcp360_in_sfmc_sent_data
)
select * from final