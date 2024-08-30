{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} itg
                    USING {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_bounce_data') }} sdl
                    WHERE sdl.JOB_ID = itg.JOB_ID
                        AND sdl.BATCH_ID = itg.BATCH_ID
                        AND sdl.SUBSCRIBER_ID = itg.SUBSCRIBER_ID
                        AND sdl.SUBSCRIBER_KEY = itg.SUBSCRIBER_KEY
                        AND sdl.EVENT_DATE = itg.EVENT_DATE
                        AND sdl.EMAIL_ID = itg.EMAIL_ID
						AND sdl.file_name not in (
						select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_sfmc_bounce_data__null_test') }}
						union all
						select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_sfmc_bounce_data__duplicate_test') }}
						);
                    {% endif %}"
    )
}}

with sdl_hcp360_in_sfmc_bounce_data as 
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_bounce_data') }}
	where file_name not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_sfmc_bounce_data__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_sfmc_bounce_data__duplicate_test') }}
	)
),
final as 
(
    select 'IN',
        *,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    from sdl_hcp360_in_sfmc_bounce_data
)
select
	'IN' as cntry_cd,
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
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm,
	file_name as file_name
from final
