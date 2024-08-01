{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} itg
                    USING {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_click_data') }} sdl
                    WHERE sdl.JOB_ID = itg.JOB_ID
                        AND sdl.BATCH_ID = itg.BATCH_ID
                        AND sdl.SUBSCRIBER_ID = itg.SUBSCRIBER_ID
                        AND sdl.SUBSCRIBER_KEY = itg.SUBSCRIBER_KEY
                        AND sdl.EVENT_DATE = itg.EVENT_DATE
                        AND sdl.URL = itg.URL
                        AND NVL(sdl.LINK_NAME, 'NA') = NVL(itg.LINK_NAME, 'NA');
                    {% endif %}"
    )
}}

with sdl_hcp360_in_sfmc_click_data as 
(
        select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_click_data') }}
),
final as
(
    select 'IN',
        *,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    from sdl_hcp360_in_sfmc_click_data
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
	domain::varchar(50) as domain,
	url::varchar(1000) as url,
	link_name::varchar(200) as link_name,
	link_content::varchar(1000) as link_content,
	is_unique::varchar(10) as is_unique,
	email_name::varchar(100) as email_name,
	email_subject::varchar(200) as email_subject,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
from final
