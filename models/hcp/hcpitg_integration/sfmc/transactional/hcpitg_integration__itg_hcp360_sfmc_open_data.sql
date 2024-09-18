{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} itg
                    USING {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_open_data') }} sdl
                    WHERE sdl.JOB_ID = itg.JOB_ID
                        AND sdl.BATCH_ID = itg.BATCH_ID
                        AND sdl.SUBSCRIBER_ID = itg.SUBSCRIBER_ID
                        AND sdl.SUBSCRIBER_KEY = itg.SUBSCRIBER_KEY
                        AND sdl.EVENT_DATE = itg.EVENT_DATE
                        AND sdl.EMAIL_ID = itg.EMAIL_ID
                        and sdl.file_name not in (
                        select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_sfmc_open_data__null_test') }}
                        union all
                        select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_sfmc_open_data__duplicate_test') }}
                        );
                    {% endif %}"
    )
}}

with sdl_hcp360_in_sfmc_open_data as 
(
    select *, dense_rank() over(partition by JOB_ID, BATCH_ID, SUBSCRIBER_ID, SUBSCRIBER_KEY, EVENT_DATE, EMAIL_ID order by file_name desc) as rnk 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_sfmc_open_data') }}
    where file_name not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_sfmc_open_data__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_sfmc_open_data__duplicate_test') }}
    ) qualify rnk =1
),
final as
(
    select 'IN',
        *,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    from sdl_hcp360_in_sfmc_open_data
)
select
    'IN' as cntry_cd,
    oyb_account_id::varchar(20) as oyb_account_id,
    job_id::varchar(20) as job_id,
    list_id::varchar(10) as list_id,
    batch_id::varchar(10) as batch_id,
    subscriber_id::varchar(20) as subscriber_id,
    subscriber_key::varchar(100) as subscriber_key,
    email_name::varchar(100) as email_name,
    email_subject::varchar(200) as email_subject,
    bcc_email::varchar(50) as bcc_email,
    email_id::varchar(20) as email_id,
    event_date::timestamp_ntz(9) as event_date,
    domain::varchar(50) as domain,
    is_unique::varchar(10) as is_unique,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name
from final
