{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['subscriberkey']
    )
}}


with source as
(
    select * from {{ source('jpdclsdl_raw', 'sfmc_unsubs') }}
),

final as
(
    select 
		clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        subscriberkey::varchar(300) as subscriberkey,
        emailaddress::varchar(300) as emailaddress,
        subscriberid::number(38,0) as subscriberid,
        listid::number(38,0) as listid,
        eventdate::timestamp_ntz(9) as eventdate,
        eventtype::varchar(20) as eventtype,
        batchid::number(38,0) as batchid,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        null::varchar(30) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from source
)

select * from final