{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
            delete from {{this}} where subscriberkey in (select subscriberkey from {{ source('jpdclsdl_raw', 'sfmc_clicks') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'sfmc_clicks') }}
),
final as(
    select 
        clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        subscriberkey::varchar(300) as subscriberkey,
        emailaddress::varchar(300) as emailaddress,
        subscriberid::number(38,0) as subscriberid,
        listid::number(38,0) as listid,
        eventdate::timestamp_ntz(9) as eventdate,
        eventtype::varchar(20) as eventtype,
        sendurlid::number(38,0) as sendurlid,
        urlid::number(38,0) as urlid,
        url::varchar(65535) as url,
        alias::varchar(65535) as alias,
        batchid::number(38,0) as batchid,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by,
        file_name::varchar(255) as file_name
    from source
)
select * from final