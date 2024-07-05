{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
            delete from {{this}} where clientid in (select clientid from {{ source('jpndclsdl_raw', 'sfmc_sendjobs') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpndclsdl_raw', 'sfmc_sendjobs') }}
),
final as(
    select 
        clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        fromname::varchar(150) as fromname,
        fromemail::varchar(100) as fromemail,
        schedtime::timestamp_ntz(9) as schedtime,
        senttime::timestamp_ntz(9) as senttime,
        subject::varchar(300) as subject,
        emailname::varchar(100) as emailname,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        senddefinitionexternalkey::varchar(100) as senddefinitionexternalkey,
        jobstatus::varchar(30) as jobstatus,
        previewurl::varchar(65535) as previewurl,
        ismultipart::varchar(65535) as ismultipart,
        additional::varchar(50) as additional,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final