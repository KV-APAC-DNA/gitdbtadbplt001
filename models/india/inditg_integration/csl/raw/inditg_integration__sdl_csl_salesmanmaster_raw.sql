{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    select * from {{source('indsdl_raw', 'sdl_csl_salesmanmaster')}}
),
final as
(
    select 
        distcode::varchar(100) as distcode,
        smid::number(18,0) as smid,
        smcode::varchar(100) as smcode,
        smname::varchar(100) as smname,
        smphoneno::varchar(100) as smphoneno,
        smemail::varchar(100) as smemail,
        smotherdetails::varchar(500) as smotherdetails,
        smdailyallowance::number(38,6) as smdailyallowance,
        smmonthlysalary::number(38,6) as smmonthlysalary,
        smmktcredit::number(38,6) as smmktcredit,
        smcreditdays::number(18,0) as smcreditdays,
        status::varchar(20) as status,
        rmid::number(18,0) as rmid,
        rmcode::varchar(100) as rmcode,
        rmname::varchar(100) as rmname,
        uploadflag::varchar(1) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        rdssmtype::varchar(100) as rdssmtype,
        uniquesalescode::varchar(15) as uniquesalescode,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        file_name::varchar(50) as file_name
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %})
select * from final
