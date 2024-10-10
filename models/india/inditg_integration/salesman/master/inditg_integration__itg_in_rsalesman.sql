{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where case  when ( select count(*) from {{ source('indsdl_raw', 'sdl_in_salesman') }} ) > 0 then 1 else 0 end = 1;
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_in_salesman') }}
    where filename not in (
        select distinct file_name from {{source('indwks_integration','TRATBL_sdl_in_salesman__null_test')}}
        union all
        select distinct file_name from {{source('indwks_integration','TRATBL_sdl_in_salesman__duplicate_test')}})
),
final as 
(
    select
        cmpcode::varchar(10) as cmpcode,
        distcode::varchar(50) as distcode,
        distrbrcode::varchar(30) as distrbrcode,
        smcode::varchar(50) as smcode,
        smname::varchar(50) as smname,
        smphoneno::varchar(50) as smphoneno,
        smemail::varchar(50) as smemail,
        rdssmtype::varchar(10) as rdssmtype,
        smdailyallowance::number(38,6) as smdailyallowance,
        smmonthlysalary::number(38,6) as smmonthlysalary,
        smmktcredit::number(38,6) as smmktcredit,
        smcreditdays::number(18,0) as smcreditdays,
        STATUS::varchar(10) as STATUS,
        modusercode::varchar(50) as modusercode,
        moddt::timestamp_ntz(9) as moddt,
        aadhaarno::varchar(50) as aadhaarno,
        uniquesalescode::varchar(50) as uniquesalescode,
        createddt::timestamp_ntz(9) as createddt,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final