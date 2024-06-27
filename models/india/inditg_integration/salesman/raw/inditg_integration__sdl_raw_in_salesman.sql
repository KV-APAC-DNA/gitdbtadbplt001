{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_salesman') }} 
),
final as(
    select 
        cmpcode as cmpcode,
        distcode as distcode,
        distrbrcode as distrbrcode,
        smcode as smcode,
        smname as smname,
        smphoneno as smphoneno,
        smemail as smemail,
        rdssmtype as rdssmtype,
        smdailyallowance as smdailyallowance,
        smmonthlysalary as smmonthlysalary,
        smmktcredit as smmktcredit,
        smcreditdays as smcreditdays,
        STATUS as STATUS,
        modusercode as modusercode,
        moddt as moddt,
        aadhaarno as aadhaarno,
        uniquesalescode as uniquesalescode,
        createddt as createddt,
        filename as filename,
        run_id as run_id,
        crt_dttm as crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final