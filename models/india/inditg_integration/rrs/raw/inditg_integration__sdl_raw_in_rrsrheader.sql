{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_rrsrheader') }}
    where filename not in (
    select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_in_rrsrheader__null_test') }}
    union all
    select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_in_rrsrheader__duplicate_test') }}
)
),
final as(
    select 
        cmpcode as cmpcode,
        rsrcode as rsrcode,
        rsrname as rsrname,
        emailid as emailid,
        phoneno as phoneno,
        dateofbirth as dateofbirth,
        dateofjoin as dateofjoin,
        isactive as isactive,
        modusercode as modusercode,
        moddt as moddt,
        approvalstatus as approvalstatus,
        dailyallowance as dailyallowance,
        monthlysalary as monthlysalary,
        aadharno as aadharno,
        imagepath as imagepath,
        createddt as createddt,
        filename as filename,
        run_id as run_id,
        crt_dttm as crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final