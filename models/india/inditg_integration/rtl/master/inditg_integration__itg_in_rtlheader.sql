with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_in_rtlheader') }}
    where filename not in 
    (
        select distinct filename {{ source('indwks_integration', 'TRATBL_sdl_in_rtlheader__null_test') }}
        union all 
        select distinct filename {{ source('indwks_integration', 'TRATBL_sdl_in_rtlheader__duplicate_test') }}

    )
),
final as 
(
    select
        cmpcode::varchar(10) as cmpcode,
        tlcode::varchar(50) as tlcode,
        tlname::varchar(100) as tlname,
        emailid::varchar(50) as emailid,
        phoneno::varchar(15) as phoneno,
        dateofbirth::timestamp_ntz(9) as dateofbirth,
        dateofjoin::timestamp_ntz(9) as dateofjoin,
        isactive::varchar(1) as isactive,
        modusercode::varchar(50) as modusercode,
        moddt::timestamp_ntz(9) as moddt,
        approvalstatus::varchar(10) as approvalstatus,
        dailyallowance::number(22,6) as dailyallowance,
        monthlysalary::number(22,6) as monthlysalary,
        aadharno::varchar(15) as aadharno,
        imagepath::varchar(100) as imagepath,
        createddt::timestamp_ntz(9) as createddt,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final