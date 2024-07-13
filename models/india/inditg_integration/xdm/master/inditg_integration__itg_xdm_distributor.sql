with source as 
(
  select * from {{ source('indsdl_raw', 'sdl_xdm_distributor') }}
),
final as 
(
  select 
        cmpcode::varchar(10) as cmpcode,
        usercode::varchar(50) as usercode,
        distrname::varchar(150) as distrname,
        distrbrcode::varchar(30) as distrbrcode,
        distrbrtype::varchar(2) as distrbrtype,
        distrbrname::varchar(100) as distrbrname,
        distrbraddr1::varchar(200) as distrbraddr1,
        distrbraddr2::varchar(200) as distrbraddr2,
        distrbraddr3::varchar(200) as distrbraddr3,
        city::varchar(50) as city,
        distrbrstate::varchar(50) as distrbrstate,
        postalcode::number(18,0) as postalcode,
        country::varchar(50) as country,
        contactperson::varchar(100) as contactperson,
        phone::varchar(15) as phone,
        emailid::varchar(50) as emailid,
        isactive::varchar(1) as isactive,
        gstinnumber::varchar(15) as gstinnumber,
        gstdistrtype::varchar(1) as gstdistrtype,
        gststatecode::varchar(3) as gststatecode,
        others1::varchar(15) as others1,
        isdirectacct::varchar(1) as isdirectacct,
        typecode::varchar(50) as typecode,
        psnonps::varchar(1) as psnonps,
        createddt::timestamp_ntz(9) as createddt,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        current_timestamp()::timestamp_ntz(9) AS crt_dttm,
        current_timestamp()::timestamp_ntz(9) AS updt_dttm
    from source
)
select * from final