{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["customercode"],
        merge_exclude_columns= ["crt_dttm", "updt_dttm"]
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_rkeyacccustomer') }}
),
final as 
(
    select
        src.customercode::varchar(50) as customercode,
        customername::varchar(50) as customername,
        customeraddress1::varchar(250) as customeraddress1,
        customeraddress2::varchar(250) as customeraddress2,
        customeraddress3::varchar(250) as customeraddress3,
        sapid::varchar(50) as sapid,
        regioncode::varchar(50) as regioncode,
        zonecode::varchar(50) as zonecode,
        territorycode::varchar(50) as territorycode,
        statecode::varchar(50) as statecode,
        towncode::varchar(50) as towncode,
        emailid::varchar(50) as emailid,
        mobilell::varchar(50) as mobilell,
        isactive::varchar(1) as isactive,
        wholesalercode::varchar(50) as wholesalercode,
        urc::varchar(50) as urc,
        nkacstores::varchar(1) as nkacstores,
        parentcustomercode::varchar(50) as parentcustomercode,
        isdirectacct::varchar(1) as isdirectacct,
        isparent::varchar(1) as isparent,
        abicode::varchar(50) as abicode,
        distributorsapid::varchar(50) as distributorsapid,
        isconfirm::varchar(1) as isconfirm,
        createddate::timestamp_ntz(9) as createddate,
        createdusercode::varchar(50) as createdusercode,
        moddt::timestamp_ntz(9) as moddt,
        modusercode::varchar(50) as modusercode,
        createddt::timestamp_ntz(9) as createddt,
        convert_timezone('UTC', current_timestamp()) AS crt_dttm,
        convert_timezone('UTC', current_timestamp()) AS updt_dttm
	FROM
	(Select distinct * from source) src
)
select * from final


