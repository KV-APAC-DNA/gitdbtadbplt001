{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_surveycustomers') }}
    where filename not in (
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_tbl_surveycustomers__null_test')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_tbl_surveycustomers__duplicate_test')}}
    )
),
final as(
    select 
        custcode::varchar(10) as custcode,
        slsperid::varchar(10) as slsperid,
        branchcode::varchar(30) as branchcode,
        iseid::varchar(100) as iseid,
        visitdate::timestamp_ntz(9) as visitdate,
        createddate::timestamp_ntz(9) as createddate,
        storeprioritization::varchar(100) as storeprioritization,
        filename::varchar(50) as filename,
        filename_dt::number(14,0) as filename_dt,
        run_id::number(14,0) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
     {% if is_incremental() %}
    -- -- this filter will only be applied on an incremental run
     where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
     {% endif %}
)
select * from final