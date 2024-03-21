
{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="delete from {{this}} where (coalesce(upper(trim(saleunit)),'N/A')) in (select distinct (coalesce(upper(trim(saleunit)),'N/A')) from {{ source('thasdl_raw', 'sdl_th_gt_route_detail') }}  ) AND   UPPER(flag) IN ('I','U');"
    )
}}

with source as (
    select  * from {{ ref('thawks_integration__wks_th_gt_route_detail_pre_load') }}
),
final as (
    SELECT 
        cntry_cd::varchar(5) as cntry_cd,
        crncy_cd::varchar(5) as crncy_cd,
        routeid::varchar(50) as routeid,
        customerid::varchar(50) as customerid,
        routeno::varchar(50) as routeno,
        saleunit::varchar(50) as saleunit,
        ship_to::varchar(50) as ship_to,
        contact_person::varchar(100) as contact_person,
        created_date::date as created_date,
        last_modified_date::date as last_modified_date,
        flag::varchar(5) as flag,
        filename::varchar(100) as filename,
        file_uploaded_date::date as file_uploaded_date,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)

select * from final