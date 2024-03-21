{#pre_hook="delete from {{this}} where (coalesce(upper(trim(saleunit)),'N/A')) in (select distinct (coalesce(upper(trim(saleunit)),'N/A')) from {{ source('thasdl_raw', 'sdl_th_gt_route') }} ) and AND   UPPER(flag) IN ('I','U');"#}

{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="delete from {{this}} where {{ source('thasdl_raw', 'sdl_th_gt_route') }} ) AND   UPPER(flag) IN ('I','U');"
    )
}}

with source as (
    select  * from {{ ref('thawks_integration__wks_th_gt_route_pre_load') }}
),
final as (
    select 
        cntry_cd::varchar(5) as cntry_cd,
        crncy_cd::varchar(5) as crncy_cd,
        routeid::varchar(50) as routeid,
        name::varchar(100) as name,
        route_description::varchar(100) as route_description,
        isactive::varchar(10) as isactive,
        routesale::varchar(50) as routesale,
        (coalesce(upper(trim(saleunit)),'N/A'))::varchar(50)  as saleunit,
        routecode::varchar(50) as routecode,
        description::varchar(100) as description,
        effective_start_date::date as effective_start_date,
        effective_end_date::date as effective_end_date,
        flag::varchar(5) as flag,
        filename::varchar(100) as filename,
        file_uploaded_date::date as file_uploaded_date,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final
