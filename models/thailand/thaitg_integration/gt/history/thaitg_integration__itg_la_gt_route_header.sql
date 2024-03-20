--pre_hook="DELETE FROM {{this}} WHERE (COALESCE(UPPER(TRIM(saleunit)),'N/A')) IN (SELECT DISTINCT COALESCE(UPPER(TRIM(saleunit)),'N/A')
                                                  --FROM {{ source('thasdl_raw', 'sdl_la_gt_route_header') }}) AND   UPPER(flag) IN ('I','U');"
    

{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="DELETE FROM {{this}} WHERE (COALESCE(UPPER(TRIM(saleunit)),'N/A')) IN (SELECT DISTINCT COALESCE(UPPER(TRIM(saleunit)),'N/A') FROM dev_dna_load.snaposesdl_raw.sdl_la_gt_route_header) AND   UPPER(flag) IN ('I','U');"
    )
}}

with wks_la_gt_route_header_pre_load as (
    select * from {{ ref('thawks_integration__wks_la_gt_route_header_pre_load') }}
),
final as (
    select 
    route_id::varchar(50) as route_id,
    route_name::varchar(100) as route_name,
    route_desc::varchar(200) as route_desc,
    is_active::varchar(5) as is_active,
    routesale::varchar(100) as routesale,
    saleunit::varchar(100) as saleunit,
    route_code::varchar(50) as route_code,
    description::varchar(100) as description,
    effective_start_date::date as effective_start_date,
    effective_end_date::date as effective_end_date,
    flag::varchar(5) as flag,
    file_upload_date::date as file_upload_date,
    filename::varchar(50) as filename,
    run_id::varchar(14) as run_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from wks_la_gt_route_header_pre_load
)
select * from final 