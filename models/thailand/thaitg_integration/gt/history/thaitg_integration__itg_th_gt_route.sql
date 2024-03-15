{{
    config(
        materialized="incremental",
        unique_key='saleunit',
        incremental_strategy='append',
        pre_hook="delete from {{this}} where (coalesce(upper(trim(saleunit)),'N/A')) in (select (coalesce(upper(trim(saleunit)),'N/A')) from {{ ref('thawks_integration__wks_th_gt_route_pre_load') }});"
    )
}}

with source as (
    select  * from {{ ref('thawks_integration__wks_th_gt_route_pre_load') }}
),
final as (
    select 
        cntry_cd,
        crncy_cd,
        routeid,
        name,
        route_description,
        isactive,
        routesale,
        (coalesce(upper(trim(saleunit)),'N/A')) as saleunit,
        routecode,
        description,
        effective_start_date,
        effective_end_date,
        flag,
        filename,
        file_uploaded_date,
        run_id,
        crt_dttm
    from source
)
select * from final