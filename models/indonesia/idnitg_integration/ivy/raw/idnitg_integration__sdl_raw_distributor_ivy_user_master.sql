{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw','sdl_distributor_ivy_user_master') }}
),
final as (
    select 
    md_location,
    md_code,
    md_name,
    sd_location,
    sd_code,
    sd_name,
    rbdm_location,
    rbdm_code,
    rbdm_name,
    bdm_location,
    bdm_code,
    bdm_name,
    bdr_location,
    bdr_code,
    bdr_name,
    dis_location,
    dis_code,
    dis_name,
    rsm_code,
    rsm_name,
    sup_code,
    sup_name,
    sr_code,
    sr_name,
    cdl_dttm,
    null as run_id,
    source_file_name
    from source
)

select * from final