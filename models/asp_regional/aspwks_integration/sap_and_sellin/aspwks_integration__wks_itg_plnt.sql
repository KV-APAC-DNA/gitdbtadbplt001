{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_plant_attr') }}
),

final as (
    select
        plant,
        country,
        logsys,
        purch_org,
        region,
        comp_code,
        factcal_id,
        zmarclust,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final
