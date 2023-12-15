{{
    config(
        materialized="view",
        alias="stg_sdl_sap_bw_time",
        tags=["daily","SAP_BW"]
    )
}}

with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_time') }}

),

final as (

    select
        calday,
        fiscvarnt,
        weekday1,
        calweek,
        calmonth,
        calmonth2,
        calquart1,
        calquarter,
        halfyear1,
        calyear,
        fiscper,
        fiscper3,
        fiscyear,
        recordmode,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
