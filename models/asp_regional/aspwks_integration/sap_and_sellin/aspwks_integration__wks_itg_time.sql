{{
    config(
        alias="wks_itg_time",
        tags=["daily"]
    )
}}

with 

source as (

    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_time') }}
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
    -- current_timestamp() as tgt_crt_dttm,
    updt_dttm as updt_dttm
  from source
)

select * from final