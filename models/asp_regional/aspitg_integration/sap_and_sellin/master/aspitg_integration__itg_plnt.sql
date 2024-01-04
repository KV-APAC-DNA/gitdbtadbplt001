{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["plnt"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_plnt') }}
),

final as (
    select
    plant as plnt,
    country as ctry,
    logsys as src_sys,
    purch_org as prchsng_org,
    region as rgn,
    comp_code as co_cd,
    factcal_id as fctry_cal,
    zmarclust as mkt_clus,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final