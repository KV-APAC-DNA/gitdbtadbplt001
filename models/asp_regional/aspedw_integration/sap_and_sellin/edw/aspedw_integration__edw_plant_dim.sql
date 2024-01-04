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

    select * from {{ ref('aspwks_integration__wks_edw_plant_dim') }}
),

final as (
    select
        plnt,
        plnt_nm,
        ctry as ctry_key,
        src_sys,
        prchsng_org,
        rgn,
        co_cd,
        fctry_cal,
        mkt_clus,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final