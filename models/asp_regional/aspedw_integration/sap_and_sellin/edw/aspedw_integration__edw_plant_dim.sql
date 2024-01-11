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
        plnt :: varchar(4) as plnt,
        plnt_nm :: varchar(40) as plnt_nm,
        ctry :: varchar(3) as ctry_key,
        src_sys :: varchar(10) as src_sys,
        prchsng_org :: varchar(4) as prchsng_org,
        rgn :: varchar(3) as rgn,
        co_cd :: varchar(4) as co_cd,
        fctry_cal :: varchar(2) as fctry_cal,
        mkt_clus :: varchar(2) as mkt_clus,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final