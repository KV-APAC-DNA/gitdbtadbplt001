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
    plant :: varchar(4) as plnt,
    country :: varchar(3) as ctry,
    logsys :: varchar(10) as src_sys,
    purch_org :: varchar(4) as prchsng_org,
    region :: varchar(3) as rgn,
    comp_code :: varchar(4) as co_cd,
    factcal_id :: varchar(2) as fctry_cal,
    zmarclust :: varchar(2) as mkt_clus,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final