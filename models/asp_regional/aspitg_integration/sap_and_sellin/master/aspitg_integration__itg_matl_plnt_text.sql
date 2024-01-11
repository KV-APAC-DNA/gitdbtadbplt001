{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["plnt","plnt_mat","lang_key"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_matl_plnt_text') }}
),

final as (
    select
        plant :: varchar(4) as plnt,
        mat_plant :: varchar(18) as plnt_mat,
        langu :: varchar(1) as lang_key,
        txtmd :: varchar(150) as med_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final