{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["sls_org","dstn_chnl","mat_sls","lang_key"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_matl_sls_text') }}
),

final as (
    select
        salesorg :: varchar(4) as sls_org,
        distr_chan :: varchar(2) as dstn_chnl,
        mat_sales :: varchar(18) as mat_sls,
        langu :: varchar(1) as lang_key,
        txtmd :: varchar(50) as med_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final