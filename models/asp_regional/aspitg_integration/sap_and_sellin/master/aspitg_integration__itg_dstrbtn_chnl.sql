{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["distr_chan","langu"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_dstrbtn_chnl') }}
),

--Logical CTE

--Final CTE
final as (
    select
        distr_chan,
        langu,
        txtsh,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final