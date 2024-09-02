{{
    config
    (
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["salinvdate"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}
with wks_lks_businesscalender as 
(
    select * from {{ ref('indwks_integration__wks_lks_businesscalender') }}
),
final as
(
    SELECT salinvdate::timestamp_ntz(9) AS salinvdate,
      month::VARCHAR(30) AS month,
      year::number(18, 0) AS year,
      week::VARCHAR(50) AS week,
      monthkey::number(18, 0) AS monthkey,
      current_timestamp()::timestamp_ntz(9) AS crt_dttm,
      current_timestamp()::timestamp_ntz(9) AS updt_dttm
      
    FROM wks_lks_businesscalender
)
select * from final
