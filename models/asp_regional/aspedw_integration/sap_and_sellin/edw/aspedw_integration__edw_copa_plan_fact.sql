{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
    )
}}


with source as(
    select * from {{ ref('aspwks_integration__wks_edw_copa_plan_fact') }}
),
final as(
SELECT
    A.*,
    current_timestamp()::timestamp_ntz(9) AS crt_dttm,
    current_timestamp()::timestamp_ntz(9) AS upd_dttm
  FROM source AS A
)
select * from final