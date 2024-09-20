with report_006_a_present_data as (
select * from {{ ref('jpndcledw_integration__report_006_a_present_data1') }}
),
report_006_a_historical_data2 as (
select * from {{ source('jpdcledw_integration', 'report_006_a_historical_data2') }}
),
cld_m as (
select * from {{ ref('jpndcledw_integration__cld_m') }}
),
final as (
SELECT 
  a.channel_name, 
  a.channel_id, 
  a.yymm, 
  a.total, 
  a.day_of_week, 
  a.report_exec_date, 
  b.year_445, 
  b.month_445, 
  b.month_445_nm 
FROM 
  (
    (
      SELECT 
        report_006_a_historical_data2.channel_name, 
        report_006_a_historical_data2.channel_id, 
        report_006_a_historical_data2.yymm, 
        report_006_a_historical_data2.total, 
        report_006_a_historical_data2.day_of_week, 
        report_006_a_historical_data2.report_exec_date 
      FROM 
        report_006_a_historical_data2 
      UNION ALL 
      SELECT 
        report_006_a_present_data.channel_name, 
        report_006_a_present_data.channel_id, 
        report_006_a_present_data.yymm, 
        report_006_a_present_data.total, 
        report_006_a_present_data.day_of_week, 
        report_006_a_present_data.report_exec_date 
      FROM 
        report_006_a_present_data
    ) a 
    LEFT JOIN cld_m b ON (
      (
        (a.report_exec_date):: timestamp without time zone = b.ymd_dt
      )
    )
  )
)
select * from final