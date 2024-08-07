with report_006_a_present_data1 as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.REPORT_006_A_PRESENT_DATA1
),
report_006_a_historical_data as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.REPORT_006_A_HISTORICAL_DATA
),
cld_m as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CLD_M
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
        report_006_a_historical_data.channel_name, 
        report_006_a_historical_data.channel_id, 
        report_006_a_historical_data.yymm, 
        report_006_a_historical_data.total, 
        report_006_a_historical_data.day_of_week, 
        report_006_a_historical_data.report_exec_date 
      FROM 
        report_006_a_historical_data 
      UNION ALL 
      SELECT 
        report_006_a_present_data1.channel_name, 
        report_006_a_present_data1.channel_id, 
        report_006_a_present_data1.yymm, 
        report_006_a_present_data1.total, 
        report_006_a_present_data1.day_of_week, 
        report_006_a_present_data1.report_exec_date 
      FROM 
        report_006_a_present_data1
    ) a 
    LEFT JOIN cld_m b ON (
      (
        (a.report_exec_date):: timestamp without time zone = b.ymd_dt
      )
    )
  )
)
select * from final