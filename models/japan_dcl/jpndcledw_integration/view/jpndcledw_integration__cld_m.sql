{{
    config(
        post_hook='{{update_kr_comm_point_para()}}'
    )
}}
WITH cld_m
AS (
  SELECT *
  FROM {{ source('jpdcledw_integration', 'cld_m') }}
  ),
final
AS (
  SELECT cld_m.ymd_dt,
    cld_m.year,
    cld_m.year_445,
    cld_m.year_15,
    cld_m.year_20,
    cld_m.half,
    cld_m.half_nm,
    cld_m.half_445,
    cld_m.half_445_nm,
    cld_m.half_15,
    cld_m.half_15_nm,
    cld_m.half_20,
    cld_m.half_20_nm,
    cld_m.quarter,
    cld_m.quarter_nm,
    cld_m.quarter_445,
    cld_m.quarter_445_nm,
    cld_m.quarter_15,
    cld_m.quarter_15_nm,
    cld_m.quarter_20,
    cld_m.quarter_20_nm,
    cld_m.month,
    cld_m.month_nm,
    cld_m.month_445,
    cld_m.month_445_nm,
    cld_m.month_15,
    cld_m.month_15_nm,
    cld_m.month_20,
    cld_m.month_20_nm,
    cld_m.ymonth_445,
    cld_m.ymonth_15,
    cld_m.ymonth_20,
    cld_m.week_ms,
    cld_m.week_ss,
    cld_m.mweek_445,
    cld_m.mweek_15ms,
    cld_m.mweek_15ss,
    cld_m.mweek_20,
    cld_m.mweek_445_iso,
    cld_m.mweek_15ms_iso,
    cld_m.mweek_15ss_iso,
    cld_m.mweek_20_iso,
    cld_m.day,
    cld_m.day_of_week,
    cld_m.opr_flg,
    cld_m.sls_flg
  FROM cld_m
  )
SELECT *
FROM final
