with cld_m as (
    select * from {{ ref('jpndcledw_integration__cld_m') }}
),

final as (
select ymd_dt as "ymd_dt",
    year as "year",
    year_445 as "year_445",
    year_15 as "year_15",
    year_20 as "year_20",
    half as "half",
    half_nm as "half_nm",
    half_445 as "half_445",
    half_445_nm as "half_445_nm",
    half_15 as "half_15",
    half_15_nm as "half_15_nm",
    half_20 as "half_20",
    half_20_nm as "half_20_nm",
    quarter as "quarter",
    quarter_nm as "quarter_nm",
    quarter_445 as "quarter_445",
    quarter_445_nm as "quarter_445_nm",
    quarter_15 as "quarter_15",
    quarter_15_nm as "quarter_15_nm",
    quarter_20 as "quarter_20",
    quarter_20_nm as "quarter_20_nm",
    month as "month",
    month_nm as "month_nm",
    month_445 as "month_445",
    month_445_nm as "month_445_nm",
    month_15 as "month_15",
    month_15_nm as "month_15_nm",
    month_20 as "month_20",
    month_20_nm as "month_20_nm",
    ymonth_445 as "ymonth_445",
    ymonth_15 as "ymonth_15",
    ymonth_20 as "ymonth_20",
    week_ms as "week_ms",
    week_ss as "week_ss",
    mweek_445 as "mweek_445",
    mweek_15ms as "mweek_15ms",
    mweek_15ss as "mweek_15ss",
    mweek_20 as "mweek_20",
    mweek_445_iso as "mweek_445_iso",
    mweek_15ms_iso as "mweek_15ms_iso",
    mweek_15ss_iso as "mweek_15ss_iso",
    mweek_20_iso as "mweek_20_iso",
    day as "day",
    day_of_week as "day_of_week",
    opr_flg as "opr_flg",
    sls_flg as "sls_flg"
from cld_m
)

select * from final