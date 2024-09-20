{% if build_month_end_job_models()  %}
with KR_054_CAL_V as (
select * from {{ ref('jpndcledw_integration__kr_054_cal_v') }}
),
KR_054_SUSE_MEISAI as (
select * from {{ ref('jpndcledw_integration__kr_054_suse_meisai') }}
),
transformed as (
SELECT
       '101_利用P' as use_label,
       USE_YM ::varchar as USE_YM,
       USE_YY ::varchar as USE_YY,
       USE_MM ::varchar as USE_MM,
       V.YYMM ::varchar as POINT_YM,
       SUBSTRING(V.YYMM,1,4) ::varchar as POINT_YY,
       SUBSTRING(V.YYMM,5,2) ::varchar as POINT_MM,
       SUM(NVL(UPOINT,0))*(-1) ::varchar as UPOINT
 FROM
      KR_054_CAL_V V
 LEFT JOIN 
    KR_054_SUSE_MEISAI MEI
  ON
       V.YYMM=MEI.POINT_YM
 GROUP BY
       use_label,
       USE_YM ::varchar,
       USE_YY ::varchar,
       USE_MM ::varchar,
       V.YYMM ::varchar,
       SUBSTRING(V.YYMM,1,4)::varchar,
       SUBSTRING(V.YYMM,5,2) ::varchar
),
final as (
select
use_label::varchar(30) as use_label,
use_ym::varchar(9) as use_ym,
use_yy::varchar(6) as use_yy,
use_mm::varchar(3) as use_mm,
point_ym::varchar(9) as point_ym,
point_yy::varchar(6) as point_yy,
point_mm::varchar(3) as point_mm,
upoint::number(38,0) as upoint 
from transformed
)
select * from final
{% else %}
    select * from {{this}}
{% endif %}