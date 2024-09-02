with KR_054_SSOUTEI_MEISAI as (
select * from {{ ref('jpndcledw_integration__kr_054_ssoutei_meisai') }}
),
transformed as (SELECT
       '02_付与想定P' as fuyo_label,
       POINT_YM ::varchar as POINT_YM ,
       POINT_YY ::varchar as POINT_YY ,
       POINT_MM ::varchar as POINT_MM,
       SUM(NVL(FUYOYOTEI_POINT,0)) as point
 FROM
       KR_054_SSOUTEI_MEISAI
 GROUP BY
       '付与想定P' ::varchar,
       POINT_YM ::varchar,
       POINT_YY ::varchar,
       POINT_MM ::varchar),
final as (
select
fuyo_label::varchar(30) as fuyo_label,
point_ym::varchar(9) as point_ym,
point_yy::varchar(6) as point_yy,
point_mm::varchar(3) as point_mm,
point::number(38,0) as point
from transformed
)
select * from final