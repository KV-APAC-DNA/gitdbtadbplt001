{% if build_month_end_job_models()  %}
with KR_054_ALLADM as  (
    select * from {{ ref('jpndcledw_integration__kr_054_alladm') }}
),
KR_COMM_POINT_PARA as (
    select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),
final as (

SELECT DIECUSRID      AS KOKYANO
      ,C_DIISSUEPOINT AS POINT
  FROM KR_054_ALLADM                         -- ＜ヽード葺*・・潤[ク
 WHERE DIELIMFLG                  = '0'
   AND DIREGISTDIVCODE            = '10104'
   AND TO_CHAR(DSPOINTREN,'YYYY') = (select Target_Year from KR_COMM_POINT_PARA)
)

SELECT * from final
{% else %}
    select * from {{this}}
{% endif %}
