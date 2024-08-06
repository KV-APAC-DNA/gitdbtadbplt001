with cit85osalh as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT85OSALH
),
cit80saleh as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT80SALEH
),
tt01saleh_sum_mv as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TT01SALEH_SUM_MV
),
union_1 as (
    SELECT 
      cit80saleh.saleno, 
      cit80saleh.shukadate, 
      cit80saleh.kaisha, 
      cit80saleh.kokyano, 
      cit80saleh.cancelflg, 
      cit80saleh.torikeikbn, 
      cit80saleh.hanrocode, 
      cit80saleh.syohanrobunname, 
      cit80saleh.chuhanrobunname, 
      cit80saleh.daihanrobunname, 
      cit80saleh.juchkbn, 
      cit80saleh.henreasoncode, 
      cit80saleh.henreasonname, 
      cit80saleh.kakokbn, 
      cit80saleh.dispsaleno, 
      cit80saleh.smkeiroid 
    FROM 
      cit80saleh 
    WHERE 
      (
        (cit80saleh.torikeikbn):: text = ('01' :: character varying):: text
      ) 
    UNION ALL 
    SELECT 
      cit85osalh.ourino AS saleno, 
      cit85osalh.shukadate, 
      cit85osalh.tokuicode AS kaisha, 
      cit85osalh.kokyano, 
      cit85osalh.cancelflg, 
      cit85osalh.torikeikbn, 
      'その他' AS hanrocode, 
      '不明' AS syohanrobunname, 
      '不明' AS chuhanrobunname, 
      '不明' AS daihanrobunname, 
      cit85osalh.juchkbn, 
      cit85osalh.henreasoncode, 
      cit85osalh.henreasonname, 
      cit85osalh.kakokbn, 
      cit85osalh.dispourino AS dispsaleno, 
      NULL AS smkeiroid 
    FROM 
      cit85osalh 
    WHERE 
      (
        (cit85osalh.shokuikibunrui):: text <> ('0' :: character varying):: text
      )
  ) ,
union_2 as (
  SELECT 
    tt01saleh_sum.saleno, 
    tt01saleh_sum.shukadate, 
    'XXX' AS kaisha, 
    tt01saleh_sum.kokyano, 
    (
      (tt01saleh_sum.cancelflg):: numeric
    ):: numeric(18, 0) AS cancelflg, 
    tt01saleh_sum.torikeikbn, 
    tt01saleh_sum.hanrocode, 
    tt01saleh_sum.syohanrobunname, 
    tt01saleh_sum.chuhanrobunname, 
    tt01saleh_sum.daihanrobunname, 
    tt01saleh_sum.juchkbn, 
    tt01saleh_sum.henreasoncode, 
    tt01saleh_sum.henreasonname, 
    0 AS kakokbn, 
    tt01saleh_sum.dispsaleno, 
    tt01saleh_sum.smkeiroid 
  FROM 
    tt01saleh_sum_mv tt01saleh_sum 
  WHERE 
    (
      (
        (
          (tt01saleh_sum.torikeikbn):: text = ('03' :: character varying):: text
        ) 
        OR (
          (tt01saleh_sum.torikeikbn):: text = ('04' :: character varying):: text
        )
      ) 
      OR (
        (tt01saleh_sum.torikeikbn):: text = ('05' :: character varying):: text
      )
    )
) ,
union_3 as ( SELECT 
  cit80saleh.saleno, 
  cit80saleh.shukadate, 
  cit80saleh.kaisha, 
  cit80saleh.kokyano, 
  cit80saleh.cancelflg, 
  cit80saleh.torikeikbn, 
  cit80saleh.hanrocode, 
  cit80saleh.syohanrobunname, 
  cit80saleh.chuhanrobunname, 
  cit80saleh.daihanrobunname, 
  cit80saleh.juchkbn, 
  cit80saleh.henreasoncode, 
  cit80saleh.henreasonname, 
  cit80saleh.kakokbn, 
  cit80saleh.dispsaleno, 
  cit80saleh.smkeiroid 
FROM 
  cit80saleh 
WHERE 
  (
    (
      cit80saleh.kakokbn = (
        (1):: numeric
      ):: numeric(18, 0)
    ) 
    AND (
      (
        (cit80saleh.torikeikbn):: text = ('03' :: character varying):: text
      ) 
      OR (
        (cit80saleh.torikeikbn):: text = ('04' :: character varying):: text
      )
    )
  )),
final as (
select * from union_1
union ALL
select * from union_2
union ALL
select * from union_3
)
select * from final

