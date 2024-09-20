with syouhincd_henkan as (
select * from {{ ref('jpndcledw_integration__syouhincd_henkan') }}
),
cit86osalm as (
select * from {{ ref('jpndcledw_integration__cit86osalm') }}
),
cit85osalh as (
select * from {{ ref('jpndcledw_integration__cit85osalh') }}
),
cit81salem as (
select * from {{ ref('jpndcledw_integration__cit81salem') }}
),
cit80saleh as (
select * from {{ ref('jpndcledw_integration__cit80saleh') }}
),
tt02salem_mv as (
select * from {{ ref('jpndcledw_integration__tt02salem_mv') }}
),
tt01saleh_sum_mv as (
select * from {{ ref('jpndcledw_integration__tt01saleh_sum_mv') }}
),
final as (
(
  (
    SELECT 
      cit81salem.saleno, 
      cit81salem.gyono, 
      cit81salem.itemcode, 
      cit81salem.itemcode_hanbai, 
      cit81salem.suryo, 
      cit81salem.meisainukikingaku, 
      cit81salem.kakokbn, 
      cit81salem.dispsaleno, 
      cit81salem.tyoseikikingaku 
    FROM 
      (
        cit80saleh cit80saleh 
        JOIN cit81salem cit81salem ON (
          (
            (cit80saleh.saleno):: text = (cit81salem.saleno):: text
          )
        )
      ) 
    WHERE 
      (
        (cit80saleh.torikeikbn):: text = ('01' :: character varying):: text
      ) 
    UNION ALL 
    SELECT 
      cit86osalm.ourino AS saleno, 
      cit86osalm.gyono, 
      cit86osalm.itemcode, 
      cit86osalm.itemcode_hanbai, 
      cit86osalm.suryo, 
      cit86osalm.meisainukikingaku, 
      cit86osalm.kakokbn, 
      cit86osalm.dispourino AS dispsaleno, 
      0 AS tyoseikikingaku 
    FROM 
      (
        cit85osalh 
        JOIN cit86osalm ON (
          (
            (cit85osalh.ourino):: text = (cit86osalm.ourino):: text
          )
        )
      ) 
    WHERE 
      (
        (cit85osalh.shokuikibunrui):: text <> ('0' :: character varying):: text
      )
  ) 
  UNION ALL 
  SELECT 
    tt02salem_mv.saleno, 
    tt02salem_mv.gyono, 
    COALESCE(
      henkan.koseiocode, tt02salem_mv.itemcode
    ) AS itemcode, 
    tt02salem_mv.itemcode AS itemcode_hanbai, 
    tt02salem_mv.suryo, 
    tt02salem_mv.meisainukikingaku, 
    0 AS kakokbn, 
    tt02salem_mv.dispsaleno, 
    0 AS tyoseikikingaku 
  FROM 
    (
      (
        tt01saleh_sum_mv tt01saleh_sum 
        JOIN tt02salem_mv ON (
          (
            (tt01saleh_sum.saleno):: text = (tt02salem_mv.saleno):: text
          )
        )
      ) 
      LEFT JOIN syouhincd_henkan henkan ON (
        (
          (tt02salem_mv.itemcode):: text = (henkan.itemcode):: text
        )
      )
    ) 
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
) 
UNION ALL 
SELECT 
  cit81salem.saleno, 
  cit81salem.gyono, 
  cit81salem.itemcode, 
  cit81salem.itemcode_hanbai, 
  cit81salem.suryo, 
  cit81salem.meisainukikingaku, 
  cit81salem.kakokbn, 
  cit81salem.dispsaleno, 
  cit81salem.tyoseikikingaku 
FROM 
  (
    cit80saleh cit80saleh 
    JOIN cit81salem cit81salem ON (
      (
        (cit80saleh.saleno):: text = (cit81salem.saleno):: text
      )
    )
  ) 
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
  )
)
select * from final