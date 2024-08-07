with aspac_common_new as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.aspac_common_new
),
final as (
SELECT 
  aspac_common_new.todofukennm AS areaname, 
  aspac_common_new.shimebi, 
  aspac_common_new.kingaku, 
  aspac_common_new.nohindate, 
  aspac_common_new.shukadate, 
  aspac_common_new.torikeikbn 
FROM 
  aspac_common_new 
WHERE 
  (
    (
      (
        (aspac_common_new.sum_todofuken):: text = ('その他' :: character varying):: text
      ) 
      OR (
        (aspac_common_new.sum_todofuken):: text = ('住商DS' :: character varying):: text
      )
    ) 
    OR (
      (aspac_common_new.sum_todofuken):: text = ('シロノG' :: character varying):: text
    )
  )
)
select * from final