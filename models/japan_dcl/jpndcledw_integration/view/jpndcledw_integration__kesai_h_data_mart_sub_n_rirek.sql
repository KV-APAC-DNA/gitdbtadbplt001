with tbecorderhist as (
select * from {{ ref('jpndclitg_integration__tbecorderhist') }}
),
final as (
SELECT 
  "max"(tbecorderhist.diorderhistid) AS diorderhistid, 
  tbecorderhist.diorderid 
FROM 
  tbecorderhist 
WHERE 
  (
    (
      (
        (tbecorderhist.c_dirirekikubun):: text = ('10' :: character varying):: text
      ) 
      OR (
        (tbecorderhist.c_dirirekikubun):: text = ('20' :: character varying):: text
      )
    ) 
    AND (
      (tbecorderhist.dielimflg):: text = (
        (0):: character varying
      ):: text
    )
  ) 
GROUP BY 
  tbecorderhist.diorderid
)
select * from final