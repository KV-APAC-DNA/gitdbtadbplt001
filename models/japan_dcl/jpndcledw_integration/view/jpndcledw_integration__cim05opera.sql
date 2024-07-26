with tbperson as (
select * from {{ ref('jpndclitg_integration__tbperson') }}
),
final as (
SELECT 
  (tbperson.diid):: character varying(22) AS opecode, 
  tbperson.dsname AS opename, 
  (tbperson.c_dstelcompanycd):: character varying(30) AS bumoncode, 
  tbperson.dslogin AS logincode, 
  'NEXT' AS ciflg, 
  to_number(
    COALESCE(
      to_char(
        (
          to_date(
            (
              COALESCE(tbperson.dsren, tbperson.dsprep)
            ):: text, 
            'YYYY-MM-DD HH24:MI:SS' :: text
          )
        ):: timestamp without time zone, 
        'YYYYMMDDHH24MISS' :: text
      ), 
      '0' :: text
    ), 
    '99999999999999' :: text
  ) AS join_rec_upddate 
FROM 
  tbperson 
WHERE 
  (
    (tbperson.dielimflg):: text = (0):: text
  )
)
select * from final
