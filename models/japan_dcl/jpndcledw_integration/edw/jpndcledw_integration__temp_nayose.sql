
with TEMP_REL as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.TEMP_REL
),
TM58_KOYKARANK_YM as (
   select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.TM58_KOYKARANK_YM
),
union_1 as (
SELECT
       ROWNUM as seq,
       TM58.YM,
       REL.C_DIPARENTUSRID as KOKYANO,
       TM58.RANK,
       REL.C_DIPARENTUSRID,
       REL.C_DICHILDUSRID
 FROM
       TEMP_REL REL
 INNER JOIN 
   TM58_KOYKARANK_YM TM58
  ON
       REL.C_DICHILDUSRID = TM58.KOKYANO
),
union_2 as (
SELECT
       REL.ROWNUM + NVL((SELECT MAX(seq) FROM union_1),0),    
       TM58.YM,
       TM58.KOKYANO,
       TM58.RANK,
       TM58.KOKYANO as C_DIPARENTUSRID,
       TM58.KOKYANO as C_DICHILDUSRID
 FROM
 (SELECT
 distinct
       C_DIPARENTUSRID,rownum
 FROM
       TEMP_REL ) REL
 INNER JOIN 
    TM58_KOYKARANK_YM TM58
  ON
       REL.C_DIPARENTUSRID = TM58.KOKYANO
),
transformed as (
select * from union_1
union all
select * from union_2
),
final as (
select
seq::number(38,0) as seq,
ym::varchar(9) as ym,
kokyano::varchar(15) as kokyano,
rank::varchar(18) as rank,
c_diparentusrid::varchar(15) as c_diparentusrid,
c_dichildusrid::varchar(15) as c_dichildusrid,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
from transformed
)
select * from final
