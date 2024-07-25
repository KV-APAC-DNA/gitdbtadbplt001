with tbEcOrderMeisai as (
select * from DEV_DNA_CORE.JPDCLITG_INTEGRATION.TBECORDERMEISAI
),
c_tbEcKesai as (
select * from DEV_DNA_CORE.JPDCLITG_INTEGRATION.c_tbEcKesai
),
tbecorder as (
select * from DEV_DNA_CORE.JPDCLITG_INTEGRATION.tbecorder
),
transformed as (
SELECT
   'U' || CAST(tbEcOrderMeisai.c_dikesaiid as VARCHAR) as SALENO,
   NVL(tbEcOrderMeisai.DIMEISAIID,0) as GYONO,
   tbEcOrderMeisai.dsItemID as ITEMCODE,
   tbEcOrderMeisai.diItemNum as SURYO,
   trunc(DECODE(tbEcOrderMeisai.diItemNum,0,0,(NVL(tbEcOrderMeisai.c_diitemtotalprc,0)- NVL(tbEcOrderMeisai.c_didiscountmeisai,0))/ NVL(tbEcOrderMeisai.diItemNum,1))) as TANKA,
   DECODE(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,NVL(tbEcOrderMeisai.c_diitemtotalprc,0)- NVL(tbEcOrderMeisai.c_didiscountmeisai,0)) as KINGAKU,
   DECODE(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,ceil(((NVL(tbEcOrderMeisai.c_diitemtotalprc,0)- NVL(tbEcOrderMeisai.c_didiscountmeisai,0))/trunc((100 + tbecorder.DITAXRATE)/ 100)))) as MEISAINUKIKINGAKU,
   DECODE(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,(NVL(tbEcOrderMeisai.c_diitemtotalprc,0)- NVL(tbEcOrderMeisai.c_didiscountmeisai,0))- ceil(((NVL(tbEcOrderMeisai.c_diitemtotalprc,0)- NVL(tbEcOrderMeisai.c_didiscountmeisai,0))/trunc((100 + tbecorder.DITAXRATE)/ 100)))) as MEISAITAX,
   DECODE(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,NVL(tbEcOrderMeisai.c_didiscountrate,0)) as WARIRITU,
   NVL(tbEcOrderMeisai.diTotalPrc,0) as WARIMAEKOMITANKA,
   DECODE(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,NVL((tbEcOrderMeisai.c_diitemtotalprc - tbEcOrderMeisai.diItemTax),0)) as WARIMAENUKIKINGAKU,
   DECODE(tbEcOrderMeisai.c_dinoshinshoitemprc,0,0,NVL(tbEcOrderMeisai.c_diitemtotalprc,0)) as WARIMAEKOMIKINGAKU,
   CAST(tbEcOrderMeisai.c_dikesaiid as VARCHAR) as DISPSALENO,
   tbEcOrderMeisai.c_dikesaiid as KESAIID,
   null as MEISAIROWID,
   null as KESAIROWID,
   null as ORDERROWID
 FROM
       tbEcOrderMeisai tbEcOrderMeisai,
       c_tbEcKesai c_tbEcKesai,
       tbecorder tbecorder
 WHERE   
       tbEcOrderMeisai.c_dikesaiid = c_tbEcKesai.c_dikesaiid AND  
       tbEcOrderMeisai.DIORDERID = tbecorder.DIORDERID AND  
       c_tbEcKesai.diCancel = '0'
 ),
 final as (
 select 
 saleno::varchar(62) as saleno,
gyono::number(18,0) as gyono,
itemcode::varchar(45) as itemcode,
suryo::number(10,0) as suryo,
tanka::number(18,0) as tanka,
kingaku::number(18,0) as kingaku,
meisainukikingaku::number(18,0) as meisainukikingaku,
meisaitax::number(18,0) as meisaitax,
wariritu::number(18,0) as wariritu,
warimaekomitanka::number(18,0) as warimaekomitanka,
warimaenukikingaku::number(18,0) as warimaenukikingaku,
warimaekomikingaku::number(18,0) as warimaekomikingaku,
dispsaleno::varchar(60) as dispsaleno,
kesaiid::number(10,0) as kesaiid,
meisairowid::varchar(1) as meisairowid,
kesairowid::varchar(1) as kesairowid,
orderrowid::varchar(1) as orderrowid,
current_timestamp()::timestamp_ntz(9) as inserted_date,
'etl_batch'::varchar(100) as inserted_by ,
current_timestamp::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
from transformed
)
select * from final