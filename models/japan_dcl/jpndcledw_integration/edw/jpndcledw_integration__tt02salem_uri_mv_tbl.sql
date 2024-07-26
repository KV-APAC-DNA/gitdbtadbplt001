with tbecordermeisai as (
select * from dev_dna_core.jpdclitg_integration.tbecordermeisai
),
c_tbeckesai as (
select * from {{ ref('jpndclitg_integration__c_tbeckesai') }}
),
tbecorder as (
select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
transformed as (
select
   'U' || cast(tbecordermeisai.c_dikesaiid as varchar) as saleno,
   nvl(tbecordermeisai.dimeisaiid,0) as gyono,
   tbecordermeisai.dsitemid as itemcode,
   tbecordermeisai.diitemnum as suryo,
   trunc(decode(tbecordermeisai.diitemnum,0,0,(nvl(tbecordermeisai.c_diitemtotalprc,0)- nvl(tbecordermeisai.c_didiscountmeisai,0))/ nvl(tbecordermeisai.diitemnum,1))) as tanka,
   decode(tbecordermeisai.c_dinoshinshoitemprc,0,0,nvl(tbecordermeisai.c_diitemtotalprc,0)- nvl(tbecordermeisai.c_didiscountmeisai,0)) as kingaku,
   decode(tbecordermeisai.c_dinoshinshoitemprc,0,0,ceil(((nvl(tbecordermeisai.c_diitemtotalprc,0)- nvl(tbecordermeisai.c_didiscountmeisai,0))/trunc((100 + tbecorder.ditaxrate)/ 100)))) as meisainukikingaku,
   decode(tbecordermeisai.c_dinoshinshoitemprc,0,0,(nvl(tbecordermeisai.c_diitemtotalprc,0)- nvl(tbecordermeisai.c_didiscountmeisai,0))- ceil(((nvl(tbecordermeisai.c_diitemtotalprc,0)- nvl(tbecordermeisai.c_didiscountmeisai,0))/trunc((100 + tbecorder.ditaxrate)/ 100)))) as meisaitax,
   decode(tbecordermeisai.c_dinoshinshoitemprc,0,0,nvl(tbecordermeisai.c_didiscountrate,0)) as wariritu,
   nvl(tbecordermeisai.ditotalprc,0) as warimaekomitanka,
   decode(tbecordermeisai.c_dinoshinshoitemprc,0,0,nvl((tbecordermeisai.c_diitemtotalprc - tbecordermeisai.diitemtax),0)) as warimaenukikingaku,
   decode(tbecordermeisai.c_dinoshinshoitemprc,0,0,nvl(tbecordermeisai.c_diitemtotalprc,0)) as warimaekomikingaku,
   cast(tbecordermeisai.c_dikesaiid as varchar) as dispsaleno,
   tbecordermeisai.c_dikesaiid as kesaiid,
   null as meisairowid,
   null as kesairowid,
   null as orderrowid
 from
       tbecordermeisai tbecordermeisai,
       c_tbeckesai c_tbeckesai,
       tbecorder tbecorder
 where   
       tbecordermeisai.c_dikesaiid = c_tbeckesai.c_dikesaiid and  
       tbecordermeisai.diorderid = tbecorder.diorderid and  
       c_tbeckesai.dicancel = '0'
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