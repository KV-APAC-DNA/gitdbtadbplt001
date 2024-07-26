with c_tbecinquirekesai as (
select * from {{ ref('jpndclitg_integration__c_tbecinquirekesai') }}
),
c_tbecinquiremeisai as (
select * from {{ ref('jpndclitg_integration__c_tbecinquiremeisai') }}
),
c_tbeckesai as (
select * from {{ ref('jpndclitg_integration__c_tbeckesai') }}
),
transformed as (
SELECT
       'H' || cast(c_tbecinquiremeisai.diinquirekesaiid as varchar) as saleno,
       nvl(c_tbecinquiremeisai.dimeisaiid,0) as gyono,
       c_tbecinquiremeisai.dsitemid as itemcode,
       nvl(c_tbecinquiremeisai.c_dihenpinnum,0) as suryo,
       nvl(c_tbecinquiremeisai.ditotalprc,0) as tanka,
       nvl(c_tbecinquiremeisai.c_diitemtotalprc,0) as kingaku,
       nvl((c_tbecinquiremeisai.diitemprc * c_dihenpinnum),0) as meisainukikingaku,
       nvl(c_tbecinquiremeisai.diitemtax,0) as meisaitax,
       nvl(c_tbecinquiremeisai.c_didiscountrate,0) as wariritu,
       trunc(decode(c_tbecinquiremeisai.c_dihenpinnum,0,0,((nvl(c_tbecinquiremeisai.c_diitemtotalprc,0)+ nvl(c_tbecinquiremeisai.c_didiscountmeisai,0))/ nvl(c_tbecinquiremeisai.c_dihenpinnum,1)))) as warimaekomitanka,
       nvl((c_tbecinquiremeisai.diusualprc * c_tbecinquiremeisai.c_dihenpinnum),0) as warimaenukikingaku,
       nvl((c_tbecinquiremeisai.c_diitemtotalprc + c_tbecinquiremeisai.c_didiscountmeisai),0) as warimaekomikingaku,
       cast(c_tbecinquiremeisai.c_dikesaiid as varchar) as dispsaleno,
       c_tbecinquiremeisai.c_dikesaiid as kesaiid,
       null as inqmeisairowid,
       null as inqkesairowid,
       null as kesairowid
 from
       c_tbecinquiremeisai c_tbecinquiremeisai,
       c_tbecinquirekesai c_tbecinquirekesai,
       c_tbeckesai c_tbeckesai
 where   
       c_tbecinquiremeisai.diinquireid = c_tbecinquirekesai.diinquireid and  
       c_tbecinquiremeisai.diinquirekesaiid = c_tbecinquirekesai.c_diinquirekesaiid and  
       c_tbecinquirekesai.c_dikesaiid = c_tbeckesai.c_dikesaiid and  
       c_tbeckesai.dicancel = '0' and  
       c_tbecinquiremeisai.dihenpinkubun <> 0
 ),
 final as (
select
saleno::varchar(62) as saleno,
gyono::number(18,0) as gyono,
itemcode::varchar(45) as itemcode,
suryo::number(18,0) as suryo,
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
inqmeisairowid::varchar(1) as inqmeisairowid,
inqkesairowid::varchar(1) as inqkesairowid,
kesairowid::varchar(1) as kesairowid,
current_timestamp()::timestamp_ntz(9) as inserted_date,
'etl_batch'::varchar(100) as inserted_by ,
current_timestamp::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
from transformed
 )
select * from final


