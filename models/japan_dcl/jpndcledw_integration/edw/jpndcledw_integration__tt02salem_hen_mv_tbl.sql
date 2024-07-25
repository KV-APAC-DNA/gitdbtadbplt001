with c_tbecinquirekesai as (
select * from DEV_DNA_CORE.JPDCLITG_INTEGRATION.C_TBECINQUIREKESAI
),
c_tbecinquiremeisai as (
select * from DEV_DNA_CORE.JPDCLITG_INTEGRATION.C_TBECINQUIREMEISAI
),
c_tbeckesai as (
select * from DEV_DNA_CORE.JPDCLITG_INTEGRATION.C_TBECKESAI
),
transformed as (
SELECT
       'H' || CAST(c_tbEcInquireMeisai.diinquirekesaiid as VARCHAR) as SALENO,
       NVL(c_tbEcInquireMeisai.DIMEISAIID,0) as GYONO,
       c_tbEcInquireMeisai.dsItemID as ITEMCODE,
       NVL(c_tbEcInquireMeisai.c_dihenpinnum,0) as SURYO,
       NVL(c_tbEcInquireMeisai.ditotalprc,0) as TANKA,
       NVL(c_tbEcInquireMeisai.c_diitemtotalprc,0) as KINGAKU,
       NVL((c_tbEcInquireMeisai.diitemprc * c_dihenpinnum),0) as MEISAINUKIKINGAKU,
       NVL(c_tbEcInquireMeisai.diitemtax,0) as MEISAITAX,
       NVL(c_tbEcInquireMeisai.c_didiscountrate,0) as WARIRITU,
       trunc(DECODE(c_tbEcInquireMeisai.c_dihenpinnum,0,0,((NVL(c_tbEcInquireMeisai.c_diitemtotalprc,0)+ NVL(c_tbEcInquireMeisai.c_didiscountmeisai,0))/ NVL(c_tbEcInquireMeisai.c_dihenpinnum,1)))) as WARIMAEKOMITANKA,
       NVL((c_tbEcInquireMeisai.diUsualPrc * c_tbEcInquireMeisai.c_dihenpinnum),0) as WARIMAENUKIKINGAKU,
       NVL((c_tbEcInquireMeisai.c_diitemtotalprc + c_tbEcInquireMeisai.c_didiscountmeisai),0) as WARIMAEKOMIKINGAKU,
       CAST(c_tbEcInquireMeisai.c_dikesaiid as VARCHAR) as DISPSALENO,
       c_tbEcInquireMeisai.c_dikesaiid as KESAIID,
       null as INQMEISAIROWID,
       null as INQKESAIROWID,
       null as KESAIROWID
 FROM
       c_tbEcInquireMeisai c_tbEcInquireMeisai,
       c_tbEcInquireKesai c_tbEcInquireKesai,
       c_tbEcKesai c_tbEcKesai
 WHERE   
       c_tbEcInquireMeisai.diinquireid = c_tbEcInquireKesai.diinquireid AND  
       c_tbEcInquireMeisai.diinquirekesaiid = c_tbEcInquireKesai.c_diinquirekesaiid AND  
       c_tbEcInquireKesai.c_dikesaiid = c_tbEcKesai.c_dikesaiid AND  
       c_tbEcKesai.diCancel = '0' AND  
       c_tbEcInquireMeisai.dihenpinkubun <> 0
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


