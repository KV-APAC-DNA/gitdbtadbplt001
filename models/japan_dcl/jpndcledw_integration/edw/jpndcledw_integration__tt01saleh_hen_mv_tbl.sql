with tbecorder as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECORDER
),
tbpromotion as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBPROMOTION
),
c_tbeckesai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECKESAI
),
keirokbn as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KEIROKBN
),
tt01_henpin_riyu as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TT01_HENPIN_RIYU
),
hanyo_attr as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.HANYO_ATTR
),
tbechenpinriyu as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECHENPINRIYU
),
c_tbecinquirekesai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECINQUIREKESAI
),
c_tbEcInquire as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECINQUIRE
),
c_tbEcUserCard as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECUSERCARD
),

transformed as (
SELECT
('H' || CAST(c_tbEcInquireKesai.c_diinquirekesaiid as varchar))			AS	SALENO
,DECODE(tbEcOrder.c_dsorderkbn,'0','90','1','91','2','92','90')		AS	JUCHKBN
,CAST(TO_CHAR(tbEcOrder.dsOrderDt, 'YYYYMMDD') as numeric)			AS	JUCHDATE
,NVL(LPAD(c_tbEcKesai.diEcUsrID,10,'0'),'0000000000')	AS	KOKYANO
,NVL(HANYO_ATTR.ATTR2,'その他')						AS	HANROCODE
--2018/4/20 ADD 芹澤 変更0261 START
,CASE WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB' 
      AND  WEBKEIRO.KEIRO             IS NOT NULL THEN
           NVL(WEBKEIRO.ATTR3,'不明')
      ELSE NVL(HANYO_ATTR.ATTR3,'不明')
 END                                                AS  SYOHANROBUNNAME
,CASE WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB'
      AND  WEBKEIRO.KEIRO             IS NOT NULL THEN
           NVL(WEBKEIRO.ATTR4,'不明')
      ELSE NVL(HANYO_ATTR.ATTR4,'不明')
 END                                                AS  CHUHANROBUNNAME
,CASE WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB'
      AND  WEBKEIRO.KEIRO             IS NOT NULL THEN
           NVL(WEBKEIRO.ATTR5,'不明')
      ELSE NVL(HANYO_ATTR.ATTR5,'不明')
 END                                                AS  DAIHANROBUNNAME
--2018/4/20 ADD 芹澤 変更0261 END
,NVL(tbPromotion.dspromcode,'00000')					AS	MEDIACODE
,c_tbEcKesai.dskessaihoho						AS	KESSAIKBN
,NVL(c_tbEcInquireKesai.dihaisoprc,0)					AS	SORYO
,c_tbEcInquireKesai.diOrderTax						AS	TAX
,NVL(c_tbEcInquireKesai.c_dinyukinprc,0)				AS	SOGOKEI
,NVL(CAST(c_tbEcUserCard.c_dsCardCompanyId as varchar),'0000000')	AS	CARDCORPCODE
,NVL(CAST(c_tbEcInquireMeisai.dihenpinriyuid as varchar),'000')			AS	HENREASONCODE
,c_tbEcKesai.diCancel							AS	CANCELFLG
,NVL(TO_NUMBER(TO_CHAR(c_tbEcInquireKesai.dsPrep,'YYYYMMDD'),'99999999'),0)	AS	INSERTDATE
,NVL(TO_NUMBER(TO_CHAR(c_tbEcInquireKesai.dsPrep,'HH24MISS'),'99999999'),0)	AS	INSERTTIME
,NVL(CAST(c_tbEcInquireKesai.diPrepUsr as varchar),'000000')	AS	INSERTID
,NVL(TO_NUMBER(TO_CHAR(c_tbEcInquireKesai.dsRen,'YYYYMMDD'),'99999999'),0)		AS	UPDATEDATE
,NVL(TO_NUMBER(TO_CHAR(c_tbEcInquireKesai.dsRen,'HH24MISS'),'99999999'),0)		AS	UPDATETIME
,NVL(c_tbEcInquireKesai.dsTodokeZip,' ')				AS	ZIPCODE
,NVL(c_tbEcInquireKesai.dsTodokePref,' ')				AS	TODOFUKENCODE
,NVL((c_tbEcKesai.diUsePoint + c_tbEcKesai.c_diExchangePoint),0)	AS	HAPPENPOINT
,NVL(c_tbEcInquireKesai.c_diHenkyakuYoteiPoint,0)			AS	RIYOPOINT
,NVL(c_tbEcKesai.diShukkaSts,'0')					AS	SHUKKASTS
,DECODE(tbEcOrder.dirouteid, 7, '03', 9, '04', 8, '05', '01')		AS	TORIKEIKBN
,NVL(tbEcOrder.c_dstempocode,'00000')					AS	TENPOCODE
,NVL(TO_NUMBER(TO_CHAR(c_tbEcKesai.dsUriageDt, 'YYYYMMDD'),'99999999'),0)		AS	SHUKADATE
,NVL(tbEcOrder.c_diClassID,0)						AS	RANK
,CAST(c_tbEcInquireKesai.c_dikesaiid as varchar)				AS	DISPSALENO
,c_tbEcInquireKesai.c_dikesaiid						AS	KESAIID
,NVL(tbEcHenpinRiyu.dshenpinriyu,' ')					AS	HENREASONNAME
,NVL(tbEcOrder.c_diuketsukeusrid,0)					AS	UKETSUKEUSRID
,NVL(tbEcOrder.C_DSUKETSUKETELCOMPANYCD,'000')				AS	UKETSUKETELCOMPANYCD
,NVL(tbEcOrder.dirouteid,0)						AS	SMKEIROID
,tbEcOrder.diPromId		AS DIPROMID
,null AS INQKESAIROWID
,null AS INQROWID
,null AS KESAIROWID
,null AS ORDERROWID
,null AS UCROWID
,null AS INQMEISAIROWID
,null AS HENRIYUROWID
,null AS PROMOROWID
,null AS HANYOROWID
FROM c_tbEcInquireKesai --返品交換依頼決済情報
    ,c_tbEcInquire --返品交換依頼情報
    ,c_tbEcKesai --決済情報
    ,tbEcOrder --受注情報
    ,c_tbEcUserCard --クレジットカード情報(顧客）
    ,TT01_HENPIN_RIYU c_tbEcInquireMeisai --返品交換依頼明細情報
    ,tbEcHenpinRiyu --返品理由
    ,tbPromotion --プロモーションマスタ
    ,HANYO_ATTR --汎用属性（通信経路）
--2018/4/20 ADD 芹澤 変更0261 START
    , (
      SELECT  KEIROKBN.DIORDERID
             ,KEIROKBN.KEIRO
             ,HANYO_WEB.ATTR1
             ,HANYO_WEB.ATTR3
             ,HANYO_WEB.ATTR4
             ,HANYO_WEB.ATTR5
       FROM   KEIROKBN
       INNER JOIN HANYO_ATTR HANYO_WEB
       ON HANYO_WEB.ATTR1     = 'WEB' || '-' || KEIROKBN.KEIRO
       AND HANYO_WEB.KBNMEI = 'TUUSHINKEIRO'
       )WEBKEIRO
--2018/4/20 ADD 芹澤 変更0261 END
WHERE c_tbEcInquireKesai.diinquireid = c_tbEcInquire.diinquireid --返品交換依頼決済情報.返品交換依頼ID = 返品交換依頼情報.返品交換依頼ID
AND c_tbEcInquireKesai.c_dikesaiid = c_tbEcKesai.c_dikesaiid --返品交換依頼決済情報.決済ID = 決済情報.決済ID
AND c_tbEcInquireKesai.diorderid = tbEcOrder.diOrderID --返品交換依頼決済情報.受注内部ID = 受注情報.受注内部ID
AND c_tbEcKesai.c_dicardid = c_tbEcUserCard.c_dicardid(+) --決済情報.クレジットカード内部ID = クレジットカード情報(顧客）.クレジットカード内部ID
AND c_tbEcInquire.diinquireid = c_tbEcInquireMeisai.diinquireid --返品交換依頼情報.返品交換依頼ID = TT01返品理由.返品交換依頼ID
AND c_tbEcInquireKesai.c_diinquirekesaiid = c_tbEcInquireMeisai.diinquirekesaiid --返品交換依頼決済情報.返品交換依頼決済ID = TT01返品理由.返品交換依頼決済ID
AND c_tbEcInquireMeisai.dihenpinriyuid = tbEcHenpinRiyu.dihenpinriyuid(+) --返品交換依頼情報.返品理由ID = 返品理由.返品理由ID
AND c_tbEcKesai.diCancel = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
AND tbEcHenpinRiyu.dielimflg(+) = '0'--返品理由.削除FLG = 0：未削除
AND tbEcOrder.diPromId = tbPromotion.dipromid(+) --受注情報．プロモーションID = プロモーションマスタ．プロモーションID
AND tbEcOrder.C_DSUKETSUKETELCOMPANYCD = HANYO_ATTR.ATTR1(+) --受注情報．受付担当者テレマ会社コード = 汎用属性（通信経路）.属性1（テレマ会社コード）
AND HANYO_ATTR.KBNMEI(+) = 'TUUSHINKEIRO'
AND tbEcOrder.diOrderID = WEBKEIRO.DIORDERID(+)
),
final as (
select
saleno::varchar(62) as saleno,
juchkbn::varchar(10) as juchkbn,
juchdate::number(18,0) as juchdate,
kokyano::varchar(30) as kokyano,
hanrocode::varchar(60) as hanrocode,
syohanrobunname::varchar(60) as syohanrobunname,
chuhanrobunname::varchar(60) as chuhanrobunname,
daihanrobunname::varchar(60) as daihanrobunname,
mediacode::varchar(8) as mediacode,
kessaikbn::varchar(10) as kessaikbn,
soryo::number(18,0) as soryo,
tax::number(10,0) as tax,
sogokei::number(18,0) as sogokei,
cardcorpcode::varchar(60) as cardcorpcode,
henreasoncode::varchar(60) as henreasoncode,
cancelflg::varchar(20) as cancelflg,
insertdate::number(18,0) as insertdate,
inserttime::number(18,0) as inserttime,
insertid::varchar(60) as insertid,
updatedate::number(18,0) as updatedate,
updatetime::number(18,0) as updatetime,
zipcode::varchar(15) as zipcode,
todofukencode::varchar(15) as todofukencode,
happenpoint::number(18,0) as happenpoint,
riyopoint::number(18,0) as riyopoint,
shukkasts::varchar(6) as shukkasts,
torikeikbn::varchar(10) as torikeikbn,
tenpocode::varchar(8) as tenpocode,
shukadate::number(18,0) as shukadate,
rank::number(18,0) as rank,
dispsaleno::varchar(60) as dispsaleno,
kesaiid::number(10,0) as kesaiid,
henreasonname::varchar(48) as henreasonname,
uketsukeusrid::number(18,0) as uketsukeusrid,
uketsuketelcompanycd::varchar(10) as uketsuketelcompanycd,
smkeiroid::number(18,0) as smkeiroid,
dipromid::number(10,0) as dipromid,
inqkesairowid::varchar(1) as inqkesairowid,
inqrowid::varchar(1) as inqrowid,
kesairowid::varchar(1) as kesairowid,
orderrowid::varchar(1) as orderrowid,
ucrowid::varchar(1) as ucrowid,
inqmeisairowid::varchar(1) as inqmeisairowid,
henriyurowid::varchar(1) as henriyurowid,
promorowid::varchar(1) as promorowid,
hanyorowid::varchar(1) as hanyorowid,
current_timestamp()::timestamp_ntz(9) as inserted_date,
'etl_batch'::varchar(100) as inserted_by ,
current_timestamp::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
from transformed
)
select * from final 