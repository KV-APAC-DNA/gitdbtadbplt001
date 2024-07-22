with keirokbn as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KEIROKBN 
),
hanyo_attr as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.HANYO_ATTR
),
tbecorder as (
select * from DEV_DNA_LOAD.SNAPJPDCLSDL_RAW.TBECORDER
),
c_tbecusercard as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECUSERCARD
),
tbpromotion as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBPROMOTION
),
c_tbeckesai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECKESAI
),
transformed as (
SELECT CAST('U' || c_tbEcKesai.c_dikesaiid AS VARCHAR) AS SALENO,
     tbEcOrder.c_dsorderkbn AS JUCHKBN,
     CAST(TO_CHAR(tbEcOrder.dsOrderDt, 'YYYYMMDD') AS NUMERIC) AS JUCHDATE,
     NVL(LPAD(tbEcOrder.diEcUsrID, 10, '0'), '0000000000') AS KOKYANO,
     NVL(HANYO_ATTR.ATTR2, 'その他') AS HANROCODE
     --2018/4/20 ADD 芹澤 変更0261 START
     ,
     CASE 
          WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB'
               AND WEBKEIRO.KEIRO IS NOT NULL
               THEN NVL(WEBKEIRO.ATTR3, '不明')
          ELSE NVL(HANYO_ATTR.ATTR3, '不明')
          END AS SYOHANROBUNNAME,
     CASE 
          WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB'
               AND WEBKEIRO.KEIRO IS NOT NULL
               THEN NVL(WEBKEIRO.ATTR4, '不明')
          ELSE NVL(HANYO_ATTR.ATTR4, '不明')
          END AS CHUHANROBUNNAME,
     CASE 
          WHEN tbEcOrder.C_DSUKETSUKETELCOMPANYCD = 'WEB'
               AND WEBKEIRO.KEIRO IS NOT NULL
               THEN NVL(WEBKEIRO.ATTR5, '不明')
          ELSE NVL(HANYO_ATTR.ATTR5, '不明')
          END AS DAIHANROBUNNAME
     --2018/4/20 ADD 芹澤 変更0261 END
     ,
     NVL(tbPromotion.dspromcode, '00000') AS MEDIACODE,
     c_tbEcKesai.dskessaihoho AS KESSAIKBN,
     NVL(c_tbEcKesai.diHaisoPrc, 0) AS SORYO,
     c_tbEcKesai.diOrderTax AS TAX,
     NVL(c_tbEcKesai.diseikyuprc, 0) AS SOGOKEI,
     NVL(CAST(c_tbEcUserCard.c_dsCardCompanyId AS VARCHAR), '0000000') AS CARDCORPCODE,
     '000' AS HENREASONCODE,
     c_tbEcKesai.diCancel AS CANCELFLG,
     NVL(CAST(TO_CHAR(c_tbEcKesai.dsprep, 'YYYYMMDD') AS NUMERIC), 0) AS INSERTDATE,
     NVL(CAST(TO_CHAR(c_tbEcKesai.dsprep, 'HH24MISS') AS NUMERIC), 0) AS INSERTTIME,
     NVL(CAST(c_tbEcKesai.diprepusr AS VARCHAR), '000000') AS INSERTID,
     NVL(CAST(TO_CHAR(c_tbEcKesai.dsren, 'YYYYMMDD') AS NUMERIC), 0) AS UPDATEDATE,
     NVL(CAST(TO_CHAR(c_tbEcKesai.dsren, 'HH24MISS') AS NUMERIC), 0) AS UPDATETIME,
     NVL(c_tbEcKesai.dsTodokeZip, ' ') AS ZIPCODE,
     NVL(c_tbEcKesai.dsTodokePref, ' ') AS TODOFUKENCODE,
     NVL((c_tbEcKesai.c_diOrderAvailablePoint), 0) AS HAPPENPOINT,
     NVL(c_tbEcKesai.diUsePoint + c_tbEcKesai.c_diExchangePoint, 0) AS RIYOPOINT,
     NVL(c_tbEcKesai.diShukkaSts, '0') AS SHUKKASTS,
     DECODE(tbEcOrder.dirouteid, 7, '03', 9, '04', 8, '05', '01') AS TORIKEIKBN,
     NVL(tbEcOrder.c_dstempocode, '00000') AS TENPOCODE,
     NVL(CAST(TO_CHAR(c_tbEcKesai.dsUriageDt, 'YYYYMMDD') AS NUMERIC), 0) AS SHUKADATE,
     NVL(tbEcOrder.c_diClassID, 0) AS RANK,
     CAST(c_tbEcKesai.c_dikesaiid AS VARCHAR) AS DISPSALENO,
     c_tbEcKesai.c_dikesaiid AS KESAIID,
     ' ' AS HENREASONNAME,
     NVL(tbEcOrder.c_diuketsukeusrid, 0) AS UKETSUKEUSRID,
     NVL(tbEcOrder.C_DSUKETSUKETELCOMPANYCD, '000') AS UKETSUKETELCOMPANYCD,
     NVL(tbEcOrder.dirouteid, 0) AS SMKEIROID,
     tbEcOrder.diPromId AS DIPROMID,
     NULL AS KESAIROWID,
     NULL AS ORDERROWID,
     NULL AS USERCARDROWID,
     NULL AS PROMOROWID,
     NULL AS HANYOROWID
FROM c_tbEcKesai --決済情報
     ,
     tbEcOrder --受注情報
     ,
     c_tbEcUserCard --クレジットカード情報(顧客）
     ,
     tbPromotion --プロモーションマスタ
     ,
     HANYO_ATTR --汎用属性（通信経路）
     --2018/4/20 ADD 芹澤 変更0261 START
     ,
     (
          SELECT KEIROKBN.DIORDERID,
               KEIROKBN.KEIRO,
               HANYO_WEB.ATTR1,
               HANYO_WEB.ATTR3,
               HANYO_WEB.ATTR4,
               HANYO_WEB.ATTR5
          FROM KEIROKBN
          INNER JOIN HANYO_ATTR HANYO_WEB ON HANYO_WEB.ATTR1 = 'WEB' || '-' || KEIROKBN.KEIRO
               AND HANYO_WEB.KBNMEI = 'TUUSHINKEIRO'
          ) WEBKEIRO
--2018/4/20 ADD 芹澤 変更0261 END
WHERE c_tbEcKesai.diorderid = tbEcOrder.diOrderID --決済情報．受注内部ID = 受注情報．受注内部ID
     AND c_tbEcKesai.c_dicardid = c_tbEcUserCard.c_dicardid(+) --決済情報．クレジットカード内部ID = クレジットカード情報(顧客）．クレジットカード内部ID
     AND tbEcOrder.diPromId = tbPromotion.dipromid(+) --受注情報．プロモーションID = プロモーションマスタ．プロモーションID
     AND HANYO_ATTR.ATTR1(+) = tbEcOrder.C_DSUKETSUKETELCOMPANYCD --汎用属性（通信経路）.属性1（テレマ会社コード） = 受注情報．受付担当者テレマ会社コード
     AND HANYO_ATTR.KBNMEI(+) = 'TUUSHINKEIRO'
     AND c_tbEcKesai.diCancel = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
     AND tbEcOrder.diOrderID = WEBKEIRO.DIORDERID(+)
),
final as (
select
saleno::varchar(62) as saleno,
juchkbn::varchar(2) as juchkbn,
juchdate::number(18,0) as juchdate,
kokyano::varchar(30) as kokyano,
hanrocode::varchar(60) as hanrocode,
syohanrobunname::varchar(60) as syohanrobunname,
chuhanrobunname::varchar(60) as chuhanrobunname,
daihanrobunname::varchar(60) as daihanrobunname,
mediacode::varchar(8) as mediacode,
kessaikbn::varchar(4) as kessaikbn,
soryo::number(18,0) as soryo,
tax::number(10,0) as tax,
sogokei::number(18,0) as sogokei,
cardcorpcode::varchar(60) as cardcorpcode,
henreasoncode::varchar(3) as henreasoncode,
cancelflg::varchar(2) as cancelflg,
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
torikeikbn::varchar(3) as torikeikbn,
tenpocode::varchar(8) as tenpocode,
shukadate::number(18,0) as shukadate,
rank::number(18,0) as rank,
dispsaleno::varchar(60) as dispsaleno,
kesaiid::number(10,0) as kesaiid,
henreasonname::varchar(2) as henreasonname,
uketsukeusrid::number(18,0) as uketsukeusrid,
uketsuketelcompanycd::varchar(5) as uketsuketelcompanycd,
smkeiroid::number(18,0) as smkeiroid,
dipromid::number(10,0) as dipromid,
kesairowid::varchar(1) as kesairowid,
orderrowid::varchar(1) as orderrowid,
usercardrowid::varchar(1) as usercardrowid,
promorowid::varchar(1) as promorowid,
hanyorowid::varchar(1) as hanyorowid
from transformed
)
select * from final