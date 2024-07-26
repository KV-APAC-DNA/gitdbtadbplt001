with keirokbn as (
select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.KEIROKBN 
),
hanyo_attr as (
select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.HANYO_ATTR
),
tbecorder as (
select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
c_tbecusercard as (
select * from {{ ref('jpndclitg_integration__c_tbecusercard') }}
),
tbpromotion as (
select * from {{ ref('jpndclitg_integration__tbpromotion') }}
),
c_tbeckesai as (
select * from {{ ref('jpndclitg_integration__c_tbeckesai') }}
),
transformed as (
select cast('u' || c_tbeckesai.c_dikesaiid as varchar) as saleno,
     tbecorder.c_dsorderkbn as juchkbn,
     cast(to_char(tbecorder.dsorderdt, 'yyyymmdd') as numeric) as juchdate,
     nvl(lpad(tbecorder.diecusrid, 10, '0'), '0000000000') as kokyano,
     nvl(hanyo_attr.attr2, 'その他') as hanrocode
     --2018/4/20 add 芹澤 変更0261 start
     ,
     case 
          when tbecorder.c_dsuketsuketelcompanycd = 'web'
               and webkeiro.keiro is not null
               then nvl(webkeiro.attr3, '不明')
          else nvl(hanyo_attr.attr3, '不明')
          end as syohanrobunname,
     case 
          when tbecorder.c_dsuketsuketelcompanycd = 'WEB'
               and webkeiro.keiro is not null
               then nvl(webkeiro.attr4, '不明')
          else nvl(hanyo_attr.attr4, '不明')
          end as chuhanrobunname,
     case 
          when tbecorder.c_dsuketsuketelcompanycd = 'WEB'
               and webkeiro.keiro is not null
               then nvl(webkeiro.attr5, '不明')
          else nvl(hanyo_attr.attr5, '不明')
          end as daihanrobunname
     --2018/4/20 add 芹澤 変更0261 end
     ,
     nvl(tbpromotion.dspromcode, '00000') as mediacode,
     c_tbeckesai.dskessaihoho as kessaikbn,
     nvl(c_tbeckesai.dihaisoprc, 0) as soryo,
     c_tbeckesai.diordertax as tax,
     nvl(c_tbeckesai.diseikyuprc, 0) as sogokei,
     nvl(cast(c_tbecusercard.c_dscardcompanyid as varchar), '0000000') as cardcorpcode,
     '000' as henreasoncode,
     c_tbeckesai.dicancel as cancelflg,
     nvl(cast(to_char(c_tbeckesai.dsprep, 'YYYYMMDD') as numeric), 0) as insertdate,
     nvl(cast(to_char(c_tbeckesai.dsprep, 'HH24MISS') as numeric), 0) as inserttime,
     nvl(cast(c_tbeckesai.diprepusr as varchar), '000000') as insertid,
     nvl(cast(to_char(c_tbeckesai.dsren, 'YYYYMMDD') as numeric), 0) as updatedate,
     nvl(cast(to_char(c_tbeckesai.dsren, 'HH24MISS') as numeric), 0) as updatetime,
     nvl(c_tbeckesai.dstodokezip, ' ') as zipcode,
     nvl(c_tbeckesai.dstodokepref, ' ') as todofukencode,
     nvl((c_tbeckesai.c_diorderavailablepoint), 0) as happenpoint,
     nvl(c_tbeckesai.diusepoint + c_tbeckesai.c_diexchangepoint, 0) as riyopoint,
     nvl(c_tbeckesai.dishukkasts, '0') as shukkasts,
     decode(tbecorder.dirouteid, 7, '03', 9, '04', 8, '05', '01') as torikeikbn,
     nvl(tbecorder.c_dstempocode, '00000') as tenpocode,
     nvl(cast(to_char(c_tbeckesai.dsuriagedt, 'YYYYMMDD') as numeric), 0) as shukadate,
     nvl(tbecorder.c_diclassid, 0) as rank,
     cast(c_tbeckesai.c_dikesaiid as varchar) as dispsaleno,
     c_tbeckesai.c_dikesaiid as kesaiid,
     ' ' as henreasonname,
     nvl(tbecorder.c_diuketsukeusrid, 0) as uketsukeusrid,
     nvl(tbecorder.c_dsuketsuketelcompanycd, '000') as uketsuketelcompanycd,
     nvl(tbecorder.dirouteid, 0) as smkeiroid,
     tbecorder.dipromid as dipromid,
     null as kesairowid,
     null as orderrowid,
     null as usercardrowid,
     null as promorowid,
     null as hanyorowid
from c_tbeckesai --決済情報
     ,
     tbecorder --受注情報
     ,
     c_tbecusercard --クレジットカード情報(顧客）
     ,
     tbpromotion --プロモーションマスタ
     ,
     hanyo_attr --汎用属性（通信経路）
     --2018/4/20 add 芹澤 変更0261 start
     ,
     (
          select keirokbn.diorderid,
               keirokbn.keiro,
               hanyo_web.attr1,
               hanyo_web.attr3,
               hanyo_web.attr4,
               hanyo_web.attr5
          from keirokbn
          inner join hanyo_attr hanyo_web on hanyo_web.attr1 = 'WEB' || '-' || keirokbn.keiro
               and hanyo_web.kbnmei = 'TUUSHINKEIRO'
          ) webkeiro
--2018/4/20 add 芹澤 変更0261 end
where c_tbeckesai.diorderid = tbecorder.diorderid --決済情報．受注内部id = 受注情報．受注内部id
     and c_tbeckesai.c_dicardid = c_tbecusercard.c_dicardid(+) --決済情報．クレジットカード内部id = クレジットカード情報(顧客）．クレジットカード内部id
     and tbecorder.dipromid = tbpromotion.dipromid(+) --受注情報．プロモーションid = プロモーションマスタ．プロモーションid
     and hanyo_attr.attr1(+) = tbecorder.c_dsuketsuketelcompanycd --汎用属性（通信経路）.属性1（テレマ会社コード） = 受注情報．受付担当者テレマ会社コード
     and hanyo_attr.kbnmei(+) = 'TUUSHINKEIRO'
     and c_tbeckesai.dicancel = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
     and tbecorder.diorderid = webkeiro.diorderid(+)
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
hanyorowid::varchar(1) as hanyorowid,
current_timestamp()::timestamp_ntz(9) as inserted_date,
'etl_batch'::varchar(100) as inserted_by ,
current_timestamp::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
from transformed
)
select * from final