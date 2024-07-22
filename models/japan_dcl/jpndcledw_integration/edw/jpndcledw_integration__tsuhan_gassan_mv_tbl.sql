with CIT80SALEH as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT80SALEH
),
CIT81SALEM as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT81SALEM
),
transformed as (
select
C80.SALENO
, C80.JUCHKBN
, C80.TORIKEIKBN
,SUBSTRING(C80.JUCHDATE,1,6) JUCHYM
, C80.JUCHDATE
,SUBSTRING(C80.SHUKADATE,1,6) SHUKAYM
, C80.SHUKADATE
,DECODE(C80.KAKOKBN,'1',
CASE C80.KAISHA
WHEN '000' THEN '01'
WHEN '001' THEN '02'
ELSE            '03'
END,
CASE C80.SMKEIROID
WHEN 6 THEN '02'
ELSE        '01'
END
) CHANNEL
,DECODE(C80.KAKOKBN,'1',
CASE C80.KAISHA
WHEN '000' THEN '01 : 通販'
WHEN '001' THEN '02 : 社内販売'
ELSE            '03 : 職域販売'
END,
CASE C80.SMKEIROID
WHEN 6 THEN '02 : 社内販売'
ELSE        '01 : 通販'
END
) CHANNELCNAME
, C81.GYONO
, C81.ITEMCODE
, C81.ITEMCODE_HANBAI
, C81.SURYO
, C81.HENSU
, C81.MEISAINUKIKINGAKU
, C80.DAIHANROBUNNAME
, C80.CHUHANROBUNNAME
, C80.SYOHANROBUNNAME
, C80.KOKYANO
, C80.MEDIACODE
, C80.DIPROMID
, C80.KAISHA
, C80.KAKOKBN
, C81.JUCH_SHUR
, C81.ANBUNMEISAINUKIKINGAKU
from
CIT80SALEH C80
INNER JOIN CIT81SALEM C81
ON (C81.SALENO = C80.SALENO)
where
C80.CANCELFLG = 0
),
final as 
(
select
	saleno::varchar(18) as saleno,
	juchkbn::varchar(3) as juchkbn,
	torikeikbn::varchar(3) as torikeikbn,
	juchym::varchar(18) as juchym,
	juchdate::number(18,0) as juchdate,
	shukaym::varchar(18) as shukaym,
	shukadate::number(18,0) as shukadate,
	channel::varchar(3) as channel,
	channelcname::varchar(20) as channelcname,
	gyono::number(18,0) as gyono,
	itemcode::varchar(45) as itemcode,
	itemcode_hanbai::varchar(45) as itemcode_hanbai,
	suryo::number(18,0) as suryo,
	hensu::number(18,0) as hensu,
	meisainukikingaku::number(18,0) as meisainukikingaku,
	daihanrobunname::varchar(60) as daihanrobunname,
	chuhanrobunname::varchar(60) as chuhanrobunname,
	syohanrobunname::varchar(60) as syohanrobunname,
	kokyano::varchar(30) as kokyano,
	mediacode::varchar(8) as mediacode,
	dipromid::number(18,0) as dipromid,
	kaisha::varchar(15) as kaisha,
	kakokbn::number(18,0) as kakokbn,
	juch_shur::varchar(2) as juch_shur,
	anbunmeisainukikingaku::number(18,0) as anbunmeisainukikingaku,
	current_timestamp()::timestamp_ntz(9) as inserted_date,
	'etl_batch'::varchar(100) as inserted_by ,
	current_timestamp::timestamp_ntz(9) as updated_date,
	null::varchar(100) as updated_by
from transformed
    )
select * from final
