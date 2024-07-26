with cit80saleh as (
select * from dev_dna_core.jpdcledw_integration.cit80saleh
),
cit81salem as (
select * from dev_dna_core.jpdcledw_integration.cit81salem
),
transformed as (
select
c80.saleno
, c80.juchkbn
, c80.torikeikbn
,substring(c80.juchdate,1,6) juchym
, c80.juchdate
,substring(c80.shukadate,1,6) shukaym
, c80.shukadate
,decode(c80.kakokbn,'1',
case c80.kaisha
when '000' then '01'
when '001' then '02'
else            '03'
end,
case c80.smkeiroid
when 6 then '02'
else        '01'
end
) channel
,decode(c80.kakokbn,'1',
case c80.kaisha
when '000' then '01 : 通販'
when '001' then '02 : 社内販売'
else            '03 : 職域販売'
end,
case c80.smkeiroid
when 6 then '02 : 社内販売'
else        '01 : 通販'
end
) channelcname
, c81.gyono
, c81.itemcode
, c81.itemcode_hanbai
, c81.suryo
, c81.hensu
, c81.meisainukikingaku
, c80.daihanrobunname
, c80.chuhanrobunname
, c80.syohanrobunname
, c80.kokyano
, c80.mediacode
, c80.dipromid
, c80.kaisha
, c80.kakokbn
, c81.juch_shur
, c81.anbunmeisainukikingaku
from
cit80saleh c80
inner join cit81salem c81
on (c81.saleno = c80.saleno)
where
c80.cancelflg = 0
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
