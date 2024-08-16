{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}


with kesai_h_data_mart_sub_old as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_sub_old') }}
),
cit80saleh_ikou as(
    select * from {{ source('jpdcledw_integration', 'cit80saleh_ikou') }} 
),
tbechenpinriyu as(
    select * from {{ ref('jpndclitg_integration__tbechenpinriyu') }}
),
conv_mst_smkeiroid as(
    select * from {{ source('jpdcledw_integration', 'conv_mst_smkeiroid') }} 
),
kesai_h_data_mart_sub as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_sub') }}
),
c_tbecinquirekesai as(
    select * from {{ ref('jpndclitg_integration__c_tbecinquirekesai') }}
),
c_tbecinquire as(
    select * from {{ ref('jpndclitg_integration__c_tbecinquire') }}
),
kesai_h_data_mart_sub_old_chsi as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_sub_old_chsi') }}
),
union1 as(
SELECT 
    old_1.saleno as saleno_key,
	old_1.saleno  as saleno,
	old_1.juchkbn as juchkbn,
	old_1.juchym as juchym,
	old_1.juchdate as juchdate,
	old_1.juchquarter as juchquarter,
	old_1.juchjigyoki as juchjigyoki,
	cit80.kokyano as kokyano,
	old_1.torikeikbn as torikeikbn,
	old_1.cancelflg as cancelflg,
	old_1.hanrocode as hanrocode,
	old_1.syohanrobunname as syohanrobunname,
	old_1.chuhanrobunname as chuhanrobunname,
	old_1.daihanrobunname as daihanrobunname,
	old_1.mediacode as mediacode,
	old_1.soryo as soryo,
	old_1.tax as tax,
	old_1.sogokei as sogokei,
	old_1.tenpocode as tenpocode,
	old_1.shukaym as shukaym,
	old_1.shukadate as shukadate,
	old_1.shukaquarter as shukaquarter,
	old_1.shukajigyoki as shukajigyoki,
	old_1.zipcode as zipcode,
	old_1.todofukencode as todofukencode,
	old_1.riyopoint as riyopoint,
	old_1.happenpoint as happenpoint,
	old_1.kessaikbn as kessaikbn,
	old_1.cardcorpcode as cardcorpcode,
	old_1.henreasoncode as henreasoncode,
	old_1.motoinsertid as motoinsertid,
	old_1.motoinsertdate as motoinsertdate,
	old_1.motoupdatedate as motoupdatedate,
	old_1.insertdate as insertdate,
	old_1.inserttime as inserttime,
	old_1.insertid as insertid,
	old_1.updatedate as updatedate,
	old_1.updatetime as updatetime,
	old_1.updateid as updateid,
	old_1.rank as rank,
	old_1.dispsaleno as dispsaleno,
	old_1.kesaiid as kesaiid,
	old_1.ordercode as ordercode,
	old_1.maker as maker,
	old_1.todofuken_code as todofuken_code,
	COALESCE(hen.dshenpinriyu, 'DUMMY'::CHARACTER VARYING) AS henreasonname,
	0 AS uketsukeusrid,
	'DUMMY' AS uketsuketelcompanycd,
	CASE 
		WHEN old_1.daihanrobunname::TEXT = '直営・百貨店'::TEXT
			THEN conv_tnp.smkeiroid
		ELSE COALESCE(conv_tnp_igai.smkeiroid, 1)
		END AS smkeiroid,
	0 AS dipromid,
	'DUMMY' AS saleno_trm,
	0 AS dicollectprc,
	0 AS ditoujitsuhaisoprc,
	0 AS didiscountall,
	0 AS c_didiscountprc,
	0 AS point_exchange,
	'DUMMY' AS logincode,
	CASE 
		WHEN old_1.shukadate <> 0::NUMERIC
			THEN '1060'::TEXT
		WHEN CASE 
				WHEN cit80.torikeikbn::TEXT = '03'::TEXT
					OR cit80.torikeikbn IS NULL
					AND '03' IS NULL
					THEN 0::NUMERIC
				ELSE old_1.shukadate_p
				END <> 0::NUMERIC
			THEN '1060'::TEXT
		ELSE '1010'::TEXT
		END::CHARACTER VARYING AS shukkasts,
	'DUMMY' AS divouchercode,
	0 AS ditaxrate,
	0 AS diseikyuremain,
	'DUMMY' AS dinyukinsts,
	'DUMMY' AS dicardnyukinsts,
	0 AS DISOKOID,
	0 AS dihaisokeitai,
	CASE 
		WHEN cit80.torikeikbn::TEXT = '03'::TEXT
			OR cit80.torikeikbn IS NULL
			AND '03' IS NULL
			THEN 0::NUMERIC
		ELSE old_1.shukadate_p
		END AS shukadate_e,
	'1' AS kakokbn
FROM kesai_h_data_mart_sub_old old_1
LEFT JOIN cit80saleh_ikou cit80 ON old_1.dispsaleno::TEXT = cit80.saleno::TEXT
	AND old_1.maker = '1'::char
LEFT JOIN tbechenpinriyu hen ON old_1.henreasoncode::TEXT = hen.dihenpinriyuid::TEXT
	AND hen.dielimflg::TEXT = '0'::TEXT
LEFT JOIN conv_mst_smkeiroid conv_tnp_igai ON old_1.hanrocode::TEXT = conv_tnp_igai.hanrocode::TEXT
	AND old_1.syohanrobunname::TEXT = conv_tnp_igai.syohanrobunname::TEXT
	AND old_1.chuhanrobunname::TEXT = conv_tnp_igai.chuhanrobunname::TEXT
	AND old_1.daihanrobunname::TEXT = conv_tnp_igai.daihanrobunname::TEXT
LEFT JOIN conv_mst_smkeiroid conv_tnp ON old_1.tenpocode::TEXT = conv_tnp.tenpocode::TEXT
WHERE COALESCE(old_1.cancelflg, '0'::CHARACTER VARYING)::TEXT <> '1'::TEXT


),
union2 as( 

SELECT 
    NVL(new_1.saleno::TEXT, '')::CHARACTER VARYING as saleno_key,
	new_1.saleno  as saleno,
	new_1.juchkbn as juchkbn,
	new_1.juchym::INTEGER  as juchym,
	new_1.juchdate as juchdate,
	new_1.juchquarter as juchquarter,
	new_1.juchjigyoki as juchjigyoki,
	new_1.kokyano as kokyano,
	new_1.torikeikbn as torikeikbn,
	new_1.cancelflg as cancelflg,
	new_1.hanrocode as hanrocode,
	new_1.syohanrobunname as syohanrobunname,
	new_1.chuhanrobunname as chuhanrobunname,
	new_1.daihanrobunname as daihanrobunname,
	new_1.mediacode as mediacode,
	new_1.soryo as soryo,
	new_1.tax as tax,
	new_1.sogokei as sogokei,
	new_1.tenpocode as tenpocode,
	new_1.shukaym::INTEGER as shukaym,
	new_1.shukadate as shukadate,
	new_1.shukaquarter as shukaquarter,
	new_1.shukajigyoki as shukajigyoki,
	new_1.zipcode as zipcode,
	new_1.todofukencode as todofukencode,
	new_1.riyopoint as riyopoint,
	new_1.happenpoint as happenpoint,
	new_1.kessaikbn as kessaikbn,
	new_1.cardcorpcode as cardcorpcode,
	new_1.henreasoncode as henreasoncode,
	new_1.motoinsertid as motoinsertid,
	new_1.motoinsertdate as motoinsertdate,
	new_1.motoupdatedate as motoupdatedate,
	new_1.insertdate as insertdate,
	new_1.inserttime as inserttime,
	new_1.insertid as insertid,
	new_1.updatedate as updatedate,
	new_1.updatetime as updatetime,
	new_1.updateid as updateid,
	new_1.rank::CHARACTER VARYING as rank,
	new_1.dispsaleno as dispsaleno,
	new_1.kesaiid as kesaiid,
	new_1.ordercode as ordercode,
	new_1.maker::CHARACTER VARYING  as maker,
	new_1.todofuken_code as todofuken_code,
	new_1.henreasonname as henreasonname,
	new_1.uketsukeusrid as uketsukeusrid,
	new_1.uketsuketelcompanycd as uketsuketelcompanycd,
	new_1.smkeiroid as smkeiroid,
	new_1.dipromid as dipromid,
	new_1.saleno_trm as saleno_trm,
	new_1.dicollectprc as dicollectprc,
	new_1.ditoujitsuhaisoprc as ditoujitsuhaisoprc,
	new_1.didiscountall as didiscountall,
	new_1.c_didiscountprc as c_didiscountprc,
	new_1.point_exchange as point_exchange,
	new_1.logincode as logincode,
	new_1.shukkasts as shukkasts,
	new_1.divouchercode as divouchercode,
	new_1.ditaxrate as ditaxrate,
	new_1.diseikyuremain as diseikyuremain,
	new_1.dinyukinsts as dinyukinsts,
	new_1.dicardnyukinsts as dicardnyukinsts,
	new_1.disokoid as disokoid,
	new_1.dihaisokeitai as dihaisokeitai,
	CASE 
		WHEN substring(new_1.saleno, 1, 1) = 'H'
			THEN to_char(iq.c_dihenpinkakuteidt, 'yyyymmdd')::NUMERIC
		ELSE new_1.shukadate
		END AS shukadate_e,
	'0' AS kakokbn
FROM kesai_h_data_mart_sub new_1
LEFT JOIN c_tbecinquirekesai iq_kesai ON SUBSTRING(new_1.saleno, 2, 10) = iq_kesai.C_DIINQUIREKESAIID
LEFT JOIN c_tbecinquire iq ON iq.diinquireid = iq_kesai.diinquireid


),

union3 as(

SELECT 
    (NVL(old_chsi.saleno::TEXT, '') || '調整行DUMMY'::TEXT)::CHARACTER VARYING as saleno_key,
	old_chsi.saleno  as saleno,
	old_chsi.juchkbn as juchkbn,
	old_chsi.juchym as juchym,
	old_chsi.juchdate as juchdate,
	old_chsi.juchquarter as juchquarter,
	old_chsi.juchjigyoki as juchjigyoki,
	old_chsi.kokyano as kokyano,
	old_chsi.torikeikbn as torikeikbn,
	old_chsi.cancelflg as cancelflg,
	old_chsi.hanrocode as hanrocode,
	old_chsi.syohanrobunname as syohanrobunname,
	old_chsi.chuhanrobunname as chuhanrobunname,
	old_chsi.daihanrobunname as daihanrobunname,
	old_chsi.mediacode as mediacode,
	0  as soryo,
	0 as tax,
	0  as sogokei,
	old_chsi.tenpocode as tenpocode,
	old_chsi.shukaym as shukaym,
	old_chsi.shukadate as shukadate,
	old_chsi.shukaquarter as shukaquarter,
	old_chsi.shukajigyoki as shukajigyoki,
	old_chsi.zipcode as zipcode,
	old_chsi.todofukencode as todofukencode,
	0  as riyopoint,
	0  as happenpoint,
	old_chsi.kessaikbn as kessaikbn,
	old_chsi.cardcorpcode as cardcorpcode,
	old_chsi.henreasoncode as henreasoncode,
	old_chsi.motoinsertid as motoinsertid,
	old_chsi.motoinsertdate as motoinsertdate,
	old_chsi.motoupdatedate as motoupdatedate,
	old_chsi.insertdate as insertdate,
	old_chsi.inserttime as inserttime,
	old_chsi.insertid as insertid,
	old_chsi.updatedate as updatedate,
	old_chsi.updatetime as updatetime,
	old_chsi.updateid as updateid,
	old_chsi.rank as rank,
	old_chsi.dispsaleno as dispsaleno,
	old_chsi.kesaiid as kesaiid,
	old_chsi.ordercode as ordercode,
	'3'  as maker,
	old_chsi.todofuken_code as todofuken_code,
	old_chsi.henreasonname as henreasonname,
	0  as uketsukeusrid,
	'DUMMY'  as uketsuketelcompanycd,
	old_chsi.smkeiroid as smkeiroid,
	0  as dipromid,
	'DUMMY'  as saleno_trm,
	0  as dicollectprc,
	0  as ditoujitsuhaisoprc,
	0  as didiscountall,
	0  as c_didiscountprc,
	0  as point_exchange,
	'DUMMY'  as logincode,
	old_chsi.shukkasts as shukkasts,
	'DUMMY' as divouchercode,
	0  as ditaxrate,
	0  as diseikyuremain,
	'DUMMY'  as dinyukinsts,
	'DUMMY' as dicardnyukinsts,
	0  as disokoid,
	0  as dihaisokeitai,
	old_chsi.shukadate_p AS shukadate_e,
	'1' AS kakokbn
FROM kesai_h_data_mart_sub_old_chsi old_chsi


),
transformed as(
    select * from union1
    union all
    select * from union2
    union all
    select * from union3
),
final as(
    select 
        saleno_key::varchar(44) as saleno_key,
        saleno::varchar(63) as saleno,
        juchkbn::varchar(3) as juchkbn,
        juchym::number(18,0) as juchym,
        juchdate::number(18,0) as juchdate,
        juchquarter::varchar(3) as juchquarter,
        juchjigyoki::varchar(135) as juchjigyoki,
        kokyano::varchar(68) as kokyano,
        torikeikbn::varchar(3) as torikeikbn,
        cancelflg::varchar(135) as cancelflg,
        hanrocode::varchar(135) as hanrocode,
        syohanrobunname::varchar(135) as syohanrobunname,
        chuhanrobunname::varchar(135) as chuhanrobunname,
        daihanrobunname::varchar(135) as daihanrobunname,
        mediacode::varchar(8) as mediacode,
        soryo::number(18,0) as soryo,
        tax::number(18,0) as tax,
        sogokei::number(18,0) as sogokei,
        tenpocode::varchar(8) as tenpocode,
        shukaym::number(18,0) as shukaym,
        shukadate::number(18,0) as shukadate,
        shukaquarter::varchar(3) as shukaquarter,
        shukajigyoki::varchar(135) as shukajigyoki,
        zipcode::varchar(15) as zipcode,
        todofukencode::varchar(15) as todofukencode,
        riyopoint::number(18,0) as riyopoint,
        happenpoint::number(18,0) as happenpoint,
        kessaikbn::varchar(8) as kessaikbn,
        cardcorpcode::varchar(135) as cardcorpcode,
        henreasoncode::varchar(135) as henreasoncode,
        motoinsertid::varchar(135) as motoinsertid,
        motoinsertdate::number(18,0) as motoinsertdate,
        motoupdatedate::number(18,0) as motoupdatedate,
        insertdate::number(18,0) as insertdate,
        inserttime::number(18,0) as inserttime,
        insertid::varchar(15) as insertid,
        updatedate::number(18,0) as updatedate,
        updatetime::number(18,0) as updatetime,
        updateid::varchar(9) as updateid,
        rank::varchar(135) as rank,
        dispsaleno::varchar(62) as dispsaleno,
        kesaiid::varchar(62) as kesaiid,
        ordercode::varchar(20) as ordercode,
        maker::varchar(135) as maker,
        todofuken_code::varchar(135) as todofuken_code,
        henreasonname::varchar(48) as henreasonname,
        uketsukeusrid::number(18,0) as uketsukeusrid,
        uketsuketelcompanycd::varchar(8) as uketsuketelcompanycd,
        smkeiroid::number(18,0) as smkeiroid,
        dipromid::number(18,0) as dipromid,
        saleno_trm::varchar(63) as saleno_trm,
        dicollectprc::number(18,0) as dicollectprc,
        ditoujitsuhaisoprc::number(18,0) as ditoujitsuhaisoprc,
        didiscountall::number(18,0) as didiscountall,
        c_didiscountprc::number(18,0) as c_didiscountprc,
        point_exchange::number(18,0) as point_exchange,
        logincode::varchar(48) as logincode,
        shukkasts::varchar(6) as shukkasts,
        divouchercode::varchar(768) as divouchercode,
        ditaxrate::number(18,0) as ditaxrate,
        diseikyuremain::number(18,0) as diseikyuremain,
        dinyukinsts::varchar(8) as dinyukinsts,
        dicardnyukinsts::varchar(8) as dicardnyukinsts,
        disokoid::number(18,0) as disokoid,
        dihaisokeitai::number(18,0) as dihaisokeitai,
        shukadate_e::number(18,0) as shukadate_e,
        kakokbn::varchar(2) as kakokbn,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by ,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final