{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}


with kesai_h_data_mart_sub_old as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_H_DATA_MART_SUB_OLD
),
cit80saleh_ikou as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT80SALEH_IKOU
),
tbechenpinriyu as(
    select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECHENPINRIYU
),
conv_mst_smkeiroid as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CONV_MST_SMKEIROID
),
kesai_h_data_mart_sub_kizuna as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_H_DATA_MART_SUB_KIZUNA
),
c_tbecinquirekesai as(
    select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECINQUIREKESAI
),
c_tbecinquire as(
    select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECINQUIRE
),
kesai_h_data_mart_sub_old_chsi as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KESAI_H_DATA_MART_SUB_OLD_CHSI
),
union1 as(
SELECT old_1.saleno AS TEXT,
	old_1.saleno AS error_msg,
	old_1.juchkbn,
	old_1.juchym,
	old_1.juchdate,
	old_1.juchquarter,
	old_1.juchjigyoki,
	cit80.kokyano,
	old_1.torikeikbn,
	old_1.cancelflg,
	old_1.hanrocode,
	old_1.syohanrobunname,
	old_1.chuhanrobunname,
	old_1.daihanrobunname,
	old_1.mediacode,
	old_1.soryo,
	old_1.tax,
	old_1.sogokei,
	old_1.tenpocode,
	old_1.shukaym,
	old_1.shukadate,
	old_1.shukaquarter,
	old_1.shukajigyoki,
	old_1.zipcode,
	old_1.todofukencode,
	old_1.riyopoint,
	old_1.happenpoint,
	old_1.kessaikbn,
	old_1.cardcorpcode,
	old_1.henreasoncode,
	old_1.motoinsertid,
	old_1.motoinsertdate,
	old_1.motoupdatedate,
	old_1.insertdate,
	old_1.inserttime,
	old_1.insertid,
	old_1.updatedate,
	old_1.updatetime,
	old_1.updateid,
	old_1.rank,
	old_1.dispsaleno,
	old_1.kesaiid,
	old_1.ordercode,
	old_1.maker,
	old_1.todofuken_code,
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
	0 AS sokoid,
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

SELECT NVL(new_1.saleno::TEXT, '')::CHARACTER VARYING AS TEXT,
	new_1.saleno AS error_msg,
	new_1.juchkbn,
	new_1.juchym::INTEGER AS juchym,
	new_1.juchdate,
	new_1.juchquarter,
	new_1.juchjigyoki,
	new_1.kokyano,
	new_1.torikeikbn,
	new_1.cancelflg,
	new_1.hanrocode,
	new_1.syohanrobunname,
	new_1.chuhanrobunname,
	new_1.daihanrobunname,
	new_1.mediacode,
	new_1.soryo,
	new_1.tax,
	new_1.sogokei,
	new_1.tenpocode,
	new_1.shukaym::INTEGER AS shukaym,
	new_1.shukadate,
	new_1.shukaquarter,
	new_1.shukajigyoki,
	new_1.zipcode,
	new_1.todofukencode,
	new_1.riyopoint,
	new_1.happenpoint,
	new_1.kessaikbn,
	new_1.cardcorpcode,
	new_1.henreasoncode,
	new_1.motoinsertid,
	new_1.motoinsertdate,
	new_1.motoupdatedate,
	new_1.insertdate,
	new_1.inserttime,
	new_1.insertid,
	new_1.updatedate,
	new_1.updatetime,
	new_1.updateid,
	new_1.rank::CHARACTER VARYING AS rank,
	new_1.dispsaleno,
	new_1.kesaiid,
	new_1.ordercode,
	new_1.maker::CHARACTER VARYING AS maker,
	new_1.todofuken_code,
	new_1.henreasonname,
	new_1.uketsukeusrid,
	new_1.uketsuketelcompanycd,
	new_1.smkeiroid,
	new_1.dipromid,
	new_1.saleno_trm,
	new_1.dicollectprc,
	new_1.ditoujitsuhaisoprc,
	new_1.didiscountall,
	new_1.c_didiscountprc,
	new_1.point_exchange,
	new_1.logincode,
	new_1.shukkasts,
	new_1.divouchercode,
	new_1.ditaxrate,
	new_1.diseikyuremain,
	new_1.dinyukinsts,
	new_1.dicardnyukinsts,
	new_1.disokoid AS sokoid,
	new_1.dihaisokeitai,
	CASE 
		WHEN substring(new_1.saleno, 1, 1) = 'H'
			THEN to_char(iq.c_dihenpinkakuteidt, 'yyyymmdd')::NUMERIC
		ELSE new_1.shukadate
		END AS shukadate_e,
	'0' AS kakokbn
FROM kesai_h_data_mart_sub_kizuna new_1
LEFT JOIN c_tbecinquirekesai iq_kesai ON SUBSTRING(new_1.saleno, 2, 10) = iq_kesai.C_DIINQUIREKESAIID
LEFT JOIN c_tbecinquire iq ON iq.diinquireid = iq_kesai.diinquireid


),

union3 as(

SELECT (NVL(old_chsi.saleno::TEXT, '') || '調整行DUMMY'::TEXT)::CHARACTER VARYING AS TEXT,
	old_chsi.saleno AS error_msg,
	old_chsi.juchkbn,
	old_chsi.juchym,
	old_chsi.juchdate,
	old_chsi.juchquarter,
	old_chsi.juchjigyoki,
	old_chsi.kokyano,
	old_chsi.torikeikbn,
	old_chsi.cancelflg,
	old_chsi.hanrocode,
	old_chsi.syohanrobunname,
	old_chsi.chuhanrobunname,
	old_chsi.daihanrobunname,
	old_chsi.mediacode,
	0 AS soryo,
	0 AS tax,
	0 AS sogokei,
	old_chsi.tenpocode,
	old_chsi.shukaym,
	old_chsi.shukadate,
	old_chsi.shukaquarter,
	old_chsi.shukajigyoki,
	old_chsi.zipcode,
	old_chsi.todofukencode,
	0 AS riyopoint,
	0 AS happenpoint,
	old_chsi.kessaikbn,
	old_chsi.cardcorpcode,
	old_chsi.henreasoncode,
	old_chsi.motoinsertid,
	old_chsi.motoinsertdate,
	old_chsi.motoupdatedate,
	old_chsi.insertdate,
	old_chsi.inserttime,
	old_chsi.insertid,
	old_chsi.updatedate,
	old_chsi.updatetime,
	old_chsi.updateid,
	old_chsi.rank,
	old_chsi.dispsaleno,
	old_chsi.kesaiid,
	old_chsi.ordercode,
	'3' AS maker,
	old_chsi.todofuken_code,
	old_chsi.henreasonname,
	0 AS uketsukeusrid,
	'DUMMY' AS uketsuketelcompanycd,
	old_chsi.smkeiroid,
	0 AS dipromid,
	'DUMMY' AS saleno_trm,
	0 AS dicollectprc,
	0 AS ditoujitsuhaisoprc,
	0 AS didiscountall,
	0 AS c_didiscountprc,
	0 AS point_exchange,
	'DUMMY' AS logincode,
	old_chsi.shukkasts,
	'DUMMY' AS divouchercode,
	0 AS ditaxrate,
	0 AS diseikyuremain,
	'DUMMY' AS dinyukinsts,
	'DUMMY' AS dicardnyukinsts,
	0 AS sokoid,
	0 AS dihaisokeitai,
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
)
select * from transformed