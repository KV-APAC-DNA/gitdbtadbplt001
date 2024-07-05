with KESAI_H_DATA_MART_SUB_N_U as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_sub_n_u') }}
),
KESAI_H_DATA_MART_SUB_N_H as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_sub_n_h') }}
),
TBPERSON as(
    select * from {{ ref('jpndclitg_integration__tbperson') }}
),
HANYO_ATTR as(
        select * from {{ source('jpndcledw_integration', 'hanyo_attr') }}
),
union1 as(
    SELECT SALENO,
            JUCHKBN,
            JUCHDATE,
            KOKYANO,
            HANROCODE,
            SYOHANROBUNNAME,
            CHUHANROBUNNAME,
            DAIHANROBUNNAME,
            MEDIACODE,
            KESSAIKBN,
            SORYO,
            TAX,
            SOGOKEI,
            CARDCORPCODE,
            HENREASONCODE,
            CANCELFLG,
            INSERTDATE,
            INSERTTIME,
            INSERTID,
            UPDATEDATE,
            UPDATETIME,
            ZIPCODE,
            TODOFUKENCODE,
            HAPPENPOINT,
            RIYOPOINT,
            SHUKKASTS,
            TORIKEIKBN,
            TENPOCODE,
            SHUKADATE,
            RANK as rank,
            DISPSALENO,
            KESAIID,
            ORDERCODE,
            cast(HENREASONNAME AS VARCHAR) as HENREASONNAME,
            UKETSUKEUSRID,
            UKETSUKETELCOMPANYCD,
            SMKEIROID,
            DIPROMID,
            DICOLLECTPRC,
            DITOUJITSUHAISOPRC,
            DIDISCOUNTALL,
            C_DIDISCOUNTPRC,
            POINT_Exchange,
            LASTUPDUSRID,
            1 AS MAKER,
            DIVOUCHERCODE,
            DITAXRATE,
            DISEIKYUREMAIN,
            DINYUKINSTS,
            DICARDNYUKINSTS,
            DISOKOID,
            DIHAISOKEITAI
	FROM KESAI_H_DATA_MART_SUB_N_U
	
),
union2 as(
SELECT SALENO,
		JUCHKBN,
		JUCHDATE,
		KOKYANO,
		HANROCODE,
		SYOHANROBUNNAME,
		CHUHANROBUNNAME,
		DAIHANROBUNNAME,
		MEDIACODE,
		KESSAIKBN,
		SORYO,
		TAX,
		SOGOKEI,
		CARDCORPCODE,
		HENREASONCODE,
		CANCELFLG,
		INSERTDATE,
		INSERTTIME,
		INSERTID,
		UPDATEDATE,
		UPDATETIME,
		ZIPCODE,
		TODOFUKENCODE,
		HAPPENPOINT,
		RIYOPOINT,
		SHUKKASTS,
		TORIKEIKBN,
		TENPOCODE,
		SHUKADATE,
		RANK as rank,
		DISPSALENO,
		KESAIID,
		ORDERCODE,
		cast(HENREASONNAME AS VARCHAR) as HENREASONNAME,
		UKETSUKEUSRID,
		UKETSUKETELCOMPANYCD,
		SMKEIROID,
		DIPROMID,
		DICOLLECTPRC,
		DITOUJITSUHAISOPRC,
		DIDISCOUNTALL,
		C_DIDISCOUNTPRC,
		POINT_Exchange,
		LASTUPDUSRID,
		2 AS MAKER,
		DIVOUCHERCODE,
		DITAXRATE,
		DISEIKYUREMAIN,
		DINYUKINSTS,
		DICARDNYUKINSTS,
		DISOKOID,
		DIHAISOKEITAI
	FROM KESAI_H_DATA_MART_SUB_N_H
),

KESAI_H_UNION as(
    select * from union1
    union all
    select * from union2
),
transformed as(
    SELECT KESAI_H_UNION.SALENO AS SALENO,
	KESAI_H_UNION.JUCHKBN AS JUCHKBN,
	SUBSTRING(KESAI_H_UNION.JUCHDATE, 1, 6) AS JUCHYM,
	KESAI_H_UNION.JUCHDATE AS JUCHDATE
	/*, DECODE(TO_CHAR(CAST(KESAI_H_UNION.JUCHDATE AS NUMERIC),'MM')
          ,'01','Q2','02','Q3','03','Q3','04','Q3'
          ,'05','Q4','06','Q4','07','Q4','08','Q1'
          ,'09','Q1','10','Q1','11','Q2','12','Q2'
   )        AS JUCHQUARTER */
	,
	DECODE(TO_CHAR(TO_DATE(CAST(KESAI_H_UNION.JUCHDATE AS STRING), 'YYYYMMDD'), 'MM'), '01', 'Q2', '02', 'Q3', '03', 'Q3', '04', 'Q3', '05', 'Q4', '06', 'Q4', '07', 'Q4', '08', 'Q1', '09', 'Q1', '10', 'Q1', '11', 'Q2', '12', 'Q2') AS JUCHQUARTER,
	HANYO_ATTR.ATTR3 AS JUCHJIGYOKI,
	KESAI_H_UNION.KOKYANO AS KOKYANO,
	KESAI_H_UNION.TORIKEIKBN AS TORIKEIKBN,
	KESAI_H_UNION.CANCELFLG AS CANCELFLG,
	KESAI_H_UNION.HANROCODE AS HANROCODE,
	KESAI_H_UNION.SYOHANROBUNNAME AS SYOHANROBUNNAME,
	KESAI_H_UNION.CHUHANROBUNNAME AS CHUHANROBUNNAME,
	KESAI_H_UNION.DAIHANROBUNNAME AS DAIHANROBUNNAME,
	KESAI_H_UNION.MEDIACODE AS MEDIACODE,
	KESAI_H_UNION.SORYO AS SORYO,
	KESAI_H_UNION.TAX AS TAX,
	KESAI_H_UNION.SOGOKEI AS SOGOKEI,
	KESAI_H_UNION.TENPOCODE AS TENPOCODE,
	SUBSTRING(KESAI_H_UNION.SHUKADATE, 1, 6) AS SHUKAYM,
	KESAI_H_UNION.SHUKADATE AS SHUKADATE
	/*, DECODE(CAST(KESAI_H_UNION.SHUKADATE AS VARCHAR),'0','Q9',DECODE(TO_CHAR(CAST(KESAI_H_UNION.SHUKADATE AS NUMERIC),'MM')
          ,'01','Q2','02','Q3','03','Q3','04','Q3'
          ,'05','Q4','06','Q4','07','Q4','08','Q1'
          ,'09','Q1','10','Q1','11','Q2','12','Q2'
   ))                                          AS SHUKAQUARTER*/
	,
	DECODE(CAST(KESAI_H_UNION.SHUKADATE AS VARCHAR), '0', 'Q9', DECODE(TO_CHAR(TO_DATE(CAST(KESAI_H_UNION.SHUKADATE AS STRING), 'YYYYMMDD'), 'MM'), '01', 'Q2', '02', 'Q3', '03', 'Q3', '04', 'Q3', '05', 'Q4', '06', 'Q4', '07', 'Q4', '08', 'Q1', '09', 'Q1', '10', 'Q1', '11', 'Q2', '12', 'Q2')) AS SHUKAQUARTER,
	DECODE(SHUKA.ATTR3, NULL, '99', SHUKA.ATTR3) AS SHUKAJIGYOKI,
	KESAI_H_UNION.ZIPCODE AS ZIPCODE,
	KESAI_H_UNION.TODOFUKENCODE AS TODOFUKENCODE,
	KESAI_H_UNION.RIYOPOINT AS RIYOPOINT,
	KESAI_H_UNION.HAPPENPOINT AS HAPPENPOINT,
	KESAI_H_UNION.KESSAIKBN AS KESSAIKBN,
	KESAI_H_UNION.CARDCORPCODE AS CARDCORPCODE,
	KESAI_H_UNION.HENREASONCODE AS HENREASONCODE,
	KESAI_H_UNION.INSERTID AS MOTOINSERTID,
	KESAI_H_UNION.INSERTDATE AS MOTOINSERTDATE,
	KESAI_H_UNION.UPDATEDATE AS MOTOUPDATEDATE,
	KESAI_H_UNION.INSERTDATE AS INSERTDATE,
	KESAI_H_UNION.INSERTTIME AS INSERTTIME,
	'001002' AS INSERTID,
	KESAI_H_UNION.UPDATEDATE AS UPDATEDATE,
	KESAI_H_UNION.UPDATETIME AS UPDATETIME,
	'001002' AS UPDATEID,
	KESAI_H_UNION.RANK AS RANK,
	KESAI_H_UNION.DISPSALENO AS DISPSALENO,
	KESAI_H_UNION.KESAIID AS KESAIID,
	KESAI_H_UNION.ORDERCODE AS ORDERCODE,
	cast(KESAI_H_UNION.HENREASONNAME AS VARCHAR) AS HENREASONNAME,
	KESAI_H_UNION.UKETSUKEUSRID AS UKETSUKEUSRID,
	KESAI_H_UNION.UKETSUKETELCOMPANYCD AS UKETSUKETELCOMPANYCD,
	KESAI_H_UNION.SMKEIROID AS SMKEIROID,
	KESAI_H_UNION.DIPROMID AS DIPROMID,
	TRIM(KESAI_H_UNION.SALENO) AS SALENO_TRM,
	KESAI_H_UNION.DICOLLECTPRC AS DICOLLECTPRC,
	KESAI_H_UNION.DITOUJITSUHAISOPRC AS DITOUJITSUHAISOPRC,
	KESAI_H_UNION.DIDISCOUNTALL AS DIDISCOUNTALL,
	KESAI_H_UNION.C_DIDISCOUNTPRC AS C_DIDISCOUNTPRC,
	KESAI_H_UNION.POINT_EXCHANGE AS POINT_EXCHANGE,
	TBPERSON.DSLOGIN AS LOGINCODE,
	KESAI_H_UNION.MAKER AS MAKER,
	TODOFUKEN.ATTR5 AS TODOFUKEN_CODE,
	KESAI_H_UNION.SHUKKASTS AS SHUKKASTS,
	KESAI_H_UNION.DIVOUCHERCODE AS DIVOUCHERCODE,
	KESAI_H_UNION.DITAXRATE AS DITAXRATE,
	KESAI_H_UNION.DISEIKYUREMAIN AS DISEIKYUREMAIN,
	KESAI_H_UNION.DINYUKINSTS AS DINYUKINSTS,
	KESAI_H_UNION.DICARDNYUKINSTS AS DICARDNYUKINSTS
	--, TO_NUMBER(GREATEST(
	--       NVL(KESAI_H_UNION.JOIN_REC_UPDDATE,0)
	--     , TO_NUMBER(NVL(TO_CHAR(CI_DWH_MAIN.HANYO_ATTR.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
	--     , TO_NUMBER(NVL(TO_CHAR(SHUKA.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
	--     , TO_NUMBER(NVL(TO_CHAR(TODOFUKEN.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
	--     , TO_NUMBER(NVL(TO_CHAR(TO_DATE(NVL(CI_NEXT.TBPERSON.DSREN,CI_NEXT.TBPERSON.DSPREP),'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDDHH24MISS'),0))
	,
	KESAI_H_UNION.DISOKOID AS DISOKOID,
	KESAI_H_UNION.DIHAISOKEITAI AS DIHAISOKEITAI
FROM
    KESAI_H_UNION,
	HANYO_ATTR,
	HANYO_ATTR SHUKA,
	HANYO_ATTR TODOFUKEN,
	TBPERSON
WHERE KESAI_H_UNION.JUCHDATE BETWEEN HANYO_ATTR.ATTR1(+)
		AND HANYO_ATTR.ATTR2(+)
	AND HANYO_ATTR.KBNMEI(+) = 'JIGYOKI'
	AND KESAI_H_UNION.SHUKADATE BETWEEN SHUKA.ATTR1(+)
		AND SHUKA.ATTR2(+)
	AND SHUKA.KBNMEI(+) = 'JIGYOKI'
	AND KESAI_H_UNION.LASTUPDUSRID = TBPERSON.DIID(+)
	AND KESAI_H_UNION.TODOFUKENCODE = TODOFUKEN.ATTR1(+)
	AND TODOFUKEN.KBNMEI(+) = 'TODOFUKEN'

),
final as(
    select 
    	saleno::varchar(63) as saleno,
        juchkbn::varchar(3) as juchkbn,
        juchym::varchar(18) as juchym,
        juchdate::number(18,0) as juchdate,
        juchquarter::varchar(3) as juchquarter,
        juchjigyoki::varchar(60) as juchjigyoki,
        kokyano::varchar(30) as kokyano,
        torikeikbn::varchar(3) as torikeikbn,
        cancelflg::varchar(10) as cancelflg,
        hanrocode::varchar(60) as hanrocode,
        syohanrobunname::varchar(60) as syohanrobunname,
        chuhanrobunname::varchar(60) as chuhanrobunname,
        daihanrobunname::varchar(60) as daihanrobunname,
        mediacode::varchar(8) as mediacode,
        soryo::number(18,0) as soryo,
        tax::number(18,0) as tax,
        sogokei::number(18,0) as sogokei,
        tenpocode::varchar(8) as tenpocode,
        shukaym::varchar(18) as shukaym,
        shukadate::number(18,0) as shukadate,
        shukaquarter::varchar(3) as shukaquarter,
        shukajigyoki::varchar(60) as shukajigyoki,
        zipcode::varchar(15) as zipcode,
        todofukencode::varchar(15) as todofukencode,
        riyopoint::number(18,0) as riyopoint,
        happenpoint::number(18,0) as happenpoint,
        kessaikbn::varchar(3) as kessaikbn,
        cardcorpcode::varchar(60) as cardcorpcode,
        henreasoncode::varchar(60) as henreasoncode,
        motoinsertid::varchar(60) as motoinsertid,
        motoinsertdate::number(18,0) as motoinsertdate,
        motoupdatedate::number(18,0) as motoupdatedate,
        insertdate::number(18,0) as insertdate,
        inserttime::number(18,0) as inserttime,
        insertid::varchar(50) as insertid,
        updatedate::number(18,0) as updatedate,
        updatetime::number(18,0) as updatetime,
        updateid::varchar(50) as updateid,
        rank::number(18,0) as rank,
        dispsaleno::varchar(62) as dispsaleno,
        kesaiid::varchar(62) as kesaiid,
        ordercode::varchar(50) as ordercode,
        henreasonname::varchar(48) as henreasonname,
        uketsukeusrid::number(18,0) as uketsukeusrid,
        uketsuketelcompanycd::varchar(5) as uketsuketelcompanycd,
        smkeiroid::number(18,0) as smkeiroid,
        dipromid::number(10,0) as dipromid,
        saleno_trm::varchar(63) as saleno_trm,
        dicollectprc::number(18,0) as dicollectprc,
        ditoujitsuhaisoprc::number(18,0) as ditoujitsuhaisoprc,
        didiscountall::number(18,0) as didiscountall,
        c_didiscountprc::number(18,0) as c_didiscountprc,
        point_exchange::number(18,0) as point_exchange,
        logincode::varchar(48) as logincode,
        maker::number(18,0) as maker,
        todofuken_code::varchar(60) as todofuken_code,
        shukkasts::varchar(6) as shukkasts,
        divouchercode::varchar(768) as divouchercode,
        ditaxrate::number(3,0) as ditaxrate,
        diseikyuremain::number(18,0) as diseikyuremain,
        dinyukinsts::varchar(10) as dinyukinsts,
        dicardnyukinsts::varchar(10) as dicardnyukinsts,
        disokoid::number(10,0) as disokoid,
        dihaisokeitai::number(10,0) as dihaisokeitai
    from transformed
)
select * from final