WITH tt01saleh_sum_mv_mt
AS (
  SELECT *
  FROM  {{ ref('jpndcledw_integration__tt01saleh_sum_mv_mt') }}
  ),
hanyo_attr
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__hanyo_attr') }}
  ),
cim05opera
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__cim05opera') }}
  ),
transformed
AS (
  SELECT
        TT01SALEH_SUM.SALENO			AS	SALENO
        ,TT01SALEH_SUM.JUCHKBN			AS	JUCHKBN
        ,SUBSTRING(TT01SALEH_SUM.JUCHDATE,1,6)	AS	JUCHYM
        ,TT01SALEH_SUM.JUCHDATE			AS	JUCHDATE
        ,DECODE(SUBSTRING(TO_CHAR(TT01SALEH_SUM.JUCHDATE),5,2)
            ,'01','Q2','02','Q3','03','Q3','04','Q3'
            ,'05','Q4','06','Q4','07','Q4','08','Q1'
            ,'09','Q1','10','Q1','11','Q2','12','Q2'
            )	AS	JUCHQUARTER
        ,HANYO_ATTR.ATTR3			AS	JUCHJIGYOKI
        ,TT01SALEH_SUM.KOKYANO			AS	KOKYANO
        ,TT01SALEH_SUM.TORIKEIKBN		AS	TORIKEIKBN
        ,TT01SALEH_SUM.CANCELFLG		AS	CANCELFLG
        ,TT01SALEH_SUM.HANROCODE		AS	HANROCODE
        ,TT01SALEH_SUM.SYOHANROBUNNAME	AS	SYOHANROBUNNAME
        ,TT01SALEH_SUM.CHUHANROBUNNAME	AS	CHUHANROBUNNAME
        ,TT01SALEH_SUM.DAIHANROBUNNAME	AS	DAIHANROBUNNAME
        ,TT01SALEH_SUM.MEDIACODE		AS	MEDIACODE
        ,TT01SALEH_SUM.SORYO			AS	SORYO
        ,TT01SALEH_SUM.TAX			AS	TAX
        ,TT01SALEH_SUM.SOGOKEI			AS	SOGOKEI
        ,TT01SALEH_SUM.TENPOCODE		AS	TENPOCODE
        ,SUBSTRING(TT01SALEH_SUM.SHUKADATE,1,6)		AS	SHUKAYM
        ,TT01SALEH_SUM.SHUKADATE			AS	SHUKADATE
        ,DECODE(cast(TT01SALEH_SUM.SHUKADATE as varchar),'0','Q9',DECODE(SUBSTRING(TO_CHAR(TT01SALEH_SUM.SHUKADATE),5,2)
            ,'01','Q2','02','Q3','03','Q3','04','Q3'
            ,'05','Q4','06','Q4','07','Q4','08','Q1'
            ,'09','Q1','10','Q1','11','Q2','12','Q2'
            ))	AS	SHUKAQUARTER
        ,DECODE(SHUKA.ATTR3, NULL, '99', SHUKA.ATTR3)					AS	SHUKAJIGYOKI
        ,TT01SALEH_SUM.ZIPCODE			AS	ZIPCODE
        ,TT01SALEH_SUM.TODOFUKENCODE	AS	TODOFUKENCODE
        ,TT01SALEH_SUM.RIYOPOINT		AS	RIYOPOINT
        ,TT01SALEH_SUM.HAPPENPOINT		AS	HAPPENPOINT
        ,TT01SALEH_SUM.KESSAIKBN		AS	KESSAIKBN
        ,TT01SALEH_SUM.CARDCORPCODE		AS	CARDCORPCODE
        ,TT01SALEH_SUM.HENREASONCODE		AS	HENREASONCODE
        ,TT01SALEH_SUM.INSERTID			AS	MOTOINSERTID
        ,TT01SALEH_SUM.INSERTDATE		AS	MOTOINSERTDATE
        ,TT01SALEH_SUM.UPDATEDATE		AS	MOTOUPDATEDATE
        ,TT01SALEH_SUM.INSERTDATE		AS	INSERTDATE
        ,TT01SALEH_SUM.INSERTTIME		AS	INSERTTIME
        ,'001002'						AS	INSERTID
        ,TT01SALEH_SUM.UPDATEDATE		AS	UPDATEDATE
        ,TT01SALEH_SUM.UPDATETIME		AS	UPDATETIME
        ,'001002'						AS	UPDATEID
        ,TT01SALEH_SUM.RANK				AS	RANK
        ,TT01SALEH_SUM.DISPSALENO		AS	DISPSALENO
        ,TT01SALEH_SUM.KESAIID			AS	KESAIID
        ,TT01SALEH_SUM.ORDERCODE		AS	ORDERCODE
        ,TT01SALEH_SUM.HENREASONNAME		AS	HENREASONNAME
        ,TT01SALEH_SUM.UKETSUKEUSRID		AS	UKETSUKEUSRID
        ,TT01SALEH_SUM.UKETSUKETELCOMPANYCD	AS	UKETSUKETELCOMPANYCD
        ,TT01SALEH_SUM.SMKEIROID		AS	SMKEIROID
        ,TT01SALEH_SUM.DIPROMID			AS	DIPROMID
        ,trim(TT01SALEH_SUM.SALENO)			AS	SALENO_TRM
        ,TT01SALEH_SUM.DICOLLECTPRC AS DICOLLECTPRC
        ,TT01SALEH_SUM.DITOUJITSUHAISOPRC AS DITOUJITSUHAISOPRC
        ,TT01SALEH_SUM.DIDISCOUNTALL AS DIDISCOUNTALL
        ,TT01SALEH_SUM.POINT_Exchange AS POINT_Exchange
        --2017/11/27 ADD 柳本 変更0195 START
        ,CIM05OPERA.LOGINCODE AS LOGINCODE
        --2017/11/27 ADD 柳本 変更0195 END
        ,maker as maker
        ,null AS ROWID01
        ,null AS ROWID02
        ,null AS ROWID03
        --2018/2/27 ADD 芹澤 変更0074 START
        ,TODOFUKEN.ATTR5 AS TODOFUKEN_CODE
        --2018/2/27 ADD 芹澤 変更0074 END
        --BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        --DnA側でデータマートを作成するため廃止
        --,TO_NUMBER(GREATEST(
        --    NVL(TT01SALEH_SUM.JOIN_REC_UPDDATE,0)
        --    ,TO_NUMBER(NVL(TO_CHAR(CI_DWH_MAIN.HANYO_ATTR.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --    ,TO_NUMBER(NVL(TO_CHAR(SHUKA.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --    ,TO_NUMBER(NVL(TO_CHAR(TODOFUKEN.UPDATEDATE,'YYYYMMDDHH24MISS'),0))
        --    ,NVL(CI_DWH_MAIN.CIM05OPERA.JOIN_REC_UPDDATE,0)
        --))      AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
        --END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        FROM tt01saleh_sum_mv_mt TT01SALEH_SUM
        ,hanyo_attr --汎用属性（事業期マスタ）
        ,hanyo_attr SHUKA --汎用属性（事業期マスタ）
        --2018/2/27 ADD 芹澤 変更0074 START
        ,hanyo_attr TODOFUKEN --汎用属性（都道府県マスタ）
        --2018/2/27 ADD 芹澤 変更0074 END
        --2017/11/27 ADD 柳本 変更0195 START
        ,cim05opera --オペレータ
        --2017/11/27 ADD 柳本 変更0195 END
        --汎用属性（事業期マスタ）と結合
        --    LEFT  JOIN CI_DWH_MAIN.HANYO_ATTR --汎用属性（事業期マスタ）
            --QV用売上（合算）.受注日 BETWEEN 汎用属性（事業期マスタ）.属性1（期初年月日） AND 汎用属性（事業期マスタ）.属性2（期末年月日）
        WHERE TT01SALEH_SUM.JUCHDATE BETWEEN HANYO_ATTR.ATTR1(+) AND HANYO_ATTR.ATTR2(+)
        AND HANYO_ATTR.KBNMEI(+) = 'JIGYOKI'
        --汎用属性（事業期マスタ）と結合（出荷日）
        --    LEFT  JOIN CI_DWH_MAIN.HANYO_ATTR SHUKA --汎用属性（事業期マスタ）
            --QV用売上（合算）.出荷日 BETWEEN 汎用属性（事業期マスタ）.属性1（期初年月日） AND 汎用属性（事業期マスタ）.属性2（期末年月日）
        AND TT01SALEH_SUM.SHUKADATE BETWEEN SHUKA.ATTR1(+) AND SHUKA.ATTR2(+)
        AND SHUKA.KBNMEI(+) = 'JIGYOKI'
        --2017/11/27 ADD 柳本 変更0195 START
        --オペレータと結合
        AND cast(TT01SALEH_SUM.LASTUPDUSRID as varchar) = CIM05OPERA.OPECODE(+)
        AND CIM05OPERA.CIFLG = 'NEXT'
        --2017/11/27 ADD 柳本 変更0195 END
        --2018/2/27 ADD 芹澤 変更0074 START
        --汎用属性（都道府県マスタ）と結合
        AND TT01SALEH_SUM.TODOFUKENCODE = TODOFUKEN.ATTR1(+)
        AND TODOFUKEN.KBNMEI(+)='TODOFUKEN'
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        juchkbn::varchar(3) as juchkbn,
        juchym::varchar(18) as juchym,
        juchdate::number(18,0) as juchdate,
        juchquarter::varchar(3) as juchquarter,
        juchjigyoki::varchar(60) as juchjigyoki,
        kokyano::varchar(30) as kokyano,
        torikeikbn::varchar(3) as torikeikbn,
        cancelflg::varchar(2) as cancelflg,
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
        insertid::varchar(9) as insertid,
        updatedate::number(18,0) as updatedate,
        updatetime::number(18,0) as updatetime,
        updateid::varchar(9) as updateid,
        rank::number(18,0) as rank,
        dispsaleno::varchar(60) as dispsaleno,
        kesaiid::number(10,0) as kesaiid,
        ordercode::varchar(18) as ordercode,
        henreasonname::varchar(48) as henreasonname,
        uketsukeusrid::number(18,0) as uketsukeusrid,
        uketsuketelcompanycd::varchar(5) as uketsuketelcompanycd,
        smkeiroid::number(18,0) as smkeiroid,
        dipromid::number(10,0) as dipromid,
        saleno_trm::varchar(62) as saleno_trm,
        dicollectprc::number(18,0) as dicollectprc,
        ditoujitsuhaisoprc::number(18,0) as ditoujitsuhaisoprc,
        didiscountall::number(18,0) as didiscountall,
        point_exchange::number(18,0) as point_exchange,
        logincode::varchar(48) as logincode,
        maker::number(18,0) as maker,
        rowid01::varchar(1) as rowid01,
        rowid02::varchar(1) as rowid02,
        rowid03::varchar(1) as rowid03,
        todofuken_code::varchar(60) as todofuken_code
FROM transformed
)
SELECT *
FROM final