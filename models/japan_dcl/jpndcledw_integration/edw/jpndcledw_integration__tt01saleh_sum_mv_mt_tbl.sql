WITH tt01saleh_uri_mv_mt
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tt01saleh_uri_mv_mt
  ),
tt01saleh_hen_mv_mt
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tt01saleh_hen_mv_mt
  ),
transformed
AS (
  SELECT
        SALENO   AS SALENO
        ,JUCHKBN  AS JUCHKBN
        ,JUCHDATE  AS JUCHDATE
        ,KOKYANO  AS KOKYANO
        ,HANROCODE  AS HANROCODE
        ,SYOHANROBUNNAME AS SYOHANROBUNNAME
        ,CHUHANROBUNNAME AS CHUHANROBUNNAME
        ,DAIHANROBUNNAME AS DAIHANROBUNNAME
        ,MEDIACODE  AS MEDIACODE
        ,KESSAIKBN  AS KESSAIKBN
        ,SORYO   AS SORYO
        ,TAX   AS TAX
        ,SOGOKEI  AS SOGOKEI
        ,CARDCORPCODE  AS CARDCORPCODE
        ,HENREASONCODE  AS HENREASONCODE
        ,CANCELFLG  AS CANCELFLG
        ,INSERTDATE  AS INSERTDATE
        ,INSERTTIME  AS INSERTTIME
        ,INSERTID  AS INSERTID
        ,UPDATEDATE  AS UPDATEDATE
        ,UPDATETIME  AS UPDATETIME
        ,ZIPCODE  AS ZIPCODE
        ,TODOFUKENCODE  AS TODOFUKENCODE
        ,HAPPENPOINT  AS HAPPENPOINT
        ,RIYOPOINT  AS RIYOPOINT
        ,SHUKKASTS  AS SHUKKASTS
        ,TORIKEIKBN  AS TORIKEIKBN
        ,TENPOCODE  AS TENPOCODE
        ,SHUKADATE  AS SHUKADATE
        ,RANK   AS RANK
        ,DISPSALENO  AS DISPSALENO
        ,KESAIID  AS KESAIID
        ,ORDERCODE  AS ORDERCODE
        ,cast(HENREASONNAME as varchar)  AS HENREASONNAME
        ,UKETSUKEUSRID  AS UKETSUKEUSRID
        ,UKETSUKETELCOMPANYCD AS UKETSUKETELCOMPANYCD
        ,SMKEIROID  AS SMKEIROID
        ,DIPROMID  AS DIPROMID
        ,DICOLLECTPRC
        ,DITOUJITSUHAISOPRC
        ,DIDISCOUNTALL
        ,POINT_Exchange
        --2017/11/27 ADD 柳本 変更0195 START
        ,LASTUPDUSRID AS LASTUPDUSRID
        --2017/11/27 ADD 柳本 変更0195 END
        ,1 as maker
        ,null as SALEHROWID 
        --BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        --DnA側でデータマートを作成するため廃止
        --,JOIN_REC_UPDDATE AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
        --END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
  FROM tt01saleh_uri_mv_mt
  UNION ALL
  SELECT
        SALENO   AS SALENO
        ,JUCHKBN  AS JUCHKBN
        ,JUCHDATE  AS JUCHDATE
        ,KOKYANO  AS KOKYANO
        ,HANROCODE  AS HANROCODE
        ,SYOHANROBUNNAME AS SYOHANROBUNNAME
        ,CHUHANROBUNNAME AS CHUHANROBUNNAME
        ,DAIHANROBUNNAME AS DAIHANROBUNNAME
        ,MEDIACODE  AS MEDIACODE
        ,KESSAIKBN  AS KESSAIKBN
        ,SORYO   AS SORYO
        ,TAX   AS TAX
        ,SOGOKEI  AS SOGOKEI
        ,CARDCORPCODE  AS CARDCORPCODE
        ,HENREASONCODE  AS HENREASONCODE
        ,cast(CANCELFLG as varchar(15)) AS CANCELFLG
        ,INSERTDATE  AS INSERTDATE
        ,INSERTTIME  AS INSERTTIME
        ,INSERTID  AS INSERTID
        ,UPDATEDATE  AS UPDATEDATE
        ,UPDATETIME  AS UPDATETIME
        ,ZIPCODE  AS ZIPCODE
        ,TODOFUKENCODE  AS TODOFUKENCODE
        ,HAPPENPOINT  AS HAPPENPOINT
        ,RIYOPOINT  AS RIYOPOINT
        ,SHUKKASTS  AS SHUKKASTS
        ,TORIKEIKBN  AS TORIKEIKBN
        ,TENPOCODE  AS TENPOCODE
        ,SHUKADATE  AS SHUKADATE
        ,RANK   AS RANK
        ,DISPSALENO  AS DISPSALENO
        ,KESAIID  AS KESAIID
        ,ORDERCODE  AS ORDERCODE
        ,cast(HENREASONNAME as varchar) AS HENREASONNAME
        ,UKETSUKEUSRID  AS UKETSUKEUSRID
        ,UKETSUKETELCOMPANYCD AS UKETSUKETELCOMPANYCD
        ,SMKEIROID  AS SMKEIROID
        ,DIPROMID  AS DIPROMID
        ,DICOLLECTPRC
        ,DITOUJITSUHAISOPRC
        ,DIDISCOUNTALL
        ,POINT_Exchange
        --2017/11/27 ADD 柳本 変更0195 START
        ,LASTUPDUSRID AS LASTUPDUSRID
        --2017/11/27 ADD 柳本 変更0195 END
        ,2 as maker
        ,null as SALEHROWID
        --BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
        --DnA側でデータマートを作成するため廃止
        --,JOIN_REC_UPDDATE AS JOIN_REC_UPDDATE     --結合レコード更新日時(JJ連携の差分抽出に使用)
        --END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
  FROM tt01saleh_hen_mv_mt
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        juchkbn::varchar(3) as juchkbn,
        juchdate::number(18,0) as juchdate,
        kokyano::varchar(30) as kokyano,
        hanrocode::varchar(60) as hanrocode,
        syohanrobunname::varchar(60) as syohanrobunname,
        chuhanrobunname::varchar(60) as chuhanrobunname,
        daihanrobunname::varchar(60) as daihanrobunname,
        mediacode::varchar(8) as mediacode,
        kessaikbn::varchar(3) as kessaikbn,
        soryo::number(18,0) as soryo,
        tax::number(18,0) as tax,
        sogokei::number(18,0) as sogokei,
        cardcorpcode::varchar(60) as cardcorpcode,
        henreasoncode::varchar(60) as henreasoncode,
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
        dispsaleno::varchar(80) as dispsaleno,
        kesaiid::number(10,0) as kesaiid,
        ordercode::varchar(18) as ordercode,
        henreasonname::varchar(48) as henreasonname,
        uketsukeusrid::number(18,0) as uketsukeusrid,
        uketsuketelcompanycd::varchar(5) as uketsuketelcompanycd,
        smkeiroid::number(18,0) as smkeiroid,
        dipromid::number(10,0) as dipromid,
        dicollectprc::number(18,0) as dicollectprc,
        ditoujitsuhaisoprc::number(18,0) as ditoujitsuhaisoprc,
        didiscountall::number(18,0) as didiscountall,
        point_exchange::number(18,0) as point_exchange,
        lastupdusrid::number(10,0) as lastupdusrid,
        maker::number(18,0) as maker,
        salehrowid::varchar(1) as salehrowid
  FROM transformed
)
SELECT *
FROM final