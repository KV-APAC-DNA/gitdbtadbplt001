{{
    config
    (
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}

WITH kesai_h_data_mart_sub_old
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_h_data_mart_sub_old') }}
    ),
kesai_h_data_mart_sub_old_chsi
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_h_data_mart_sub_old_chsi') }}
    ),
cit80saleh_ikou
AS (
    SELECT *
    FROM {{source('jpdcledw_integration', 'cit80saleh_ikou') }}
    ),
conv_mst_smkeiroid
AS (
    SELECT *
    FROM {{source('jpdcledw_integration', 'conv_mst_smkeiroid') }}
    ),
kesai_h_data_mart_sub
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_h_data_mart_sub') }}
    ),
kesai_h_data_mart_sub_n
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_h_data_mart_sub_n') }}
    ),
kesai_m_data_mart_sub_n_u
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_m_data_mart_sub_n_u') }}
    ),
kesai_m_data_mart_sub_n_h
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__kesai_m_data_mart_sub_n_h') }}
    ),
tbechenpinriyu
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__tbechenpinriyu') }}
    ),
final
AS (
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
        old_1.saleno AS saleno_p,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.shukadate_p
            END AS shukadate_p,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.shohingokei
            END AS shohingokei,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.komiwarikingaku
            END AS komiwarikingaku,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.warimaenukigokei
            END AS warimaenukigokei,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.warimaetax
            END AS warimaetax,
        old_1.transbincode,
        old_1.nyuhenkin,
        old_1.nyuhenkanflg,
        old_1.kaisha,
        old_1.nukikingaku,
        old_1.kiboudate,
        old_1.tokuicode,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.shokei
            END AS shokei,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.tax_p
            END AS tax_p,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.sogokei_p
            END AS sogokei_p,
        old_1.sendenno,
        0 AS insertdate_p,
        0 AS inserttime_p,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.updatedate_p
            END AS updatedate_p,
        0 AS updatetime_p,
        'DUMMY' AS kkng_kbn,
        'DUMMY' AS tuka_cd,
        0 AS shokei_tuka,
        old_1.tax_tuka,
        old_1.sogokei_tuka,
        'DUMMY' AS juchno,
        0 AS juchnibuno,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 'DUMMY'::CHARACTER VARYING
            ELSE COALESCE(hen.dshenpinriyu, 'DUMMY'::CHARACTER VARYING)
            END AS henreasonname_p,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 'DUMMY'::CHARACTER VARYING
            ELSE old_1.henreasoncode
            END AS henreasoncode_p,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 'DUMMY'::CHARACTER VARYING
            ELSE old_1.torikeikbn
            END AS torikeikbn_p,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 0::NUMERIC
            ELSE old_1.juchdate
            END AS juchdate_p,
        CASE 
            WHEN cit80.torikeikbn::TEXT = '03'::TEXT
                OR cit80.torikeikbn IS NULL
                AND '03' IS NULL
                THEN 'DUMMY'::CHARACTER VARYING
            ELSE old_1.tenpocode
            END AS tenpocode_p,
        'DUMMY' AS tenpobunrui,
        'DUMMY' AS shokuikibunrui,
        'DUMMY' AS contea,
        'DUMMY' AS conteb,
        'DUMMY' AS contec,
        'DUMMY' AS conted,
        'DUMMY' AS contee,
        'DUMMY' AS contef,
        'DUMMY' AS conteg,
        'DUMMY' AS conteh,
        'DUMMY' AS contei,
        'DUMMY' AS contej,
        0 AS fskbn,
        'DUMMY' AS bmn_hyouji_cd,
        'DUMMY' AS bmn_nms,
        'DUMMY' AS processtype_cd,
        'DUMMY' AS daihyou_shukask_cd,
        'DUMMY' AS daihyou_shukask_nmr,
        'DUMMY' AS insertid_p,
        'DUMMY' AS tokuiname_aspac,
        'DUMMY' AS skysk_name,
        'DUMMY' AS skysk_cd,
        'DUMMY' AS juch_bko,
        'DUMMY' AS marker,
        'DUMMY' AS uri_hen_kbn,
        'DUMMY' AS sal_jisk_imp_snsh_no,
        old_1.transactiontypekbn,
        '1' AS kakokbn,
        old_1.kingaku,
        old_1.warimaekomikingaku,
        old_1.meisainukikingaku,
        old_1.warimaenukikingaku,
        old_1.meisaitax,
        old_1.has_bn_kingaku,
        old_1.has_bn_meisainukikingaku,
        old_1.has_bn_meisaitax,
        old_1.has_bn_anbunmeisainukikingaku,
        old_1.has_bn_kingaku_tuka,
        old_1.has_bn_meisainukikingaku_tuka,
        old_1.has_bn_meisaitax_tuka,
        '1' AS port_uniq_flg
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
    
    UNION ALL
    
    SELECT (NVL(new_1.saleno::TEXT, '') || nvl(new_1.saleno_p::TEXT, ''))::CHARACTER VARYING AS TEXT,
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
        new_1.saleno_p,
        new_1.shukadate_p,
        new_1.shohingokei,
        new_1.komiwarikingaku,
        new_1.warimaenukigokei,
        new_1.warimaetax,
        new_1.transbincode,
        0 AS nyuhenkin,
        1 AS nyuhenkanflg,
        'DUMMY' AS kaisha,
        new_1.nukikingaku,
        new_1.kiboudate,
        new_1.tokuicode,
        new_1.shokei,
        new_1.tax_p,
        new_1.sogokei_p,
        new_1.sendenno,
        new_1.insertdate_p,
        new_1.inserttime_p,
        new_1.updatedate_p,
        new_1.updatetime_p,
        new_1.kkng_kbn,
        new_1.tuka_cd,
        new_1.shokei_tuka,
        new_1.tax_tuka,
        new_1.sogokei_tuka,
        new_1.juchno,
        new_1.juchnibuno,
        new_1.henreasonname_p,
        new_1.henreasoncode_p,
        new_1.torikeikbn_p,
        new_1.juchdate_p,
        new_1.tenpocode_p,
        new_1.tenpobunrui,
        new_1.shokuikibunrui,
        new_1.contea,
        new_1.conteb,
        new_1.contec,
        new_1.conted,
        new_1.contee,
        new_1.contef,
        new_1.conteg,
        new_1.conteh,
        new_1.contei,
        new_1.contej,
        new_1.fskbn,
        new_1.bmn_hyouji_cd,
        new_1.bmn_nms,
        new_1.processtype_cd,
        new_1.daihyou_shukask_cd,
        new_1.daihyou_shukask_nmr,
        new_1.insertid_p,
        new_1.tokuiname_aspac,
        new_1.skysk_name,
        new_1.skysk_cd,
        new_1.juch_bko,
        new_1.marker::CHARACTER VARYING AS marker,
        new_1.uri_hen_kbn,
        new_1.sal_jisk_imp_snsh_no,
        'DUMMY' AS transactiontypekbn,
        '0' AS kakokbn,
        new_1.sogokei AS kingaku,
        0 AS warimaekomikingaku,
        new_1.nukikingaku AS meisainukikingaku,
        0 AS warimaenukikingaku,
        new_1.tax AS meisaitax,
        new_1.sogokei_p AS has_bn_kingaku,
        new_1.nukikingaku AS has_bn_meisainukikingaku,
        new_1.tax_p AS has_bn_meisaitax,
        new_1.nukikingaku AS has_bn_anbunmeisainukikingaku,
        new_1.sogokei_tuka AS has_bn_kingaku_tuka,
        new_1.nukikingaku AS has_bn_meisainukikingaku_tuka,
        new_1.tax_tuka AS has_bn_meisaitax_tuka,
        new_1.port_uniq_flg
    FROM kesai_h_data_mart_sub new_1
    
    UNION ALL
    
    SELECT (NVL(new_1.saleno::TEXT, '') || '調整行DUMMY'::TEXT)::CHARACTER VARYING AS TEXT,
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
        0 AS soryo,
        0 AS tax,
        0 AS sogokei,
        new_1.tenpocode,
        new_1.shukaym::INTEGER AS shukaym,
        new_1.shukadate,
        new_1.shukaquarter,
        new_1.shukajigyoki,
        new_1.zipcode,
        new_1.todofukencode,
        0 AS riyopoint,
        0 AS happenpoint,
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
        '3' AS maker,
        new_1.todofuken_code,
        new_1.henreasonname,
        new_1.uketsukeusrid,
        new_1.uketsuketelcompanycd,
        new_1.smkeiroid,
        new_1.dipromid,
        new_1.saleno_trm,
        0 AS dicollectprc,
        0 AS ditoujitsuhaisoprc,
        0 AS didiscountall,
        0 AS c_didiscountprc,
        0 AS point_exchange,
        new_1.logincode,
        new_1.shukkasts,
        new_1.divouchercode,
        0 AS ditaxrate,
        0 AS diseikyuremain,
        new_1.dinyukinsts,
        new_1.dicardnyukinsts,
        new_1.disokoid AS sokoid,
        new_1.dihaisokeitai,
        '調整行DUMMY' AS saleno_p,
        new_1.shukadate_p,
        0 AS shohingokei,
        0 AS komiwarikingaku,
        0 AS warimaenukigokei,
        0 AS warimaetax,
        new_1.transbincode,
        0 AS nyuhenkin,
        1 AS nyuhenkanflg,
        'DUMMY' AS kaisha,
        0 AS nukikingaku,
        new_1.kiboudate,
        new_1.tokuicode,
        0 AS shokei,
        0 AS tax_p,
        0 AS sogokei_p,
        new_1.sendenno,
        new_1.insertdate_p,
        new_1.inserttime_p,
        new_1.updatedate_p,
        new_1.updatetime_p,
        new_1.kkng_kbn,
        new_1.tuka_cd,
        0 AS shokei_tuka,
        0 AS tax_tuka,
        0 AS sogokei_tuka,
        new_1.juchno,
        new_1.juchnibuno,
        new_1.henreasonname_p,
        new_1.henreasoncode_p,
        new_1.torikeikbn_p,
        new_1.juchdate_p,
        new_1.tenpocode_p,
        new_1.tenpobunrui,
        new_1.shokuikibunrui,
        new_1.contea,
        new_1.conteb,
        new_1.contec,
        new_1.conted,
        new_1.contee,
        new_1.contef,
        new_1.conteg,
        new_1.conteh,
        new_1.contei,
        new_1.contej,
        new_1.fskbn,
        new_1.bmn_hyouji_cd,
        new_1.bmn_nms,
        new_1.processtype_cd,
        new_1.daihyou_shukask_cd,
        new_1.daihyou_shukask_nmr,
        new_1.insertid_p,
        new_1.tokuiname_aspac,
        new_1.skysk_name,
        new_1.skysk_cd,
        new_1.juch_bko,
        'DUMMY' AS marker,
        new_1.uri_hen_kbn,
        new_1.sal_jisk_imp_snsh_no,
        'DUMMY' AS transactiontypekbn,
        '0' AS kakokbn,
        0 AS kingaku,
        0 AS warimaekomikingaku,
        0 AS meisainukikingaku,
        0 AS warimaenukikingaku,
        0 AS meisaitax,
        0 AS has_bn_kingaku,
        0 AS has_bn_meisainukikingaku,
        0 AS has_bn_meisaitax,
        0 AS has_bn_anbunmeisainukikingaku,
        0 AS has_bn_kingaku_tuka,
        0 AS has_bn_meisainukikingaku_tuka,
        0 AS has_bn_meisaitax_tuka,
        '0' AS port_uniq_flg
    FROM kesai_h_data_mart_sub new_1,
        (
            SELECT h.saleno,
                h.kesaiid,
                h.sogokei - h.soryo - h.dicollectprc - h.ditoujitsuhaisoprc - h.tax AS sum_h_kin_nuki,
                h.sogokei - h.soryo - h.dicollectprc - h.ditoujitsuhaisoprc AS sum_h_kin
            FROM kesai_h_data_mart_sub_n h
            ) h_sum,
        (
            SELECT m.saleno,
                sum(m.kingaku) AS sum_m_kin,
                sum(m.meisainukikingaku) AS sum_m_kin_nuki
            FROM (
                SELECT kesai_m_data_mart_sub_n_u.saleno,
                    kesai_m_data_mart_sub_n_u.gyono,
                    kesai_m_data_mart_sub_n_u.meisaikbn,
                    kesai_m_data_mart_sub_n_u.itemcode,
                    kesai_m_data_mart_sub_n_u.itemname,
                    kesai_m_data_mart_sub_n_u.diid,
                    kesai_m_data_mart_sub_n_u.disetid,
                    kesai_m_data_mart_sub_n_u.suryo,
                    kesai_m_data_mart_sub_n_u.tanka,
                    kesai_m_data_mart_sub_n_u.kingaku,
                    kesai_m_data_mart_sub_n_u.meisainukikingaku,
                    kesai_m_data_mart_sub_n_u.wariritu,
                    kesai_m_data_mart_sub_n_u.warimaekomitanka,
                    kesai_m_data_mart_sub_n_u.warimaenukikingaku,
                    kesai_m_data_mart_sub_n_u.warimaekomikingaku,
                    kesai_m_data_mart_sub_n_u.bun_tanka,
                    kesai_m_data_mart_sub_n_u.bun_kingaku,
                    kesai_m_data_mart_sub_n_u.bun_meisainukikingaku,
                    kesai_m_data_mart_sub_n_u.bun_wariritu,
                    kesai_m_data_mart_sub_n_u.bun_warimaekomitanka,
                    kesai_m_data_mart_sub_n_u.bun_warimaenukikingaku,
                    kesai_m_data_mart_sub_n_u.bun_warimaekomikingaku,
                    kesai_m_data_mart_sub_n_u.dispsaleno,
                    kesai_m_data_mart_sub_n_u.kesaiid,
                    kesai_m_data_mart_sub_n_u.diorderid,
                    '0' AS henpinsts,
                    kesai_m_data_mart_sub_n_u.c_dspointitemflg,
                    kesai_m_data_mart_sub_n_u.c_diitemtype,
                    kesai_m_data_mart_sub_n_u.c_diadjustprc,
                    kesai_m_data_mart_sub_n_u.ditotalprc,
                    kesai_m_data_mart_sub_n_u.diitemtax,
                    kesai_m_data_mart_sub_n_u.c_diitemtotalprc,
                    kesai_m_data_mart_sub_n_u.c_didiscountmeisai,
                    kesai_m_data_mart_sub_n_u.disetmeisaiid,
                    kesai_m_data_mart_sub_n_u.c_dssetitemkbn,
                    kesai_m_data_mart_sub_n_u.maker
                FROM kesai_m_data_mart_sub_n_u kesai_m_data_mart_sub_n_u
                
                UNION ALL
                
                SELECT kesai_m_data_mart_sub_n_h.saleno,
                    kesai_m_data_mart_sub_n_h.gyono,
                    kesai_m_data_mart_sub_n_h.meisaikbn,
                    kesai_m_data_mart_sub_n_h.itemcode,
                    kesai_m_data_mart_sub_n_h.itemname,
                    kesai_m_data_mart_sub_n_h.diid,
                    kesai_m_data_mart_sub_n_h.disetid,
                    kesai_m_data_mart_sub_n_h.suryo,
                    kesai_m_data_mart_sub_n_h.tanka,
                    kesai_m_data_mart_sub_n_h.kingaku,
                    kesai_m_data_mart_sub_n_h.meisainukikingaku,
                    kesai_m_data_mart_sub_n_h.wariritu,
                    kesai_m_data_mart_sub_n_h.warimaekomitanka,
                    kesai_m_data_mart_sub_n_h.warimaenukikingaku,
                    kesai_m_data_mart_sub_n_h.warimaekomikingaku,
                    kesai_m_data_mart_sub_n_h.bun_tanka,
                    kesai_m_data_mart_sub_n_h.bun_kingaku,
                    kesai_m_data_mart_sub_n_h.bun_meisainukikingaku,
                    kesai_m_data_mart_sub_n_h.bun_wariritu,
                    kesai_m_data_mart_sub_n_h.bun_warimaekomitanka,
                    kesai_m_data_mart_sub_n_h.bun_warimaenukikingaku,
                    kesai_m_data_mart_sub_n_h.bun_warimaekomikingaku,
                    kesai_m_data_mart_sub_n_h.dispsaleno,
                    kesai_m_data_mart_sub_n_h.kesaiid,
                    kesai_m_data_mart_sub_n_h.diorderid,
                    kesai_m_data_mart_sub_n_h.henpinsts,
                    kesai_m_data_mart_sub_n_h.c_dspointitemflg,
                    kesai_m_data_mart_sub_n_h.c_diitemtype,
                    kesai_m_data_mart_sub_n_h.c_diadjustprc,
                    kesai_m_data_mart_sub_n_h.ditotalprc,
                    kesai_m_data_mart_sub_n_h.diitemtax,
                    kesai_m_data_mart_sub_n_h.c_diitemtotalprc,
                    kesai_m_data_mart_sub_n_h.c_didiscountmeisai,
                    kesai_m_data_mart_sub_n_h.disetmeisaiid,
                    kesai_m_data_mart_sub_n_h.c_dssetitemkbn,
                    kesai_m_data_mart_sub_n_h.maker
                FROM kesai_m_data_mart_sub_n_h kesai_m_data_mart_sub_n_h
                ) m
            WHERE m.diid::TEXT = m.disetid::TEXT
            GROUP BY m.saleno
            ) m_sum
    WHERE new_1.saleno::TEXT = h_sum.saleno::TEXT
        AND new_1.port_uniq_flg::TEXT = 1::TEXT
        AND h_sum.saleno::TEXT = m_sum.saleno::TEXT
        AND (
            h_sum.sum_h_kin <> m_sum.sum_m_kin
            OR h_sum.sum_h_kin_nuki <> m_sum.sum_m_kin_nuki
            )
    
    UNION ALL
    
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
        '調整行DUMMY' AS saleno_p,
        old_chsi.shukadate_p,
        0 AS shohingokei,
        0 AS komiwarikingaku,
        0 AS warimaenukigokei,
        0 AS warimaetax,
        old_chsi.transbincode,
        0 AS nyuhenkin,
        1 AS nyuhenkanflg,
        'DUMMY' AS kaisha,
        0 AS nukikingaku,
        old_chsi.kiboudate,
        old_chsi.tokuicode,
        0 AS shokei,
        0 AS tax_p,
        0 AS sogokei_p,
        old_chsi.sendenno,
        0 AS insertdate_p,
        0 AS inserttime_p,
        old_chsi.updatedate_p,
        0 AS updatetime_p,
        'DUMMY' AS kkng_kbn,
        'DUMMY' AS tuka_cd,
        0 AS shokei_tuka,
        0 AS tax_tuka,
        0 AS sogokei_tuka,
        'DUMMY' AS juchno,
        0 AS juchnibuno,
        old_chsi.henreasonname_p,
        old_chsi.henreasoncode_p,
        old_chsi.torikeikbn_p,
        old_chsi.juchdate_p,
        old_chsi.tenpocode_p,
        'DUMMY' AS tenpobunrui,
        'DUMMY' AS shokuikibunrui,
        'DUMMY' AS contea,
        'DUMMY' AS conteb,
        'DUMMY' AS contec,
        'DUMMY' AS conted,
        'DUMMY' AS contee,
        'DUMMY' AS contef,
        'DUMMY' AS conteg,
        'DUMMY' AS conteh,
        'DUMMY' AS contei,
        'DUMMY' AS contej,
        0 AS fskbn,
        'DUMMY' AS bmn_hyouji_cd,
        'DUMMY' AS bmn_nms,
        'DUMMY' AS processtype_cd,
        'DUMMY' AS daihyou_shukask_cd,
        'DUMMY' AS daihyou_shukask_nmr,
        'DUMMY' AS insertid_p,
        'DUMMY' AS tokuiname_aspac,
        'DUMMY' AS skysk_name,
        'DUMMY' AS skysk_cd,
        'DUMMY' AS juch_bko,
        'DUMMY' AS marker,
        'DUMMY' AS uri_hen_kbn,
        'DUMMY' AS sal_jisk_imp_snsh_no,
        old_chsi.transactiontypekbn,
        '1' AS kakokbn,
        0 AS kingaku,
        0 AS warimaekomikingaku,
        0 AS meisainukikingaku,
        0 AS warimaenukikingaku,
        0 AS meisaitax,
        0 AS has_bn_kingaku,
        0 AS has_bn_meisainukikingaku,
        0 AS has_bn_meisaitax,
        0 AS has_bn_anbunmeisainukikingaku,
        0 AS has_bn_kingaku_tuka,
        0 AS has_bn_meisainukikingaku_tuka,
        0 AS has_bn_meisaitax_tuka,
        '0' AS port_uniq_flg
    FROM kesai_h_data_mart_sub_old_chsi old_chsi
    )
SELECT *
FROM final
