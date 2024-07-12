WITH item_hanbai_tbl
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.ITEM_HANBAI_TBL
      ),
tbecsetitem
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLitg_INTEGRATION.TBECSETITEM
      ),
tbecitem
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLitg_INTEGRATION.TBECITEM
      ),
cim24itbun_ikou
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM24ITBUN_IKOU
      ),
item_bunrval_v
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.ITEM_BUNRVAL_V
      ),
c_tbecprivilegemst
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLitg_INTEGRATION.C_TBECPRIVILEGEMST
      ),
cim03
AS (
      SELECT iht.h_itemcode AS itemcode,
            iht.h_itemname AS itemname,
            iht.itemnamer,
            iht.itemkbn,
            iht.tanka,
            iht.bunruicode2,
            iht.bunruicode3,
            iht.bunruicode4,
            iht.bunruicode5,
            iht.insertdate,
            iht.dsoption001,
            iht.dsoption002,
            iht.dsoption010,
            iht.haiban_hin_cd,
            iht.syutoku_kbn
      FROM item_hanbai_tbl iht
      WHERE iht.marker = 1
            OR iht.marker = 2
            OR iht.marker = 3
      ),
cim08
AS (
      SELECT item.dsitemid AS itemcode,
            item_kosei.dsitemid AS koseiocode,
            COALESCE(seti.diitemnum::NUMERIC::NUMERIC(18, 0), 0::NUMERIC::NUMERIC(18, 0)::NUMERIC(38, 18)) AS suryo
      FROM tbecsetitem seti
      JOIN tbecitem item ON seti.diid = item.diid
            AND item.dsoption001::TEXT = '販売商品'::CHARACTER VARYING::TEXT
            AND item.dielimflg::TEXT = '0'::CHARACTER VARYING::TEXT
      JOIN tbecitem item_kosei ON seti.disetitemid = item_kosei.diid
            AND item_kosei.dsoption001::TEXT = '販売商品'::CHARACTER VARYING::TEXT
            AND item.dielimflg::TEXT = '0'::CHARACTER VARYING::TEXT
      ),
derived_table1
AS (
      SELECT CASE 
                  WHEN sscmnhinbunrval.hin_bunr_taik_id::TEXT = '2'::CHARACTER VARYING::TEXT
                        AND sscmnhinbunrval.hin_bunr_kaisou_kbn::TEXT = '3'::CHARACTER VARYING::TEXT
                        THEN '5'::CHARACTER VARYING
                  WHEN sscmnhinbunrval.hin_bunr_taik_id::TEXT = '3'::CHARACTER VARYING::TEXT
                        AND sscmnhinbunrval.hin_bunr_kaisou_kbn::TEXT = '2'::CHARACTER VARYING::TEXT
                        THEN '2'::CHARACTER VARYING
                  WHEN sscmnhinbunrval.hin_bunr_taik_id::TEXT = '3'::CHARACTER VARYING::TEXT
                        AND sscmnhinbunrval.hin_bunr_kaisou_kbn::TEXT = '3'::CHARACTER VARYING::TEXT
                        THEN '3'::CHARACTER VARYING
                  ELSE NULL::CHARACTER VARYING
                  END AS itbunshcode,
            sscmnhinbunrval.hin_bunr_val_cd AS itbuncode,
            sscmnhinbunrval.hin_bunr_val_nms AS itbunname
      FROM item_bunrval_v sscmnhinbunrval
      ),
cim24
AS (
      SELECT derived_table1.itbunshcode,
            derived_table1.itbuncode,
            derived_table1.itbunname
      FROM derived_table1
      WHERE derived_table1.itbunshcode IS NOT NULL
      
      UNION
      
      SELECT ikou.itbunshcode,
            ikou.itbuncode,
            ikou.itbunname
      FROM cim24itbun_ikou ikou
      ),
ct11
AS (
      SELECT cim03.itemcode,
            cim03.itemname,
            (cim03.itemcode::TEXT || ' : '::CHARACTER VARYING::TEXT) || cim03.itemname::TEXT AS itemcname,
            cim03.tanka,
            cim03.bunruicode3 AS chubuncode,
            COALESCE(cim24_chu.itbunname, 'その他'::CHARACTER VARYING::CHARACTER VARYING(65535)) AS chubunname,
            CASE 
                  WHEN cim24_chu.itbunname::TEXT = NULL::CHARACTER VARYING::TEXT
                        OR cim24_chu.itbunname IS NULL
                        AND NULL IS NULL
                        THEN 'その他'::CHARACTER VARYING::TEXT
                  ELSE (cim03.bunruicode3::TEXT || ' : '::CHARACTER VARYING::TEXT) || cim24_chu.itbunname::TEXT
                  END AS chubuncname,
            cim03.bunruicode2 AS daibuncode,
            COALESCE(cim24_dai.itbunname, 'その他'::CHARACTER VARYING::CHARACTER VARYING(65535)) AS daibunname,
            CASE 
                  WHEN cim24_dai.itbunname::TEXT = NULL::CHARACTER VARYING::TEXT
                        OR cim24_dai.itbunname IS NULL
                        AND NULL IS NULL
                        THEN 'その他'::CHARACTER VARYING::TEXT
                  ELSE (cim03.bunruicode2::TEXT || ' : '::CHARACTER VARYING::TEXT) || cim24_dai.itbunname::TEXT
                  END AS daibuncname,
            cim03.bunruicode5 AS daidaibuncode,
            COALESCE(cim24_daidai.itbunname, 'その他'::CHARACTER VARYING::CHARACTER VARYING(65535)) AS daidaibunname,
            CASE 
                  WHEN cim24_daidai.itbunname::TEXT = NULL::CHARACTER VARYING::TEXT
                        OR cim24_daidai.itbunname IS NULL
                        AND NULL IS NULL
                        THEN 'その他'::CHARACTER VARYING::TEXT
                  ELSE (cim03.bunruicode5::TEXT || ' : '::CHARACTER VARYING::TEXT) || cim24_daidai.itbunname::TEXT
                  END AS daidaibuncname,
            COALESCE(cim03b.itemcode, cim03.itemcode::CHARACTER VARYING(65535)) AS bunkai_itemcode,
            COALESCE(cim03b.itemname, cim03.itemname::CHARACTER VARYING(65535)) AS bunkai_itemname,
            (COALESCE(cim03b.itemcode, cim03.itemcode::CHARACTER VARYING(65535))::TEXT || ' : '::CHARACTER VARYING::TEXT) || COALESCE(cim03b.itemname, cim03.itemname::CHARACTER VARYING(65535))::TEXT AS bunkai_itemcname,
            COALESCE(cim03b.tanka::NUMERIC::NUMERIC(18, 0), cim03.tanka::NUMERIC::NUMERIC(18, 0)::NUMERIC(38, 18)) AS bunkai_tanka,
            COALESCE(cim08.suryo, 1::NUMERIC::NUMERIC(18, 0)::NUMERIC(38, 18)) AS bunkai_kossu,
            COALESCE(CASE 
                        WHEN setgokei.gokeitanka = 0::NUMERIC::NUMERIC(18, 0)
                              OR setgokei.gokeitanka IS NULL
                              AND 0 IS NULL
                              THEN CASE 
                                          WHEN setgokei.gokeisuryo = 0::NUMERIC::NUMERIC(18, 0)
                                                OR setgokei.gokeisuryo IS NULL
                                                AND 0 IS NULL
                                                THEN 1::DOUBLE PRECISION
                                          ELSE cim08.suryo::DOUBLE PRECISION / setgokei.gokeisuryo::DOUBLE PRECISION
                                          END
                        ELSE cim03b.tanka::DOUBLE PRECISION * cim08.suryo::DOUBLE PRECISION / setgokei.gokeitanka::DOUBLE PRECISION
                        END, 1::NUMERIC::NUMERIC(18, 0)::NUMERIC(38, 18)::DOUBLE PRECISION) AS bunkai_kosritu,
            cim03.insertdate,
            1 AS marker
      FROM cim03 cim03
      LEFT JOIN cim08 cim08 ON cim03.itemcode::TEXT = cim08.itemcode::TEXT
      LEFT JOIN cim03 cim03b ON cim08.koseiocode::TEXT = cim03b.itemcode::TEXT
      LEFT JOIN (
            SELECT m08.itemcode,
                  COALESCE(sum(m03.tanka::NUMERIC::NUMERIC(18, 0) * m08.suryo), 0::NUMERIC::NUMERIC(18, 0)::NUMERIC(38, 18)) AS gokeitanka,
                  COALESCE(sum(m08.suryo), 0::NUMERIC::NUMERIC(18, 0)::NUMERIC(38, 18)) AS gokeisuryo
            FROM cim08 m08
            JOIN cim03 m03 ON m08.koseiocode::TEXT = m03.itemcode::TEXT
            GROUP BY m08.itemcode
            ) setgokei ON cim08.itemcode::TEXT = setgokei.itemcode::TEXT
      LEFT JOIN cim24 cim24_chu ON cim03.bunruicode3::TEXT = cim24_chu.itbuncode::TEXT
            AND cim24_chu.itbunshcode::TEXT = 3::CHARACTER VARYING::TEXT
      LEFT JOIN cim24 cim24_dai ON cim03.bunruicode2::TEXT = cim24_dai.itbuncode::TEXT
            AND cim24_dai.itbunshcode::TEXT = 2::CHARACTER VARYING::TEXT
      LEFT JOIN cim24 cim24_daidai ON cim03.bunruicode5::TEXT = cim24_daidai.itbuncode::TEXT
            AND cim24_daidai.itbunshcode::TEXT = 5::CHARACTER VARYING::TEXT
      ),
ct1
AS (
      SELECT itemcode::VARCHAR(45) AS itemcode,
            itemname::VARCHAR(400) AS itemname,
            itemcname::VARCHAR(446) AS itemcname,
            tanka::VARCHAR(45) AS tanka,
            chubuncode::VARCHAR(45) AS chubuncode,
            chubunname::VARCHAR(100) AS chubunname,
            chubuncname::VARCHAR(146) AS chubuncname,
            daibuncode::VARCHAR(45) AS daibuncode,
            daibunname::VARCHAR(100) AS daibunname,
            daibuncname::VARCHAR(146) AS daibuncname,
            daidaibuncode::VARCHAR(45) AS daidaibuncode,
            daidaibunname::VARCHAR(100) AS daidaibunname,
            daidaibuncname::VARCHAR(146) AS daidaibuncname,
            bunkai_itemcode::VARCHAR(45) AS bunkai_itemcode,
            bunkai_itemname::VARCHAR(400) AS bunkai_itemname,
            bunkai_itemcname::VARCHAR(446) AS bunkai_itemcname,
            bunkai_tanka::NUMBER(38, 18) AS bunkai_tanka,
            bunkai_kossu::NUMBER(38, 18) AS bunkai_kossu,
            bunkai_kosritu::FLOAT AS bunkai_kosritu,
            insertdate::NUMBER(18, 0) AS insertdate,
            marker::NUMBER(38, 0) AS marker
      FROM ct11
      ),
ct21
AS (
      SELECT c_diprivilegeid AS itemcode,
            c_dsprivilegename AS itemname,
            c_dsprivilegename AS itemcname,
            NULL AS tanka,
            NULL AS chubuncode,
            NULL AS chubunname,
            NULL AS chubuncname,
            NULL AS daibuncode,
            NULL AS daibunname,
            NULL AS daibuncname,
            NULL AS daidaibuncode,
            NULL AS daidaibunname,
            NULL AS daidaibuncname,
            c_diprivilegeid AS bunkai_itemcode,
            c_dsprivilegename AS bunkai_itemname,
            c_tbecprivilegemst.c_diprivilegeid || ' : ' || c_tbecprivilegemst.c_dsprivilegename AS bunkai_itemcname,
            0 AS bunkai_tanka,
            1 AS bunkai_kossu,
            1 AS bunkai_kosritu,
            20170331 AS insertdate,
            2 AS MARKER
      FROM c_tbecprivilegemst
      ),
ct2
AS (
      SELECT itemcode::VARCHAR(45) AS itemcode,
            itemname::VARCHAR(400) AS itemname,
            itemcname::VARCHAR(446) AS itemcname,
            tanka::VARCHAR(45) AS tanka,
            chubuncode::VARCHAR(45) AS chubuncode,
            chubunname::VARCHAR(100) AS chubunname,
            chubuncname::VARCHAR(146) AS chubuncname,
            daibuncode::VARCHAR(45) AS daibuncode,
            daibunname::VARCHAR(100) AS daibunname,
            daibuncname::VARCHAR(146) AS daibuncname,
            daidaibuncode::VARCHAR(45) AS daidaibuncode,
            daidaibunname::VARCHAR(100) AS daidaibunname,
            daidaibuncname::VARCHAR(146) AS daidaibuncname,
            bunkai_itemcode::VARCHAR(45) AS bunkai_itemcode,
            bunkai_itemname::VARCHAR(400) AS bunkai_itemname,
            bunkai_itemcname::VARCHAR(446) AS bunkai_itemcname,
            bunkai_tanka::NUMBER(38, 18) AS bunkai_tanka,
            bunkai_kossu::NUMBER(38, 18) AS bunkai_kossu,
            bunkai_kosritu::FLOAT AS bunkai_kosritu,
            insertdate::NUMBER(18, 0) AS insertdate,
            marker::NUMBER(38, 0) AS marker
      FROM ct21
      ),
ct3
AS (
      SELECT 'X000000001',
            '利用ポイント数(交換以外)',
            'X000000001 : 利用ポイント数(交換以外)',
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            'X000000001',
            '利用ポイント数(交換以外)',
            'X000000001 : 利用ポイント数(交換以外)',
            0,
            1,
            1,
            20170331,
            3
      ),
ct4
AS (
      SELECT 'X000000002',
            '利用ポイント数(交換)',
            'X000000002 : 利用ポイント数(交換)',
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            'X000000002',
            '利用ポイント数(交換)',
            'X000000002 : 利用ポイント数(交換)',
            0,
            1,
            1,
            20170331,
            3
      ),
transformed
AS (
      SELECT *
      FROM ct1
      
      UNION ALL
      
      SELECT *
      FROM ct2
      
      UNION ALL
      
      SELECT *
      FROM ct3
      
      UNION ALL
      
      SELECT *
      FROM ct4
      ),
final
AS (
      SELECT itemcode::VARCHAR(45) AS itemcode,
            itemname::VARCHAR(400) AS itemname,
            itemcname::VARCHAR(446) AS itemcname,
            tanka::VARCHAR(45) AS tanka,
            chubuncode::VARCHAR(45) AS chubuncode,
            chubunname::VARCHAR(100) AS chubunname,
            chubuncname::VARCHAR(146) AS chubuncname,
            daibuncode::VARCHAR(45) AS daibuncode,
            daibunname::VARCHAR(100) AS daibunname,
            daibuncname::VARCHAR(146) AS daibuncname,
            daidaibuncode::VARCHAR(45) AS daidaibuncode,
            daidaibunname::VARCHAR(100) AS daidaibunname,
            daidaibuncname::VARCHAR(146) AS daidaibuncname,
            bunkai_itemcode::VARCHAR(45) AS bunkai_itemcode,
            bunkai_itemname::VARCHAR(400) AS bunkai_itemname,
            bunkai_itemcname::VARCHAR(446) AS bunkai_itemcname,
            bunkai_tanka::NUMBER(38, 18) AS bunkai_tanka,
            bunkai_kossu::NUMBER(38, 18) AS bunkai_kossu,
            bunkai_kosritu::FLOAT AS bunkai_kosritu,
            insertdate::NUMBER(18, 0) AS insertdate,
            marker::NUMBER(38, 0) AS marker,
            current_timestamp()::TIMESTAMP_NTZ(9) AS inserted_date,
            null::VARCHAR(100) AS inserted_by,
            current_timestamp()::TIMESTAMP_NTZ(9) AS updated_date,
            null::VARCHAR(100) AS updated_by
      FROM transformed
      )
SELECT *
FROM final