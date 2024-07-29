{{
    config
    (
        post_hook = "
                    INSERT INTO {{ ref('jpnitg_integration__dw_so_planet_err') }}  (
                    JCP_REC_SEQ, ID, RCV_DT, TEST_FLAG, BGN_SNDR_CD, WS_CD, RTL_TYPE, RTL_CD, TRADE_TYPE, SHP_DATE, SHP_NUM, TRADE_CD, DEP_CD, CHG_CD, PERSON_IN_CHARGE, PERSON_NAME, RTL_NAME, RTL_HO_CD, RTL_ADDRESS_CD, DATA_TYPE, OPT_FLD, ITEM_NM, ITEM_CD_TYP, ITEM_CD, QTY, QTY_TYPE, PRICE, PRICE_TYPE, BGN_SNDR_CD_GLN, RCV_CD_GLN, WS_CD_GLN, SHP_WS_CD, SHP_WS_CD_GLN, REP_NAME_KANJI, REP_INFO, TRADE_CD_GLN, RTL_CD_GLN, RTL_NAME_KANJI, RTL_HO_CD_GLN, ITEM_CD_GTIN, ITEM_NM_KANJI, UNT_PRC, NET_PRC, SALES_CHAN_TYPE, JCP_CREATE_DATE, jcp_rcv_dt_dupli, jcp_test_flag_dupli, jcp_qty_dupli, jcp_price_dupli, jcp_unt_prc_dupli, jcp_net_prc_dupli, export_flag
                    )
                    SELECT 
                    DISTINCT JCP_REC_SEQ, ID, RCV_DT, TEST_FLAG, BGN_SNDR_CD, WS_CD, RTL_TYPE, RTL_CD, TRADE_TYPE, SHP_DATE, SHP_NUM, TRADE_CD, DEP_CD, CHG_CD, PERSON_IN_CHARGE, PERSON_NAME, RTL_NAME, RTL_HO_CD, RTL_ADDRESS_CD, DATA_TYPE, OPT_FLD, ITEM_NM, ITEM_CD_TYP, ITEM_CD, QTY, QTY_TYPE, PRICE, PRICE_TYPE, BGN_SNDR_CD_GLN, RCV_CD_GLN, WS_CD_GLN, SHP_WS_CD, SHP_WS_CD_GLN, REP_NAME_KANJI, REP_INFO, TRADE_CD_GLN, RTL_CD_GLN, RTL_NAME_KANJI, RTL_HO_CD_GLN, ITEM_CD_GTIN, ITEM_NM_KANJI, UNT_PRC, NET_PRC, SALES_CHAN_TYPE, JCP_CREATE_DATE, NULL, NULL, NULL, NULL, NULL, NULL, '0'
                    FROM (
                    SELECT 
                        SU.JCP_REC_SEQ, dup.ID, dup.RCV_DT, dup.TEST_FLAG, dup.BGN_SNDR_CD, dup.WS_CD, dup.RTL_TYPE, dup.RTL_CD, dup.TRADE_TYPE, dup.SHP_DATE, dup.SHP_NUM, dup.TRADE_CD, dup.DEP_CD, dup.CHG_CD, dup.PERSON_IN_CHARGE, dup.PERSON_NAME, dup.RTL_NAME, dup.RTL_HO_CD, dup.RTL_ADDRESS_CD, dup.DATA_TYPE, dup.OPT_FLD, dup.ITEM_NM, dup.ITEM_CD_TYP, dup.ITEM_CD, dup.QTY, dup.QTY_TYPE, dup.PRICE, dup.PRICE_TYPE, dup.BGN_SNDR_CD_GLN, dup.RCV_CD_GLN, dup.WS_CD_GLN, dup.SHP_WS_CD, dup.SHP_WS_CD_GLN, dup.REP_NAME_KANJI, dup.REP_INFO, dup.TRADE_CD_GLN, dup.RTL_CD_GLN, dup.RTL_NAME_KANJI, dup.RTL_HO_CD_GLN, dup.ITEM_CD_GTIN, dup.ITEM_NM_KANJI, dup.UNT_PRC, dup.NET_PRC, dup.SALES_CHAN_TYPE, dup.JCP_CREATE_DATE,
                        ROW_NUMBER() OVER (
                        PARTITION BY SU.JCP_REC_SEQ ORDER BY tmp.priority
                        ) AS rnk
                    FROM {{this}} SU
                    INNER JOIN {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} dup ON SU.jcp_rec_seq = dup.jcp_rec_seq
                    INNER JOIN {{ source ('jpnwks_integration', 'temp_tbl')}} tmp ON SU.exec_flag = tmp.exec_flag
                    WHERE dup.jcp_rec_seq NOT IN (
                        SELECT jcp_rec_seq
                        FROM {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
                        WHERE error_cd = 'NRTL'
                        )
                    )
                    WHERE rnk = 1;


                    INSERT INTO {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
                    (JCP_REC_SEQ,ERROR_CD,EXPORT_FLAG)
                    SELECT A.JCP_REC_SEQ,A.ERROR_CD,A.EXPORT_FLAG
                    FROM
                    ((SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_001' THEN 'E001' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_002' THEN 'E002' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_004' THEN 'E004' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_007' THEN 'E007' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_008' THEN 'E008' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_009' THEN 'E009' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_012' THEN 'E012' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_014' THEN 'E014' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_017' THEN 'E017' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_018' THEN 'E018' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN ERROR_CD='ERR_020' THEN 'E020' ELSE '0' END ERROR_CD,
                    '0' EXPORT_FLAG
                    FROM {{this}} A))A
                    WHERE A.ERROR_CD!='0';


                    INSERT INTO {{ source('jpnitg_integration', 'dw_so_planet_err_cd_2_temp') }}
                    (JCP_REC_SEQ,ERROR_CD,EXEC_FLAG,EXPORT_FLAG,JCP_CREATE_DATE)
                    SELECT A.JCP_REC_SEQ,A.ERROR_CD,A.EXEC_FLAG,A.EXPORT_FLAG,A.JCP_CREATE_DATE
                    FROM
                    (
                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_001' THEN 'E001' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_002'  THEN 'E002' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ)

                    UNION

                    (
                    SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_004'  THEN 'E004' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ
                    )
                    UNION
                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_007'  THEN 'E007' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ)

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_008'  THEN 'E008' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ
                    )

                    UNION

                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_009'  THEN 'E009' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ
                    )

                    UNION
                    (
                    SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_012'  THEN 'E012' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ
                    )
                    UNION
                    (SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_014'  THEN 'E014' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ
                    )

                    UNION
                    (
                    SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_017'  THEN 'E017' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ
                    )
                    UNION
                    (
                    SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_018'  THEN 'E018' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ
                    )

                    UNION

                    (
                    SELECT A.JCP_REC_SEQ,
                    CASE WHEN error_cd='ERR_020'  THEN 'E020' ELSE '0' END ERROR_CD,
                    A.EXEC_FLAG,
                    '0' EXPORT_FLAG,
                    TO_TIMESTAMP(SUBSTRING(current_timestamp(),1,19)) JCP_CREATE_DATE
                    FROM {{this}} A inner join {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} B on A.JCP_REC_SEQ=B.JCP_REC_SEQ)
                    )A
                    WHERE A.error_cd != '0';
                    "
        
    )
}}


WITH wk_so_planet_no_dup
AS (
  SELECT *
  FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
  ),
edi_bgn_sndr
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__edi_bgn_sndr') }}
  ),
edi_rtlr_cd_chng
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__edi_rtlr_cd_chng') }}
  ),
itg_mds_jp_mt_so_rtlr_chg
AS (
  SELECT *
  FROM {{ ref('jpnitg_integration__itg_mds_jp_mt_so_rtlr_chg') }}
  ),
edi_store_m
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__edi_store_m') }}
  ),
itg_mds_jp_mt_so_item_chg
AS (
  SELECT *
  FROM {{ ref('jpnitg_integration__itg_mds_jp_mt_so_item_chg') }}
  ),
edi_item_m
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__edi_item_m') }}
  ),
itg_mds_jp_mt_so_ws_chg
AS (
  SELECT *
  FROM {{ ref('jpnitg_integration__itg_mds_jp_mt_so_ws_chg') }}
  ),
edi_jedpar
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__edi_jedpar') }}
  ),
planet_no_dup_qty
AS (
  SELECT *
  FROM {{ ref('jpnwks_integration__planet_no_dup_qty') }}
  ),
dw_so_planet_err_cd
AS (
  SELECT *
  FROM {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
  ),
mt_trade_conv
AS (
  SELECT *
  FROM {{ source('jpnedw_integration', 'mt_trade_conv') }}
  ),
ct01
AS (
  SELECT a.jcp_rec_seq,
    'ERR_001' AS error_cd,
    CASE 
      WHEN (
          rtrim(ltrim(b.bgn_sndr_cd)) = ''
          OR b.bgn_sndr_cd IS NULL
          )
        THEN 'MANUAL'
      ELSE '0'
      END exec_flag
  FROM wk_so_planet_no_dup a
  LEFT JOIN edi_bgn_sndr b ON b.bgn_sndr_cd = a.bgn_sndr_cd
  ),
ct02
AS (
  SELECT a.jcp_rec_seq,
    'ERR_002' AS error_cd,
    CASE 
      WHEN rtrim(ltrim(a.opt_fld)) = 'S'
        OR rtrim(ltrim(a.opt_fld)) = 's'
        THEN 'AUTOCORRECT'
      WHEN (
          rtrim(ltrim(a.opt_fld)) = ''
          OR a.opt_fld IS NULL
          )
        THEN '0'
      ELSE 'DELETE'
      END exec_flag
  FROM wk_so_planet_no_dup a
  ),
ct03
AS (
  SELECT nd.jcp_rec_seq,
    'ERR_004' AS error_cd,
    CASE 
      WHEN (
          nd.rtl_cd IS NULL
          OR rtrim(ltrim(nd.rtl_cd)) = ''
          OR nd.rtl_name IS NULL
          OR rtrim(ltrim(nd.rtl_name)) = ''
          )
        THEN 'DELETE'
      WHEN (
          edistr.str_cd IS NULL
          OR rtrim(ltrim(edistr.str_cd)) = ''
          )
        AND edichng.str_cd IS NOT NULL
        THEN 'AUTOCORRECT'
      WHEN (
          edichng.rtlr_cd IS NULL
          OR rtrim(ltrim(edichng.rtlr_cd)) = ''
          )
        AND (
          edistr.str_cd IS NULL
          OR rtrim(ltrim(edistr.str_cd)) = ''
          )
        AND mdsrtl.error_type = 2
        THEN 'DELETE'
      WHEN (
          edichng.rtlr_cd IS NULL
          OR rtrim(ltrim(edichng.rtlr_cd)) = ''
          )
        AND (
          edistr.str_cd IS NULL
          OR rtrim(ltrim(edistr.str_cd)) = ''
          )
        AND rtrim(ltrim(mdsrtl.str_cd)) != ''
        AND mdsrtl.error_type = 1
        THEN 'AUTOCORRECT'
      WHEN (
          edichng.rtlr_cd IS NULL
          OR rtrim(ltrim(edichng.rtlr_cd)) = ''
          )
        AND (
          edistr.str_cd IS NULL
          OR rtrim(ltrim(edistr.str_cd)) = ''
          )
        AND rtrim(ltrim(mdsrtl.str_cd)) != ''
        AND (
          mdsrtl.error_type NOT IN (1, 2)
          OR mdsrtl.error_type IS NULL
          OR rtrim(ltrim(mdsrtl.error_type)) = ''
          )
        THEN 'MANUAL'
      WHEN (
          edichng.rtlr_cd IS NULL
          OR rtrim(ltrim(edichng.rtlr_cd)) = ''
          )
        AND (
          edistr.str_cd IS NULL
          OR rtrim(ltrim(edistr.str_cd)) = ''
          )
        AND (
          mdsrtl.str_cd IS NULL
          OR rtrim(ltrim(mdsrtl.str_cd)) = ''
          )
        AND (
          mdsrtl.error_type != 2
          OR mdsrtl.error_type IS NULL
          OR rtrim(ltrim(mdsrtl.error_type)) = ''
          )
        THEN 'MANUAL'
      WHEN (
          edichng.rtlr_cd IS NULL
          OR rtrim(ltrim(edichng.rtlr_cd)) = ''
          )
        AND (
          edistr.str_cd IS NULL
          OR rtrim(ltrim(edistr.str_cd)) = ''
          )
        AND (
          mdsrtl.rtlr_cd IS NULL
          OR rtrim(ltrim(mdsrtl.rtlr_cd)) = ''
          )
        THEN 'MANUAL'
      ELSE '0'
      END AS exec_flag
  FROM wk_so_planet_no_dup nd
  LEFT JOIN edi_rtlr_cd_chng edichng ON nd.rtl_cd = edichng.rtlr_cd
    AND edichng.bgn_sndr_cd = nd.bgn_sndr_cd
  LEFT JOIN itg_mds_jp_mt_so_rtlr_chg mdsrtl ON nd.rtl_cd = mdsrtl.rtlr_cd
    AND mdsrtl.bgn_sndr_cd = nd.bgn_sndr_cd
  LEFT JOIN edi_store_m edistr ON nd.rtl_cd = edistr.str_cd
  ),
ct04
AS (
  SELECT a.jcp_rec_seq,
    'ERR_007' AS error_cd,
    CASE 
      WHEN (
          rtrim(ltrim(a.shp_date)) = ''
          OR a.shp_date IS NULL
          )
        THEN 'DELETE'
      ELSE CASE 
          WHEN to_date(a.shp_date, 'YYMMDD') > DATEADD(day, 14, CURRENT_DATE)
            THEN 'DELETE'
          ELSE '0'
          END
      END exec_flag
  FROM wk_so_planet_no_dup a
  ),
ct05
AS (
  SELECT a.jcp_rec_seq,
    'ERR_008' AS error_cd,
    CASE 
      WHEN a.trade_type IN (
          SELECT trade_type_jdnh
          FROM mt_trade_conv
          WHERE van_kbn = 'P'
            AND sell_inout_kbn = 'O'
          )
        THEN '0'
      ELSE 'DELETE'
      END exec_flag
  FROM wk_so_planet_no_dup a
  ),
ct06
AS (
  SELECT a.jcp_rec_seq,
    'ERR_009' AS error_cd,
    CASE 
      WHEN (
          rtrim(ltrim(a.item_cd)) = ''
          OR a.item_cd IS NULL
          )
        THEN 'DELETE'
      ELSE '0'
      END exec_flag
  FROM wk_so_planet_no_dup a
  ),
ct07
AS (
  SELECT nd.jcp_rec_seq,
    'ERR_012' error_cd,
    CASE 
      WHEN nd.item_cd_typ NOT IN ('J', 'j', 'K', 'k')
        THEN 'DELETE' --when the ws_cd in the transaction  data (in the no dup table) is null we want to exclude from the sell out report
      WHEN (
          edi.jan_cd_so IS NULL
          OR rtrim(ltrim(edi.jan_cd_so)) = ''
          )
        AND mdsitem.ext_jan_cd IS NOT NULL
        AND mdsitem.error_type = 3
        THEN 'DELETE'
      WHEN (
          edi.jan_cd_so IS NULL
          OR rtrim(ltrim(edi.jan_cd_so)) = ''
          )
        AND rtrim(ltrim(mdsitem.int_jan_cd)) != ''
        AND mdsitem.error_type IN (1, 2)
        THEN 'AUTOCORRECT'
      WHEN (
          edi.jan_cd_so IS NULL
          OR rtrim(ltrim(edi.jan_cd_so)) = ''
          )
        AND rtrim(ltrim(mdsitem.int_jan_cd)) != ''
        AND (
          mdsitem.error_type NOT IN (1, 2, 3)
          OR mdsitem.error_type IS NULL
          OR rtrim(ltrim(mdsitem.error_type)) = ''
          )
        THEN 'MANUAL'
      WHEN (
          edi.jan_cd_so IS NULL
          OR rtrim(ltrim(edi.jan_cd_so)) = ''
          )
        AND (
          mdsitem.int_jan_cd IS NULL
          OR rtrim(ltrim(mdsitem.int_jan_cd)) = ''
          )
        AND (
          mdsitem.error_type != 3
          OR mdsitem.error_type IS NULL
          OR rtrim(ltrim(mdsitem.error_type)) = ''
          )
        THEN 'MANUAL'
      WHEN (
          edi.jan_cd_so IS NULL
          OR rtrim(ltrim(edi.jan_cd_so)) = ''
          )
        AND (
          mdsitem.ext_jan_cd IS NULL
          OR rtrim(ltrim(mdsitem.ext_jan_cd)) = ''
          )
        THEN 'MANUAL'
      ELSE '0'
      END exec_flag
  FROM wk_so_planet_no_dup nd
  LEFT JOIN itg_mds_jp_mt_so_item_chg mdsitem ON nd.item_cd = mdsitem.ext_jan_cd
    AND nd.bgn_sndr_cd = mdsitem.bgn_sndr_cd
  LEFT JOIN edi_item_m edi ON nd.item_cd = edi.jan_cd_so
  WHERE nd.item_cd != ''
  ),
ct08
AS (
  SELECT a.jcp_rec_seq,
    'ERR_014' AS error_cd,
    CASE 
      WHEN b.lmp_dlt = '1'
        AND rtrim(ltrim(a.data_type)) IN ('R', 'P', 'r', 'p')
        THEN 'DELETE'
      ELSE '0'
      END exec_flag
  FROM wk_so_planet_no_dup a
  LEFT JOIN edi_bgn_sndr b ON b.bgn_sndr_cd = a.bgn_sndr_cd
  ),
ct09
AS (
  SELECT nd.jcp_rec_seq,
    'ERR_017' error_cd,
    CASE 
      WHEN (
          nd.ws_cd IS NULL
          OR rtrim(ltrim(nd.ws_cd)) = ''
          )
        THEN 'DELETE' --when the ws_cd in the transaction  data (in the no dup table) is null we want to exclude from the sell out report
      WHEN (
          jedp.int_partner_number IS NULL
          OR rtrim(ltrim(jedp.int_partner_number)) = ''
          )
        AND mdsws.error_type = 2
        THEN 'DELETE'
      WHEN (
          jedp.int_partner_number IS NULL
          OR rtrim(ltrim(jedp.int_partner_number)) = ''
          )
        AND rtrim(ltrim(mdsws.int_partner_number)) != ''
        AND mdsws.error_type = 1
        THEN 'AUTOCORRECT'
      WHEN (
          jedp.int_partner_number IS NULL
          OR rtrim(ltrim(jedp.int_partner_number)) = ''
          )
        AND rtrim(ltrim(mdsws.int_partner_number)) != ''
        AND (
          mdsws.error_type NOT IN (1, 2)
          OR mdsws.error_type IS NULL
          OR rtrim(ltrim(mdsws.error_type)) = ''
          )
        THEN 'MANUAL'
      WHEN (
          jedp.int_partner_number IS NULL
          OR rtrim(ltrim(jedp.int_partner_number)) = ''
          )
        AND (
          mdsws.int_partner_number IS NULL
          OR rtrim(ltrim(mdsws.int_partner_number)) = ''
          )
        AND (
          mdsws.error_type != 2
          OR mdsws.error_type IS NULL
          OR rtrim(ltrim(mdsws.error_type)) = ''
          )
        THEN 'MANUAL'
      WHEN (
          jedp.int_partner_number IS NULL
          OR rtrim(ltrim(jedp.int_partner_number)) = ''
          )
        AND (
          mdsws.ws_cd IS NULL
          OR rtrim(ltrim(mdsws.ws_cd)) = ''
          )
        THEN 'MANUAL'
      ELSE '0'
      END exec_flag
  FROM wk_so_planet_no_dup nd
  LEFT JOIN itg_mds_jp_mt_so_ws_chg mdsws ON nd.ws_cd = mdsws.ws_cd
    AND nd.bgn_sndr_cd = mdsws.bgn_sndr_cd
  LEFT JOIN edi_jedpar jedp ON nd.ws_cd = jedp.partner_customer_cd
  ),
ct10
AS (
  SELECT a.jcp_rec_seq,
    'ERR_018' AS error_cd,
    CASE 
      WHEN a.qty != '[[:digit:]]'
        AND (
          rtrim(ltrim(a.qty_type)) = ''
          OR a.qty_type IS NULL
          )
        THEN '0'
      ELSE 'DELETE'
      END AS exec_flag
  FROM wk_so_planet_no_dup a
  LEFT JOIN planet_no_dup_qty b ON a.jcp_rec_seq = b.jcp_rec_seq
  ),
ct11
AS (
  SELECT a.jcp_rec_seq,
    'ERR_020' AS error_cd,
    CASE 
      WHEN rtrim(ltrim(a.bgn_sndr_cd)) = '320633'
        AND rtrim(ltrim(a.rtl_cd)) = '456608A'
        AND rtrim(ltrim(a.price_type)) NOT IN ('K', 'k')
        THEN 'AUTOCORRECT'
      WHEN (
          rtrim(ltrim(a.price_type)) = ''
          OR a.price_type IS NULL
          )
        THEN 'AUTOCORRECT'
      WHEN rtrim(ltrim(a.price_type)) NOT IN ('T', 'K', 'P', 't', 'k', 'p')
        THEN 'DELETE'
      ELSE '0'
      END exec_flag
  FROM wk_so_planet_no_dup a
  ),
a
AS (
  SELECT *
  FROM ct01
  
  UNION
  
  SELECT *
  FROM ct02
  
  UNION
  
  SELECT *
  FROM ct03
  
  UNION
  
  SELECT *
  FROM ct04
  
  UNION
  
  SELECT *
  FROM ct05
  
  UNION
  
  SELECT *
  FROM ct06
  
  UNION
  
  SELECT *
  FROM ct07
  
  UNION
  
  SELECT *
  FROM ct08
  
  UNION
  
  SELECT *
  FROM ct09
  
  UNION
  
  SELECT *
  FROM ct10
  
  UNION
  
  SELECT *
  FROM ct11
  ),
trns
AS (
  SELECT a.jcp_rec_seq,
    a.error_cd,
    a.exec_flag
  FROM a
  WHERE a.exec_flag != '0'
    AND a.jcp_rec_seq NOT IN (
      SELECT jcp_rec_seq
      FROM dw_so_planet_err_cd
      WHERE error_cd = 'NRTL'
      )
  ),
final
AS (
  SELECT jcp_rec_seq::number(10, 0) AS jcp_rec_seq,
    error_cd::VARCHAR(50) AS error_cd,
    exec_flag::VARCHAR(50) AS exec_flag
  FROM trns
  )
SELECT *
FROM final