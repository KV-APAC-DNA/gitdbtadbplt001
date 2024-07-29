{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE
                    FROM {{this}} ab USING {{ ref('jpnedw_integration__mt_constant_range') }} mt
                    WHERE ab.JCP_DATA_SOURCE = 'SO'
                    AND ab.JCP_DATA_SOURCE = mt.IDENTIFY_CD
                    AND ab.JCP_DATE BETWEEN mt.MIN_DATE
                    AND mt.MAX_DATE;

                    UPDATE {{ ref('jpnedw_integration__mt_constant_range') }}
                    SET MAX_DATE = 
                    (
                        SELECT MAX(YMD_DT)
                        FROM {{ source('jpnedw_integration', 'mt_cld') }}
                        WHERE year_445 = (SELECT YEAR( current_timestamp()::timestamp_ntz(9)))
                    )
                    WHERE Identify_cd = 'SO';

                    UPDATE {{ ref('jpnedw_integration__mt_constant_range') }}
                    SET MIN_DATE = 
                    (
                        SELECT MAX(YMD_DT)
                        FROM {{ source('jpnedw_integration', 'mt_cld') }}
                        WHERE to_number(year_445) = 
                        (
                            SELECT YEAR( current_timestamp()::timestamp_ntz(9))) - (SELECT  DELETE_RANGE FROM {{ ref('jpnedw_integration__mt_constant_range') }}  WHERE IDENTIFY_CD='SO')
                        )
                    WHERE Identify_cd = 'SO';

                    DELETE
                    FROM {{this}} ab USING {{ ref('jpnedw_integration__mt_constant_range') }} mt
                    WHERE ab.JCP_DATA_SOURCE = 'SO'
                    AND ab.JCP_DATA_SOURCE = mt.IDENTIFY_CD
                    AND ab.JCP_DATE BETWEEN mt.MIN_DATE
                    AND mt.MAX_DATE;

                    DELETE
                    FROM {{this}} ab USING {{ ref('jpnedw_integration__mt_constant_range') }} mt
                    WHERE ab.JCP_DATA_SOURCE = 'SO'
                    AND ab.JCP_DATA_SOURCE = mt.IDENTIFY_CD
                    AND ab.JCP_DATE BETWEEN mt.MIN_DATE
                    AND mt.MAX_DATE;

                    UPDATE {{ ref('jpnedw_integration__mt_constant_range') }}
                    SET MAX_DATE = (
                            SELECT MAX(YMD_DT)
                            FROM {{ source('jpnedw_integration', 'mt_cld') }}
                            WHERE year_445 = (SELECT TO_NUMBER(RIGHT(IDENTIFY_VALUE,4),'9999') 
                    FROM {{ source('jpnedw_integration', 'mt_constant') }}
                    WHERE IDENTIFY_CD='JCP_PAN_FLG')
                            )
                    WHERE Identify_cd = 'SI';

                    UPDATE {{ ref('jpnedw_integration__mt_constant_range') }}
                    SET MIN_DATE = (
                            SELECT MIN(YMD_DT)
                            FROM {{ source('jpnedw_integration', 'mt_cld') }}
                            WHERE year_445 = (SELECT TO_NUMBER(RIGHT(IDENTIFY_VALUE,4),'9999') 
                    FROM {{ source('jpnedw_integration', 'mt_constant') }}
                    WHERE IDENTIFY_CD='JCP_PAN_FLG')
                            )
                    WHERE Identify_cd = 'SI';

                    DELETE
                    FROM {{this}} USING {{ ref('jpnedw_integration__mt_constant_range') }}
                    WHERE JCP_DATA_SOURCE = 'SI'
                        AND JCP_DATA_SOURCE = IDENTIFY_CD
                        AND JCP_DATE BETWEEN MIN_DATE
                            AND MAX_DATE;
                    {% endif %}
                    ",
        post_hook = "
                    UPDATE {{ ref('jpnedw_integration__mt_constant_seq') }}
                    SET MAX_VALUE=(SELECT MAX(JCP_REC_SEQ) FROM {{this}});
                    "
    )
}}

WITH dw_so_sell_out_dly
AS (
    SELECT *
    FROM {{ ref('jpnedw_integration__dw_so_sell_out_dly') }}
    ),
dw_si_sell_in_dly
AS (
    SELECT *
    FROM {{ ref('jpnedw_integration__dw_si_sell_in_dly') }}
    ),
edi_item_m
AS (
    SELECT *
    FROM {{ ref('jpnedw_integration__edi_item_m') }}
    ),
edi_store_m
AS (
    SELECT *
    FROM {{ ref('jpnedw_integration__edi_store_m') }}
    ),
edi_chn_m
AS (
    SELECT *
    FROM {{ ref('jpnedw_integration__edi_chn_m') }}
    ),
mt_constant_range
AS (
    SELECT *
    FROM {{ ref('jpnedw_integration__mt_constant_range') }}
    ),
mt_cld
AS (
    SELECT *
    FROM {{ source('jpnedw_integration', 'mt_cld') }}
    ),
wk_tp_datamart_promo_amt
AS (
    SELECT *
    FROM {{ ref('jpnwks_integration__wk_tp_datamart_promo_amt') }}
    ),
mt_constant
AS (
    SELECT *
    FROM {{ source('jpnedw_integration', 'mt_constant') }}
    ),
tmp1
AS (
    SELECT 'SO' AS JCP_DATA_SOURCE,
        'Actual' AS JCP_PLAN_TYPE,
        'Sell-Out' AS JCP_ANALYSIS_TYPE,
        'Sell-Out' AS JCP_DATA_CATEGORY,
        T_IN.SHP_DATE,
        T_IN.JCP_SHP_TO_CD,
        T_IN.JCP_STR_CD,
        T_STR_CHN.CHN_CD,
        T_STR_CHN.CHN_OFFC_CD,
        T_IN.ITEM_CD,
        CASE 
            WHEN T_ITEM_M.PROM_GOODS_FLG IS NOT NULL
                THEN T_ITEM_M.PROM_GOODS_FLG
            ELSE 'N'
            END AS jcp_plan_item_type,
        CASE 
            WHEN 0 <= MONTHS_BETWEEN(T_IN.SHP_DATE, T_ITEM_M.REL_DT)
                AND MONTHS_BETWEEN(T_IN.SHP_DATE, T_ITEM_M.REL_DT) < 12
                THEN 'Y'
            ELSE 'N'
            END AS jcp_new_item_type,
        CASE 
            WHEN T_IN.TRADE_TYPE IN ('11', '12', '31', '32', '51', '52')
                THEN '402000'
            WHEN T_IN.TRADE_TYPE IN ('21', '22', '41', '42')
                THEN '402098'
            ELSE NULL
            END AS jcp_account,
        T_IN.JCP_CREATE_DATE,
        TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')) AS jcp_load_date,
        CASE 
            WHEN T_IN.QTY <> 0
                THEN T_IN.JCP_NET_PRICE / T_IN.QTY
            ELSE NULL
            END AS jcp_unit_prc,
        T_IN.QTY,
        T_IN.JCP_NET_PRICE,
        T_IN.ID,
        T_IN.JCP_REC_SEQ,
        T_IN.RCV_DT,
        T_IN.WS_CD,
        T_IN.RTL_TYPE,
        T_IN.RTL_CD,
        T_IN.TRADE_TYPE,
        T_IN.SHP_NUM,
        T_IN.TRADE_CD,
        T_IN.DEP_CD,
        T_IN.CHG_CD,
        T_IN.PERSON_IN_CHARGE,
        T_IN.PERSON_NAME,
        T_IN.RTL_NAME,
        T_IN.RTL_HO_CD,
        T_IN.RTL_ADDRESS_CD,
        T_IN.DATA_TYPE,
        T_IN.OPT_FLD,
        T_IN.ITEM_NM,
        T_IN.ITEM_CD_TYP,
        T_IN.QTY_TYPE,
        T_IN.PRICE,
        T_IN.PRICE_TYPE,
        T_IN.SHP_WS_CD,
        T_IN.REP_NAME_KANJI,
        T_IN.REP_INFO,
        T_IN.RTL_NAME_KANJI,
        T_IN.ITEM_NM_KANJI,
        T_IN.UNT_PRC,
        T_IN.NET_PRC,
        T_IN.SALES_CHAN_TYPE
    FROM DW_SO_SELL_OUT_DLY T_IN
    LEFT OUTER JOIN (
        SELECT EDI_STORE_M.STR_CD,
            EDI_CHN_M.CHN_CD,
            EDI_CHN_M.CHN_OFFC_CD
        FROM EDI_STORE_M
        LEFT OUTER JOIN EDI_CHN_M ON EDI_STORE_M.CHN_CD = EDI_CHN_M.CHN_CD
        ) T_STR_CHN ON T_IN.JCP_STR_CD = T_STR_CHN.STR_CD
    LEFT OUTER JOIN EDI_ITEM_M T_ITEM_M ON T_IN.ITEM_CD = T_ITEM_M.JAN_CD_SO
    LEFT JOIN mt_constant_range mt
    WHERE T_IN.SHP_DATE BETWEEN mt.min_date
            AND mt.max_date
        AND BGN_SNDR_CD NOT LIKE 'PAN%'
    ),
insert1
AS (
    SELECT JCP_DATA_SOURCE::VARCHAR(40) AS JCP_DATA_SOURCE,
        JCP_PLAN_TYPE::VARCHAR(20) AS JCP_PLAN_TYPE,
        JCP_ANALYSIS_TYPE::VARCHAR(40) AS JCP_ANALYSIS_TYPE,
        JCP_DATA_CATEGORY::VARCHAR(40) AS JCP_DATA_CATEGORY,
        SHP_DATE::TIMESTAMP_NTZ(9) AS JCP_DATE,
        JCP_SHP_TO_CD::VARCHAR(40) AS JCP_CSTM_CD,
        JCP_STR_CD::VARCHAR(40) AS JCP_STR_CD,
        CHN_CD::VARCHAR(40) AS JCP_CHN_CD,
        CHN_OFFC_CD::VARCHAR(40) AS JCP_CHN_OFFC_CD,
        ITEM_CD::VARCHAR(40) AS JCP_JAN_CD,
        JCP_PLAN_ITEM_TYPE::VARCHAR(2) AS JCP_PLAN_ITEM_TYPE,
        JCP_NEW_ITEM_TYPE::VARCHAR(2) AS JCP_NEW_ITEM_TYPE,
        JCP_ACCOUNT::VARCHAR(40) AS JCP_ACCOUNT,
        JCP_CREATE_DATE::TIMESTAMP_NTZ(9) AS JCP_CREATE_DATE,
        JCP_LOAD_DATE::TIMESTAMP_NTZ(9) AS JCP_LOAD_DATE,
        JCP_UNIT_PRC::NUMBER(32, 10) AS JCP_UNIT_PRC,
        QTY::NUMBER(32, 10) AS JCP_QTY,
        JCP_NET_PRICE::NUMBER(32, 10) AS JCP_AMT,
        RCV_DT::TIMESTAMP_NTZ(9) AS SO_RCV_DT,
        WS_CD::VARCHAR(32) AS SO_WS_CD,
        RTL_TYPE::VARCHAR(2) AS SO_RTL_TYPE,
        RTL_CD::VARCHAR(32) AS SO_RTL_CD,
        TRADE_TYPE::VARCHAR(4) AS SO_TRADE_TYPE,
        SHP_NUM::VARCHAR(20) AS SO_SHP_NUM,
        TRADE_CD::VARCHAR(14) AS SO_TRADE_CD,
        DEP_CD::VARCHAR(6) AS SO_DEP_CD,
        CHG_CD::VARCHAR(2) AS SO_CHG_CD,
        PERSON_IN_CHARGE::VARCHAR(10) AS SO_PERSON_IN_CHARGE,
        PERSON_NAME::VARCHAR(30) AS SO_PERSON_NAME,
        RTL_NAME::VARCHAR(160) AS SO_RTL_NAME,
        RTL_HO_CD::VARCHAR(64) AS SO_RTL_HO_CD,
        RTL_ADDRESS_CD::VARCHAR(30) AS SO_RTL_ADDRESS_CD,
        DATA_TYPE::VARCHAR(2) AS SO_DATA_TYPE,
        OPT_FLD::VARCHAR(2) AS SO_OPT_FLD,
        ITEM_NM::VARCHAR(160) AS SO_ITEM_NM,
        ITEM_CD_TYP::VARCHAR(2) AS SO_ITEM_CD_TYP,
        QTY_TYPE::VARCHAR(2) AS SO_QTY_TYPE,
        PRICE::NUMBER(32, 10) AS SO_PRICE,
        PRICE_TYPE::VARCHAR(2) AS SO_PRICE_TYPE,
        SHP_WS_CD::VARCHAR(16) AS SO_SHP_WS_CD,
        REP_NAME_KANJI::VARCHAR(60) AS SO_REP_NAME_KANJI,
        REP_INFO::VARCHAR(28) AS SO_REP_INFO,
        RTL_NAME_KANJI::VARCHAR(160) AS SO_RTL_NAME_KANJI,
        ITEM_NM_KANJI::VARCHAR(160) AS SO_ITEM_NM_KANJI,
        UNT_PRC::NUMBER(32, 10) AS SO_UNT_PRC,
        NET_PRC::NUMBER(32, 10) AS SO_NET_PRC,
        SALES_CHAN_TYPE::VARCHAR(2) AS SO_SALES_CHAN_TYPE,
        NULL::TIMESTAMP_NTZ(9) AS SI_CALDAY,
        NULL::VARCHAR(42) AS SI_FISCPER,
        NULL::VARCHAR(108) AS SI_MATERIAL,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_CREATE_DT,
        NULL::VARCHAR(14) AS SI_JCP_PAN_FLG,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_445_YMD_DT,
        NULL::VARCHAR(2) AS SI_JCP_UPDATE_FLG,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_UPDATE_DT,
        NULL::VARCHAR(40) AS PN_CDDFC,
        NULL::VARCHAR(40) AS PN_CDDMP,
        NULL::VARCHAR(2) AS PN_ACCOUNT_SUB_CD,
        NULL::VARCHAR(400) AS PN_PLAN_TYPE,
        NULL::VARCHAR(400) AS PN_PROMOTION_NM,
        NULL::VARCHAR(400) AS PN_ITEM_GROUP,
        NULL::VARCHAR(20) AS TP_PROMO_CD,
        NULL::VARCHAR(160) AS TP_PROMO_NM,
        NULL::VARCHAR(20) AS TP_CSTM_CD,
        NULL::VARCHAR(2) AS TP_VAL_FIXED_TYPE,
        NULL::NUMBER(12, 0) AS TP_VAL_FIXED_APPL_CNT,
        NULL::TIMESTAMP_NTZ(9) AS TP_SAP_CNT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_SAP_CANCEL_DT,
        NULL::VARCHAR(4) AS TP_PROMO_STATUS_CD,
        NULL::VARCHAR(4) AS TP_APPROVE_STATUS_CD,
        NULL::VARCHAR(4) AS TP_RSLT_STATUS_CD,
        NULL::VARCHAR(4) AS TP_FILE_STATUS_CD,
        NULL::VARCHAR(8) AS TP_CSTCTR_CD,
        NULL::VARCHAR(20) AS TP_TSP_ACNT_CD,
        NULL::VARCHAR(20) AS TP_BME_PROMO_CD,
        NULL::NUMBER(24, 0) AS TP_UNIT_COST,
        NULL::VARCHAR(16) AS TP_APL_CREATE_EMP_CD,
        NULL::VARCHAR(16) AS TP_APL_APPLY_EMP_CD,
        NULL::VARCHAR(16) AS TP_APL_APPROVE_EMP_CD,
        NULL::VARCHAR(10) AS TP_APL_TARGET_CHN_CD,
        NULL::VARCHAR(20) AS TP_APL_BRANCH_CD,
        NULL::VARCHAR(2) AS TP_APL_PAYEE_TYP_CD,
        NULL::VARCHAR(2) AS TP_APL_DIRECT_FLG,
        NULL::VARCHAR(2) AS TP_APL_MIDST_APPL_FLG,
        NULL::VARCHAR(8) AS TP_APL_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_BEGIN_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_END_DT,
        NULL::VARCHAR(1024) AS TP_APL_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_UPDATE_DT,
        NULL::VARCHAR(16) AS TP_APL_FIX_EMP_CD,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_FIX_DT,
        NULL::VARCHAR(16) AS TP_APL_UPDATE_EMP_CD,
        NULL::VARCHAR(2) AS TP_APL_APPL_REL_FLG,
        NULL::VARCHAR(20) AS TP_APLD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(40) AS TP_APLD_CONTRACT_NO,
        NULL::TIMESTAMP_NTZ(9) AS TP_APLD_UPDATE_DT,
        NULL::VARCHAR(16) AS TP_RES_APPLY_EMP_CD,
        NULL::VARCHAR(16) AS TP_RES_APPROVE_EMP_CD,
        NULL::VARCHAR(16) AS TP_RES_RECOGNIZE_EMP_CD,
        NULL::VARCHAR(20) AS TP_RES_PAYEE_CD,
        NULL::VARCHAR(20) AS TP_RES_BRANCH_CD,
        NULL::VARCHAR(2) AS TP_RES_PAYEE_TYP_CD,
        NULL::VARCHAR(2) AS TP_RES_DIRECT_FLG,
        NULL::VARCHAR(8) AS TP_RES_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_RECOGNIZE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_PAYMENT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_PAYMENT_PLAN_DT,
        NULL::VARCHAR(20) AS TP_RES_TRNSFR_DTL_CD,
        NULL::VARCHAR(1024) AS TP_RES_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_UPDATE_DT,
        NULL::VARCHAR(2) AS TP_RES_TAX_FLG,
        NULL::VARCHAR(20) AS TP_RESD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(2) AS TP_RESD_PAYEE_TYP_CD,
        NULL::VARCHAR(40) AS TP_RESD_CONTRACT_NO,
        NULL::VARCHAR(1024) AS TP_RESD_COMMENT1,
        NULL::VARCHAR(2) AS TP_RESD_DIRECT_DTL_FLG,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_SERVEY_TOTAL_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_RSLT_INPUT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_CANCEL_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_UPDATE_DT,
        NULL::VARCHAR(4) AS TP_RESD_REASON_CD,
        NULL::NUMBER(8, 2) AS TP_RESD_TAX_RATE,
        NULL::NUMBER(20, 0) AS TP_RESD_TAX_AMT,
        NULL::VARCHAR(10) AS PLNT,
        NULL::VARCHAR(10) AS SALES_GRP,
        ID::NUMBER(10, 0) AS SO_ID,
        JCP_REC_SEQ::NUMBER(10, 0) AS SO_JCP_REC_SEQ
    FROM tmp1
    ),
tmp2
AS (
    SELECT 'SO' AS JCP_DATA_SOURCE,
        'Actual' AS JCP_PLAN_TYPE,
        'Sell-Out' AS JCP_ANALYSIS_TYPE,
        'Sell-Out' AS JCP_DATA_CATEGORY,
        t_in.shp_date,
        t_in.jcp_shp_to_cd,
        t_in.jcp_str_cd,
        t_str_chn.chn_cd,
        t_str_chn.chn_offc_cd,
        t_in.item_cd,
        CASE 
            WHEN t_item_m.prom_goods_flg IS NOT NULL
                THEN t_item_m.prom_goods_flg
            ELSE 'N'
            END AS jcp_plan_item_type,
        CASE 
            WHEN 0 <= months_between(t_in.shp_date, t_item_m.rel_dt)
                AND months_between(t_in.shp_date, t_item_m.rel_dt) < 12
                THEN 'Y'
            ELSE 'N'
            END AS jcp_new_item_type,
        CASE 
            WHEN t_in.trade_type IN ('11', '12', '31', '32', '51', '52')
                THEN '402000'
            WHEN t_in.trade_type IN ('21', '22', '41', '42')
                THEN '402098'
            ELSE NULL
            END AS jcp_account,
        T_IN.JCP_CREATE_DATE,
        TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')) AS jcp_load_date,
        CASE 
            WHEN t_in.qty <> 0
                THEN t_in.jcp_net_price / t_in.qty
            ELSE NULL
            END AS jcp_unit_prc,
        t_in.qty,
        t_in.jcp_net_price,
        t_in.rcv_dt,
        t_in.ws_cd,
        t_in.rtl_type,
        t_in.rtl_cd,
        t_in.trade_type,
        t_in.shp_num,
        t_in.trade_cd,
        t_in.dep_cd,
        t_in.chg_cd,
        t_in.person_in_charge,
        t_in.person_name,
        t_in.rtl_name,
        t_in.rtl_ho_cd,
        t_in.rtl_address_cd,
        t_in.data_type,
        t_in.opt_fld,
        t_in.item_nm,
        t_in.item_cd_typ,
        t_in.qty_type,
        t_in.price,
        t_in.price_type,
        t_in.shp_ws_cd,
        t_in.rep_name_kanji,
        t_in.rep_info,
        t_in.rtl_name_kanji,
        t_in.item_nm_kanji,
        t_in.unt_prc,
        t_in.net_prc,
        t_in.sales_chan_type
    FROM dw_so_sell_out_dly t_in
    LEFT JOIN mt_constant_range mt --ON t_in.shp_date BETWEEN mt.MIN_DATE AND mt.MAX_DATE
    LEFT OUTER JOIN (
        SELECT edi_store_m.str_cd,
            edi_chn_m.chn_cd,
            edi_chn_m.chn_offc_cd
        FROM edi_store_m
        LEFT OUTER JOIN edi_chn_m ON edi_store_m.chn_cd = edi_chn_m.chn_cd
        ) t_str_chn ON t_in.jcp_str_cd = t_str_chn.str_cd
    LEFT OUTER JOIN edi_item_m t_item_m ON t_in.item_cd = t_item_m.jan_cd_so
    WHERE t_in.shp_date BETWEEN mt.MIN_DATE
            AND mt.MAX_DATE
        AND bgn_sndr_cd NOT LIKE 'PAN%'
    ),
insert2
AS (
    SELECT JCP_DATA_SOURCE::VARCHAR(40) AS JCP_DATA_SOURCE,
        JCP_PLAN_TYPE::VARCHAR(20) AS JCP_PLAN_TYPE,
        JCP_ANALYSIS_TYPE::VARCHAR(40) AS JCP_ANALYSIS_TYPE,
        JCP_DATA_CATEGORY::VARCHAR(40) AS JCP_DATA_CATEGORY,
        SHP_DATE::TIMESTAMP_NTZ(9) AS JCP_DATE,
        JCP_SHP_TO_CD::VARCHAR(40) AS JCP_CSTM_CD,
        JCP_STR_CD::VARCHAR(40) AS JCP_STR_CD,
        CHN_CD::VARCHAR(40) AS JCP_CHN_CD,
        CHN_OFFC_CD::VARCHAR(40) AS JCP_CHN_OFFC_CD,
        ITEM_CD::VARCHAR(40) AS JCP_JAN_CD,
        JCP_PLAN_ITEM_TYPE::VARCHAR(2) AS JCP_PLAN_ITEM_TYPE,
        JCP_NEW_ITEM_TYPE::VARCHAR(2) AS JCP_NEW_ITEM_TYPE,
        JCP_ACCOUNT::VARCHAR(40) AS JCP_ACCOUNT,
        JCP_CREATE_DATE::TIMESTAMP_NTZ(9) AS JCP_CREATE_DATE,
        JCP_LOAD_DATE::TIMESTAMP_NTZ(9) AS JCP_LOAD_DATE,
        JCP_UNIT_PRC::NUMBER(32, 10) AS JCP_UNIT_PRC,
        QTY::NUMBER(32, 10) AS JCP_QTY,
        JCP_NET_PRICE::NUMBER(32, 10) AS JCP_AMT,
        RCV_DT::TIMESTAMP_NTZ(9) AS SO_RCV_DT,
        WS_CD::VARCHAR(32) AS SO_WS_CD,
        RTL_TYPE::VARCHAR(2) AS SO_RTL_TYPE,
        RTL_CD::VARCHAR(32) AS SO_RTL_CD,
        TRADE_TYPE::VARCHAR(4) AS SO_TRADE_TYPE,
        SHP_NUM::VARCHAR(20) AS SO_SHP_NUM,
        TRADE_CD::VARCHAR(14) AS SO_TRADE_CD,
        DEP_CD::VARCHAR(6) AS SO_DEP_CD,
        CHG_CD::VARCHAR(2) AS SO_CHG_CD,
        PERSON_IN_CHARGE::VARCHAR(10) AS SO_PERSON_IN_CHARGE,
        PERSON_NAME::VARCHAR(30) AS SO_PERSON_NAME,
        RTL_NAME::VARCHAR(160) AS SO_RTL_NAME,
        RTL_HO_CD::VARCHAR(64) AS SO_RTL_HO_CD,
        RTL_ADDRESS_CD::VARCHAR(30) AS SO_RTL_ADDRESS_CD,
        DATA_TYPE::VARCHAR(2) AS SO_DATA_TYPE,
        OPT_FLD::VARCHAR(2) AS SO_OPT_FLD,
        ITEM_NM::VARCHAR(160) AS SO_ITEM_NM,
        ITEM_CD_TYP::VARCHAR(2) AS SO_ITEM_CD_TYP,
        QTY_TYPE::VARCHAR(2) AS SO_QTY_TYPE,
        PRICE::NUMBER(32, 10) AS SO_PRICE,
        PRICE_TYPE::VARCHAR(2) AS SO_PRICE_TYPE,
        SHP_WS_CD::VARCHAR(16) AS SO_SHP_WS_CD,
        REP_NAME_KANJI::VARCHAR(60) AS SO_REP_NAME_KANJI,
        REP_INFO::VARCHAR(28) AS SO_REP_INFO,
        RTL_NAME_KANJI::VARCHAR(160) AS SO_RTL_NAME_KANJI,
        ITEM_NM_KANJI::VARCHAR(160) AS SO_ITEM_NM_KANJI,
        UNT_PRC::NUMBER(32, 10) AS SO_UNT_PRC,
        NET_PRC::NUMBER(32, 10) AS SO_NET_PRC,
        SALES_CHAN_TYPE::VARCHAR(2) AS SO_SALES_CHAN_TYPE,
        NULL::TIMESTAMP_NTZ(9) AS SI_CALDAY,
        NULL::VARCHAR(42) AS SI_FISCPER,
        NULL::VARCHAR(108) AS SI_MATERIAL,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_CREATE_DT,
        NULL::VARCHAR(14) AS SI_JCP_PAN_FLG,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_445_YMD_DT,
        NULL::VARCHAR(2) AS SI_JCP_UPDATE_FLG,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_UPDATE_DT,
        NULL::VARCHAR(40) AS PN_CDDFC,
        NULL::VARCHAR(40) AS PN_CDDMP,
        NULL::VARCHAR(2) AS PN_ACCOUNT_SUB_CD,
        NULL::VARCHAR(400) AS PN_PLAN_TYPE,
        NULL::VARCHAR(400) AS PN_PROMOTION_NM,
        NULL::VARCHAR(400) AS PN_ITEM_GROUP,
        NULL::VARCHAR(20) AS TP_PROMO_CD,
        NULL::VARCHAR(160) AS TP_PROMO_NM,
        NULL::VARCHAR(20) AS TP_CSTM_CD,
        NULL::VARCHAR(2) AS TP_VAL_FIXED_TYPE,
        NULL::NUMBER(12, 0) AS TP_VAL_FIXED_APPL_CNT,
        NULL::TIMESTAMP_NTZ(9) AS TP_SAP_CNT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_SAP_CANCEL_DT,
        NULL::VARCHAR(4) AS TP_PROMO_STATUS_CD,
        NULL::VARCHAR(4) AS TP_APPROVE_STATUS_CD,
        NULL::VARCHAR(4) AS TP_RSLT_STATUS_CD,
        NULL::VARCHAR(4) AS TP_FILE_STATUS_CD,
        NULL::VARCHAR(8) AS TP_CSTCTR_CD,
        NULL::VARCHAR(20) AS TP_TSP_ACNT_CD,
        NULL::VARCHAR(20) AS TP_BME_PROMO_CD,
        NULL::NUMBER(24, 0) AS TP_UNIT_COST,
        NULL::VARCHAR(16) AS TP_APL_CREATE_EMP_CD,
        NULL::VARCHAR(16) AS TP_APL_APPLY_EMP_CD,
        NULL::VARCHAR(16) AS TP_APL_APPROVE_EMP_CD,
        NULL::VARCHAR(10) AS TP_APL_TARGET_CHN_CD,
        NULL::VARCHAR(20) AS TP_APL_BRANCH_CD,
        NULL::VARCHAR(2) AS TP_APL_PAYEE_TYP_CD,
        NULL::VARCHAR(2) AS TP_APL_DIRECT_FLG,
        NULL::VARCHAR(2) AS TP_APL_MIDST_APPL_FLG,
        NULL::VARCHAR(8) AS TP_APL_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_BEGIN_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_END_DT,
        NULL::VARCHAR(1024) AS TP_APL_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_UPDATE_DT,
        NULL::VARCHAR(16) AS TP_APL_FIX_EMP_CD,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_FIX_DT,
        NULL::VARCHAR(16) AS TP_APL_UPDATE_EMP_CD,
        NULL::VARCHAR(2) AS TP_APL_APPL_REL_FLG,
        NULL::VARCHAR(20) AS TP_APLD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(40) AS TP_APLD_CONTRACT_NO,
        NULL::TIMESTAMP_NTZ(9) AS TP_APLD_UPDATE_DT,
        NULL::VARCHAR(16) AS TP_RES_APPLY_EMP_CD,
        NULL::VARCHAR(16) AS TP_RES_APPROVE_EMP_CD,
        NULL::VARCHAR(16) AS TP_RES_RECOGNIZE_EMP_CD,
        NULL::VARCHAR(20) AS TP_RES_PAYEE_CD,
        NULL::VARCHAR(20) AS TP_RES_BRANCH_CD,
        NULL::VARCHAR(2) AS TP_RES_PAYEE_TYP_CD,
        NULL::VARCHAR(2) AS TP_RES_DIRECT_FLG,
        NULL::VARCHAR(8) AS TP_RES_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_RECOGNIZE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_PAYMENT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_PAYMENT_PLAN_DT,
        NULL::VARCHAR(20) AS TP_RES_TRNSFR_DTL_CD,
        NULL::VARCHAR(1024) AS TP_RES_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_UPDATE_DT,
        NULL::VARCHAR(2) AS TP_RES_TAX_FLG,
        NULL::VARCHAR(20) AS TP_RESD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(2) AS TP_RESD_PAYEE_TYP_CD,
        NULL::VARCHAR(40) AS TP_RESD_CONTRACT_NO,
        NULL::VARCHAR(1024) AS TP_RESD_COMMENT1,
        NULL::VARCHAR(2) AS TP_RESD_DIRECT_DTL_FLG,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_SERVEY_TOTAL_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_RSLT_INPUT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_CANCEL_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_UPDATE_DT,
        NULL::VARCHAR(4) AS TP_RESD_REASON_CD,
        NULL::NUMBER(8, 2) AS TP_RESD_TAX_RATE,
        NULL::NUMBER(20, 0) AS TP_RESD_TAX_AMT,
        NULL::VARCHAR(10) AS PLNT,
        NULL::VARCHAR(10) AS SALES_GRP,
        NULL::NUMBER(10, 0) AS SO_ID,
        NULL::NUMBER(10, 0) AS SO_JCP_REC_SEQ
    FROM tmp2
    ),
tmp3
AS (
    SELECT 'SO' AS JCP_DATA_SOURCE,
        'Actual' AS JCP_PLAN_TYPE,
        'Sell-Out' AS JCP_ANALYSIS_TYPE,
        'Sell-Out' AS JCP_DATA_CATEGORY,
        T_IN.SHP_DATE,
        T_IN.JCP_SHP_TO_CD,
        T_IN.JCP_STR_CD,
        T_STR_CHN.CHN_CD,
        T_STR_CHN.CHN_OFFC_CD,
        T_IN.ITEM_CD,
        CASE 
            WHEN T_ITEM_M.PROM_GOODS_FLG IS NOT NULL
                THEN T_ITEM_M.PROM_GOODS_FLG
            ELSE 'N'
            END AS jcp_plan_item_type,
        CASE 
            WHEN 0 <= MONTHS_BETWEEN(T_IN.SHP_DATE, T_ITEM_M.REL_DT)
                AND MONTHS_BETWEEN(T_IN.SHP_DATE, T_ITEM_M.REL_DT) < 12
                THEN 'Y'
            ELSE 'N'
            END AS jcp_new_item_type,
        CASE 
            WHEN T_IN.TRADE_TYPE IN ('11', '12', '31', '32', '51', '52')
                THEN '402000'
            WHEN T_IN.TRADE_TYPE IN ('21', '22', '41', '42')
                THEN '402098'
            ELSE NULL
            END AS jcp_account,
        T_IN.JCP_CREATE_DATE,
        TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')) AS jcp_load_date,
        CASE 
            WHEN T_IN.QTY <> 0
                THEN T_IN.JCP_NET_PRICE / T_IN.QTY
            ELSE NULL
            END AS jcp_unit_prc,
        T_IN.QTY,
        T_IN.JCP_NET_PRICE,
        T_IN.ID,
        T_IN.JCP_REC_SEQ,
        T_IN.RCV_DT,
        T_IN.WS_CD,
        T_IN.RTL_TYPE,
        T_IN.RTL_CD,
        T_IN.TRADE_TYPE,
        T_IN.SHP_NUM,
        T_IN.TRADE_CD,
        T_IN.DEP_CD,
        T_IN.CHG_CD,
        T_IN.PERSON_IN_CHARGE,
        T_IN.PERSON_NAME,
        T_IN.RTL_NAME,
        T_IN.RTL_HO_CD,
        T_IN.RTL_ADDRESS_CD,
        T_IN.DATA_TYPE,
        T_IN.OPT_FLD,
        T_IN.ITEM_NM,
        T_IN.ITEM_CD_TYP,
        T_IN.QTY_TYPE,
        T_IN.PRICE,
        T_IN.PRICE_TYPE,
        T_IN.SHP_WS_CD,
        T_IN.REP_NAME_KANJI,
        T_IN.REP_INFO,
        T_IN.RTL_NAME_KANJI,
        T_IN.ITEM_NM_KANJI,
        T_IN.UNT_PRC,
        T_IN.NET_PRC,
        T_IN.SALES_CHAN_TYPE
    FROM DW_SO_SELL_OUT_DLY T_IN
    LEFT OUTER JOIN (
        SELECT EDI_STORE_M.STR_CD,
            EDI_CHN_M.CHN_CD,
            EDI_CHN_M.CHN_OFFC_CD
        FROM EDI_STORE_M
        LEFT OUTER JOIN EDI_CHN_M ON EDI_STORE_M.CHN_CD = EDI_CHN_M.CHN_CD
        ) T_STR_CHN ON T_IN.JCP_STR_CD = T_STR_CHN.STR_CD
    LEFT OUTER JOIN EDI_ITEM_M T_ITEM_M ON T_IN.ITEM_CD = T_ITEM_M.JAN_CD_SO
    LEFT JOIN mt_constant_range mt
    WHERE T_IN.SHP_DATE BETWEEN mt.min_date
            AND mt.max_date
        AND BGN_SNDR_CD NOT LIKE 'PAN%'
    ),
insert3
AS (
    SELECT JCP_DATA_SOURCE::VARCHAR(40) AS JCP_DATA_SOURCE,
        JCP_PLAN_TYPE::VARCHAR(20) AS JCP_PLAN_TYPE,
        JCP_ANALYSIS_TYPE::VARCHAR(40) AS JCP_ANALYSIS_TYPE,
        JCP_DATA_CATEGORY::VARCHAR(40) AS JCP_DATA_CATEGORY,
        SHP_DATE::TIMESTAMP_NTZ(9) AS JCP_DATE,
        JCP_SHP_TO_CD::VARCHAR(40) AS JCP_CSTM_CD,
        JCP_STR_CD::VARCHAR(40) AS JCP_STR_CD,
        CHN_CD::VARCHAR(40) AS JCP_CHN_CD,
        CHN_OFFC_CD::VARCHAR(40) AS JCP_CHN_OFFC_CD,
        ITEM_CD::VARCHAR(40) AS JCP_JAN_CD,
        JCP_PLAN_ITEM_TYPE::VARCHAR(2) AS JCP_PLAN_ITEM_TYPE,
        JCP_NEW_ITEM_TYPE::VARCHAR(2) AS JCP_NEW_ITEM_TYPE,
        JCP_ACCOUNT::VARCHAR(40) AS JCP_ACCOUNT,
        JCP_CREATE_DATE::TIMESTAMP_NTZ(9) AS JCP_CREATE_DATE,
        JCP_LOAD_DATE::TIMESTAMP_NTZ(9) AS JCP_LOAD_DATE,
        JCP_UNIT_PRC::NUMBER(32, 10) AS JCP_UNIT_PRC,
        QTY::NUMBER(32, 10) AS JCP_QTY,
        JCP_NET_PRICE::NUMBER(32, 10) AS JCP_AMT,
        RCV_DT::TIMESTAMP_NTZ(9) AS SO_RCV_DT,
        WS_CD::VARCHAR(32) AS SO_WS_CD,
        RTL_TYPE::VARCHAR(2) AS SO_RTL_TYPE,
        RTL_CD::VARCHAR(32) AS SO_RTL_CD,
        TRADE_TYPE::VARCHAR(4) AS SO_TRADE_TYPE,
        SHP_NUM::VARCHAR(20) AS SO_SHP_NUM,
        TRADE_CD::VARCHAR(14) AS SO_TRADE_CD,
        DEP_CD::VARCHAR(6) AS SO_DEP_CD,
        CHG_CD::VARCHAR(2) AS SO_CHG_CD,
        PERSON_IN_CHARGE::VARCHAR(10) AS SO_PERSON_IN_CHARGE,
        PERSON_NAME::VARCHAR(30) AS SO_PERSON_NAME,
        RTL_NAME::VARCHAR(160) AS SO_RTL_NAME,
        RTL_HO_CD::VARCHAR(64) AS SO_RTL_HO_CD,
        RTL_ADDRESS_CD::VARCHAR(30) AS SO_RTL_ADDRESS_CD,
        DATA_TYPE::VARCHAR(2) AS SO_DATA_TYPE,
        OPT_FLD::VARCHAR(2) AS SO_OPT_FLD,
        ITEM_NM::VARCHAR(160) AS SO_ITEM_NM,
        ITEM_CD_TYP::VARCHAR(2) AS SO_ITEM_CD_TYP,
        QTY_TYPE::VARCHAR(2) AS SO_QTY_TYPE,
        PRICE::NUMBER(32, 10) AS SO_PRICE,
        PRICE_TYPE::VARCHAR(2) AS SO_PRICE_TYPE,
        SHP_WS_CD::VARCHAR(16) AS SO_SHP_WS_CD,
        REP_NAME_KANJI::VARCHAR(60) AS SO_REP_NAME_KANJI,
        REP_INFO::VARCHAR(28) AS SO_REP_INFO,
        RTL_NAME_KANJI::VARCHAR(160) AS SO_RTL_NAME_KANJI,
        ITEM_NM_KANJI::VARCHAR(160) AS SO_ITEM_NM_KANJI,
        UNT_PRC::NUMBER(32, 10) AS SO_UNT_PRC,
        NET_PRC::NUMBER(32, 10) AS SO_NET_PRC,
        SALES_CHAN_TYPE::VARCHAR(2) AS SO_SALES_CHAN_TYPE,
        NULL::TIMESTAMP_NTZ(9) AS SI_CALDAY,
        NULL::VARCHAR(42) AS SI_FISCPER,
        NULL::VARCHAR(108) AS SI_MATERIAL,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_CREATE_DT,
        NULL::VARCHAR(14) AS SI_JCP_PAN_FLG,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_445_YMD_DT,
        NULL::VARCHAR(2) AS SI_JCP_UPDATE_FLG,
        NULL::TIMESTAMP_NTZ(9) AS SI_JCP_UPDATE_DT,
        NULL::VARCHAR(40) AS PN_CDDFC,
        NULL::VARCHAR(40) AS PN_CDDMP,
        NULL::VARCHAR(2) AS PN_ACCOUNT_SUB_CD,
        NULL::VARCHAR(400) AS PN_PLAN_TYPE,
        NULL::VARCHAR(400) AS PN_PROMOTION_NM,
        NULL::VARCHAR(400) AS PN_ITEM_GROUP,
        NULL::VARCHAR(20) AS TP_PROMO_CD,
        NULL::VARCHAR(160) AS TP_PROMO_NM,
        NULL::VARCHAR(20) AS TP_CSTM_CD,
        NULL::VARCHAR(2) AS TP_VAL_FIXED_TYPE,
        NULL::NUMBER(12, 0) AS TP_VAL_FIXED_APPL_CNT,
        NULL::TIMESTAMP_NTZ(9) AS TP_SAP_CNT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_SAP_CANCEL_DT,
        NULL::VARCHAR(4) AS TP_PROMO_STATUS_CD,
        NULL::VARCHAR(4) AS TP_APPROVE_STATUS_CD,
        NULL::VARCHAR(4) AS TP_RSLT_STATUS_CD,
        NULL::VARCHAR(4) AS TP_FILE_STATUS_CD,
        NULL::VARCHAR(8) AS TP_CSTCTR_CD,
        NULL::VARCHAR(20) AS TP_TSP_ACNT_CD,
        NULL::VARCHAR(20) AS TP_BME_PROMO_CD,
        NULL::NUMBER(24, 0) AS TP_UNIT_COST,
        NULL::VARCHAR(16) AS TP_APL_CREATE_EMP_CD,
        NULL::VARCHAR(16) AS TP_APL_APPLY_EMP_CD,
        NULL::VARCHAR(16) AS TP_APL_APPROVE_EMP_CD,
        NULL::VARCHAR(10) AS TP_APL_TARGET_CHN_CD,
        NULL::VARCHAR(20) AS TP_APL_BRANCH_CD,
        NULL::VARCHAR(2) AS TP_APL_PAYEE_TYP_CD,
        NULL::VARCHAR(2) AS TP_APL_DIRECT_FLG,
        NULL::VARCHAR(2) AS TP_APL_MIDST_APPL_FLG,
        NULL::VARCHAR(8) AS TP_APL_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_BEGIN_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_END_DT,
        NULL::VARCHAR(1024) AS TP_APL_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_UPDATE_DT,
        NULL::VARCHAR(16) AS TP_APL_FIX_EMP_CD,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_FIX_DT,
        NULL::VARCHAR(16) AS TP_APL_UPDATE_EMP_CD,
        NULL::VARCHAR(2) AS TP_APL_APPL_REL_FLG,
        NULL::VARCHAR(20) AS TP_APLD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(40) AS TP_APLD_CONTRACT_NO,
        NULL::TIMESTAMP_NTZ(9) AS TP_APLD_UPDATE_DT,
        NULL::VARCHAR(16) AS TP_RES_APPLY_EMP_CD,
        NULL::VARCHAR(16) AS TP_RES_APPROVE_EMP_CD,
        NULL::VARCHAR(16) AS TP_RES_RECOGNIZE_EMP_CD,
        NULL::VARCHAR(20) AS TP_RES_PAYEE_CD,
        NULL::VARCHAR(20) AS TP_RES_BRANCH_CD,
        NULL::VARCHAR(2) AS TP_RES_PAYEE_TYP_CD,
        NULL::VARCHAR(2) AS TP_RES_DIRECT_FLG,
        NULL::VARCHAR(8) AS TP_RES_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_RECOGNIZE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_PAYMENT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_PAYMENT_PLAN_DT,
        NULL::VARCHAR(20) AS TP_RES_TRNSFR_DTL_CD,
        NULL::VARCHAR(1024) AS TP_RES_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_UPDATE_DT,
        NULL::VARCHAR(2) AS TP_RES_TAX_FLG,
        NULL::VARCHAR(20) AS TP_RESD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(2) AS TP_RESD_PAYEE_TYP_CD,
        NULL::VARCHAR(40) AS TP_RESD_CONTRACT_NO,
        NULL::VARCHAR(1024) AS TP_RESD_COMMENT1,
        NULL::VARCHAR(2) AS TP_RESD_DIRECT_DTL_FLG,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_SERVEY_TOTAL_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_RSLT_INPUT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_CANCEL_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_UPDATE_DT,
        NULL::VARCHAR(4) AS TP_RESD_REASON_CD,
        NULL::NUMBER(8, 2) AS TP_RESD_TAX_RATE,
        NULL::NUMBER(20, 0) AS TP_RESD_TAX_AMT,
        NULL::VARCHAR(10) AS PLNT,
        NULL::VARCHAR(10) AS SALES_GRP,
        ID::NUMBER(10, 0) AS SO_ID,
        JCP_REC_SEQ::NUMBER(10, 0) AS SO_JCP_REC_SEQ
    FROM tmp3
    ),
tmp4
AS (
    SELECT 'SI' AS JCP_DATA_SOURCE,
        '"Actual' AS JCP_PLAN_TYPE,
        'Sell-In' AS JCP_ANALYSIS_TYPE,
        'Sell-In' AS JCP_DATA_CATEGORY,
        CASE 
            WHEN T_IN.CALDAY < T_IN.JCP_445_YMD_DT
                THEN T_IN.JCP_445_YMD_DT
            ELSE T_IN.CALDAY
            END AS jcp_date,
        T_IN.CUSTOMER AS jcp_cstm_cd,
        T_ITEM_M.JAN_CD,
        CASE 
            WHEN T_ITEM_M.PROM_GOODS_FLG IS NOT NULL
                THEN T_ITEM_M.PROM_GOODS_FLG
            ELSE 'N'
            END AS jcp_plan_item_type,
        CASE 
            WHEN T_IN.CALDAY < T_IN.JCP_445_YMD_DT
                THEN CASE 
                        WHEN 0 <= MONTHS_BETWEEN(T_IN.JCP_445_YMD_DT, T_ITEM_M.REL_DT)
                            AND MONTHS_BETWEEN(T_IN.JCP_445_YMD_DT, T_ITEM_M.REL_DT) < 12
                            THEN 'Y'
                        ELSE 'N'
                        END
            ELSE CASE 
                    WHEN 0 <= MONTHS_BETWEEN(T_IN.CALDAY, T_ITEM_M.REL_DT)
                        AND MONTHS_BETWEEN(T_IN.CALDAY, T_ITEM_M.REL_DT) < 12
                        THEN 'Y'
                    ELSE 'N'
                    END
            END AS jcp_new_item_type,
        T_IN.ACCOUNT,
        T_IN.JCP_CREATE_DT,
        TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')) AS jcp_load_date,
        ABS(T_IN.JCP_UNT_PRC) AS JCP_UNT_PRC,
        T_IN.JCP_QTY * (- 1) AS JCP_QTY,
        T_IN.AMOCCC * (- 1) AS JCP_AMT,
        T_IN.CALDAY AS si_calday,
        T_IN.FISCPER AS si_fiscper,
        T_IN.MATERIAL AS si_material,
        T_IN.JCP_CREATE_DT AS SI_JCP_CREATE_DT,
        T_IN.JCP_PAN_FLG,
        T_IN.JCP_445_YMD_DT,
        T_IN.JCP_UPDATE_FLG,
        T_IN.JCP_UPDATE_DT,
        T_IN.plnt,
        T_IN.SALES_GRP
    FROM DW_SI_SELL_IN_DLY T_IN
    LEFT OUTER JOIN EDI_ITEM_M T_ITEM_M ON T_IN.MATERIAL = T_ITEM_M.ITEM_CD
    WHERE JCP_PAN_FLG = (
            SELECT IDENTIFY_VALUE
            FROM MT_CONSTANT
            WHERE IDENTIFY_CD = 'JCP_PAN_FLG'
                AND DELETE_FLAG = '0'
            )
    ),
insert4
AS (
    SELECT JCP_DATA_SOURCE::VARCHAR(40) AS JCP_DATA_SOURCE,
        JCP_PLAN_TYPE::VARCHAR(20) AS JCP_PLAN_TYPE,
        JCP_ANALYSIS_TYPE::VARCHAR(40) AS JCP_ANALYSIS_TYPE,
        JCP_DATA_CATEGORY::VARCHAR(40) AS JCP_DATA_CATEGORY,
        JCP_DATE::TIMESTAMP_NTZ(9) AS JCP_DATE,
        JCP_CSTM_CD::VARCHAR(40) AS JCP_CSTM_CD,
        NULL::VARCHAR(40) AS JCP_STR_CD,
        NULL::VARCHAR(40) AS JCP_CHN_CD,
        NULL::VARCHAR(40) AS JCP_CHN_OFFC_CD,
        JAN_CD::VARCHAR(40) AS JCP_JAN_CD,
        JCP_PLAN_ITEM_TYPE::VARCHAR(2) AS JCP_PLAN_ITEM_TYPE,
        JCP_NEW_ITEM_TYPE::VARCHAR(2) AS JCP_NEW_ITEM_TYPE,
        ACCOUNT::VARCHAR(40) AS JCP_ACCOUNT,
        JCP_CREATE_DT::TIMESTAMP_NTZ(9) AS JCP_CREATE_DATE,
        JCP_LOAD_DATE::TIMESTAMP_NTZ(9) AS JCP_LOAD_DATE,
        JCP_UNT_PRC::NUMBER(32, 10) AS JCP_UNIT_PRC,
        JCP_QTY::NUMBER(32, 10) AS JCP_QTY,
        JCP_AMT::NUMBER(32, 10) AS JCP_AMT,
        NULL::TIMESTAMP_NTZ(9) AS SO_RCV_DT,
        NULL::VARCHAR(32) AS SO_WS_CD,
        NULL::VARCHAR(2) AS SO_RTL_TYPE,
        NULL::VARCHAR(32) AS SO_RTL_CD,
        NULL::VARCHAR(4) AS SO_TRADE_TYPE,
        NULL::VARCHAR(20) AS SO_SHP_NUM,
        NULL::VARCHAR(14) AS SO_TRADE_CD,
        NULL::VARCHAR(6) AS SO_DEP_CD,
        NULL::VARCHAR(2) AS SO_CHG_CD,
        NULL::VARCHAR(10) AS SO_PERSON_IN_CHARGE,
        NULL::VARCHAR(30) AS SO_PERSON_NAME,
        NULL::VARCHAR(160) AS SO_RTL_NAME,
        NULL::VARCHAR(64) AS SO_RTL_HO_CD,
        NULL::VARCHAR(30) AS SO_RTL_ADDRESS_CD,
        NULL::VARCHAR(2) AS SO_DATA_TYPE,
        NULL::VARCHAR(2) AS SO_OPT_FLD,
        NULL::VARCHAR(160) AS SO_ITEM_NM,
        NULL::VARCHAR(2) AS SO_ITEM_CD_TYP,
        NULL::VARCHAR(2) AS SO_QTY_TYPE,
        NULL::NUMBER(32, 10) AS SO_PRICE,
        NULL::VARCHAR(2) AS SO_PRICE_TYPE,
        NULL::VARCHAR(16) AS SO_SHP_WS_CD,
        NULL::VARCHAR(60) AS SO_REP_NAME_KANJI,
        NULL::VARCHAR(28) AS SO_REP_INFO,
        NULL::VARCHAR(160) AS SO_RTL_NAME_KANJI,
        NULL::VARCHAR(160) AS SO_ITEM_NM_KANJI,
        NULL::NUMBER(32, 10) AS SO_UNT_PRC,
        NULL::NUMBER(32, 10) AS SO_NET_PRC,
        NULL::VARCHAR(2) AS SO_SALES_CHAN_TYPE,
        SI_CALDAY::TIMESTAMP_NTZ(9) AS SI_CALDAY,
        SI_FISCPER::VARCHAR(42) AS SI_FISCPER,
        SI_MATERIAL::VARCHAR(108) AS SI_MATERIAL,
        SI_JCP_CREATE_DT::TIMESTAMP_NTZ(9) AS SI_JCP_CREATE_DT,
        JCP_PAN_FLG::VARCHAR(14) AS SI_JCP_PAN_FLG,
        JCP_445_YMD_DT::TIMESTAMP_NTZ(9) AS SI_JCP_445_YMD_DT,
        JCP_UPDATE_FLG::VARCHAR(2) AS SI_JCP_UPDATE_FLG,
        JCP_UPDATE_DT::TIMESTAMP_NTZ(9) AS SI_JCP_UPDATE_DT,
        NULL::VARCHAR(40) AS PN_CDDFC,
        NULL::VARCHAR(40) AS PN_CDDMP,
        NULL::VARCHAR(2) AS PN_ACCOUNT_SUB_CD,
        NULL::VARCHAR(400) AS PN_PLAN_TYPE,
        NULL::VARCHAR(400) AS PN_PROMOTION_NM,
        NULL::VARCHAR(400) AS PN_ITEM_GROUP,
        NULL::VARCHAR(20) AS TP_PROMO_CD,
        NULL::VARCHAR(160) AS TP_PROMO_NM,
        NULL::VARCHAR(20) AS TP_CSTM_CD,
        NULL::VARCHAR(2) AS TP_VAL_FIXED_TYPE,
        NULL::NUMBER(12, 0) AS TP_VAL_FIXED_APPL_CNT,
        NULL::TIMESTAMP_NTZ(9) AS TP_SAP_CNT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_SAP_CANCEL_DT,
        NULL::VARCHAR(4) AS TP_PROMO_STATUS_CD,
        NULL::VARCHAR(4) AS TP_APPROVE_STATUS_CD,
        NULL::VARCHAR(4) AS TP_RSLT_STATUS_CD,
        NULL::VARCHAR(4) AS TP_FILE_STATUS_CD,
        NULL::VARCHAR(8) AS TP_CSTCTR_CD,
        NULL::VARCHAR(20) AS TP_TSP_ACNT_CD,
        NULL::VARCHAR(20) AS TP_BME_PROMO_CD,
        NULL::NUMBER(24, 0) AS TP_UNIT_COST,
        NULL::VARCHAR(16) AS TP_APL_CREATE_EMP_CD,
        NULL::VARCHAR(16) AS TP_APL_APPLY_EMP_CD,
        NULL::VARCHAR(16) AS TP_APL_APPROVE_EMP_CD,
        NULL::VARCHAR(10) AS TP_APL_TARGET_CHN_CD,
        NULL::VARCHAR(20) AS TP_APL_BRANCH_CD,
        NULL::VARCHAR(2) AS TP_APL_PAYEE_TYP_CD,
        NULL::VARCHAR(2) AS TP_APL_DIRECT_FLG,
        NULL::VARCHAR(2) AS TP_APL_MIDST_APPL_FLG,
        NULL::VARCHAR(8) AS TP_APL_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_BEGIN_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_APPLY_END_DT,
        NULL::VARCHAR(1024) AS TP_APL_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_UPDATE_DT,
        NULL::VARCHAR(16) AS TP_APL_FIX_EMP_CD,
        NULL::TIMESTAMP_NTZ(9) AS TP_APL_FIX_DT,
        NULL::VARCHAR(16) AS TP_APL_UPDATE_EMP_CD,
        NULL::VARCHAR(2) AS TP_APL_APPL_REL_FLG,
        NULL::VARCHAR(20) AS TP_APLD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(40) AS TP_APLD_CONTRACT_NO,
        NULL::TIMESTAMP_NTZ(9) AS TP_APLD_UPDATE_DT,
        NULL::VARCHAR(16) AS TP_RES_APPLY_EMP_CD,
        NULL::VARCHAR(16) AS TP_RES_APPROVE_EMP_CD,
        NULL::VARCHAR(16) AS TP_RES_RECOGNIZE_EMP_CD,
        NULL::VARCHAR(20) AS TP_RES_PAYEE_CD,
        NULL::VARCHAR(20) AS TP_RES_BRANCH_CD,
        NULL::VARCHAR(2) AS TP_RES_PAYEE_TYP_CD,
        NULL::VARCHAR(2) AS TP_RES_DIRECT_FLG,
        NULL::VARCHAR(8) AS TP_RES_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_RECOGNIZE_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_PAYMENT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_PAYMENT_PLAN_DT,
        NULL::VARCHAR(20) AS TP_RES_TRNSFR_DTL_CD,
        NULL::VARCHAR(1024) AS TP_RES_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) AS TP_RES_UPDATE_DT,
        NULL::VARCHAR(2) AS TP_RES_TAX_FLG,
        NULL::VARCHAR(20) AS TP_RESD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(2) AS TP_RESD_PAYEE_TYP_CD,
        NULL::VARCHAR(40) AS TP_RESD_CONTRACT_NO,
        NULL::VARCHAR(1024) AS TP_RESD_COMMENT1,
        NULL::VARCHAR(2) AS TP_RESD_DIRECT_DTL_FLG,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_SERVEY_TOTAL_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_RSLT_INPUT_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_CANCEL_DT,
        NULL::TIMESTAMP_NTZ(9) AS TP_RESD_UPDATE_DT,
        NULL::VARCHAR(4) AS TP_RESD_REASON_CD,
        NULL::NUMBER(8, 2) AS TP_RESD_TAX_RATE,
        NULL::NUMBER(20, 0) AS TP_RESD_TAX_AMT,
        PLNT::VARCHAR(10) AS PLNT,
        SALES_GRP::VARCHAR(10) AS SALES_GRP,
        NULL::NUMBER(10, 0) AS SO_ID,
        NULL::NUMBER(10, 0) AS SO_JCP_REC_SEQ
    FROM tmp4
    ),
final
AS (
    SELECT *
    FROM insert1
    
    UNION ALL
    
    SELECT *
    FROM insert2
    
    UNION ALL
    
    SELECT *
    FROM insert3
    
    UNION ALL
    
    SELECT *
    FROM insert4
    )
SELECT *
FROM final