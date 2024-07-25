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
                    {% endif %}
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
	FROM {{ ref('jpnedw_integration__mt_cld') }}
	),
wk_tp_datamart_promo_amt
AS (
	SELECT *
	FROM {{ ref('jpnwks_integration__wk_tp_datamart_promo_amt') }}
	),
mt_constant 
as
(
    SELECT * FROM {{ ref('jpnedw_integration__mt_constant') }}
),
trns
AS (
	SELECT 'SO' as JCP_DATA_SOURCE,
		'Actual' as JCP_PLAN_TYPE,
		'Sell-Out' as JCP_ANALYSIS_TYPE,
		'Sell-Out' as JCP_DATA_CATEGORY,
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
			END as jcp_plan_item_type,
		CASE 
			WHEN 0 <= MONTHS_BETWEEN(T_IN.SHP_DATE, T_ITEM_M.REL_DT)
				AND MONTHS_BETWEEN(T_IN.SHP_DATE, T_ITEM_M.REL_DT) < 12
				THEN 'Y'
			ELSE 'N'
			END as jcp_new_item_type,
		CASE 
			WHEN T_IN.TRADE_TYPE IN ('11', '12', '31', '32', '51', '52')
				THEN '402000'
			WHEN T_IN.TRADE_TYPE IN ('21', '22', '41', '42')
				THEN '402098'
			ELSE NULL
			END as jcp_account,
		T_IN.JCP_CREATE_DATE,
        TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')) as jcp_load_date,
		CASE 
			WHEN T_IN.QTY <> 0
				THEN T_IN.JCP_NET_PRICE / T_IN.QTY
			ELSE NULL
			END as jcp_unit_prc,
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

final as
(
    SELECT
        JCP_DATA_SOURCE::VARCHAR(40) as JCP_DATA_SOURCE,
        JCP_PLAN_TYPE::VARCHAR(20) as JCP_PLAN_TYPE,
        JCP_ANALYSIS_TYPE::VARCHAR(40) as JCP_ANALYSIS_TYPE,
        JCP_DATA_CATEGORY::VARCHAR(40) as JCP_DATA_CATEGORY,
        SHP_DATE::TIMESTAMP_NTZ(9) as JCP_DATE,
        JCP_SHP_TO_CD::VARCHAR(40) as JCP_CSTM_CD,
        JCP_STR_CD::VARCHAR(40) as JCP_STR_CD,
        CHN_CD::VARCHAR(40) as JCP_CHN_CD,
        CHN_OFFC_CD::VARCHAR(40) as JCP_CHN_OFFC_CD,
        ITEM_CD::VARCHAR(40) as JCP_JAN_CD,
        JCP_PLAN_ITEM_TYPE::VARCHAR(2) as JCP_PLAN_ITEM_TYPE,
        JCP_NEW_ITEM_TYPE::VARCHAR(2) as JCP_NEW_ITEM_TYPE,
        JCP_ACCOUNT::VARCHAR(40) as JCP_ACCOUNT,
        JCP_CREATE_DATE::TIMESTAMP_NTZ(9) as JCP_CREATE_DATE,
        JCP_LOAD_DATE::TIMESTAMP_NTZ(9) as JCP_LOAD_DATE,
        JCP_UNIT_PRC::NUMBER(32,10) as JCP_UNIT_PRC,
        QTY::NUMBER(32,10) as JCP_QTY,
        JCP_NET_PRICE::NUMBER(32,10) as JCP_AMT,
        RCV_DT::TIMESTAMP_NTZ(9) as SO_RCV_DT,
        WS_CD::VARCHAR(32) as SO_WS_CD,
        RTL_TYPE::VARCHAR(2) as SO_RTL_TYPE,
        RTL_CD::VARCHAR(32) as SO_RTL_CD,
        TRADE_TYPE::VARCHAR(4) as SO_TRADE_TYPE,
        SHP_NUM::VARCHAR(20) as SO_SHP_NUM,
        TRADE_CD::VARCHAR(14) as SO_TRADE_CD,
        DEP_CD::VARCHAR(6) as SO_DEP_CD,
        CHG_CD::VARCHAR(2) as SO_CHG_CD,
        PERSON_IN_CHARGE::VARCHAR(10) as SO_PERSON_IN_CHARGE,
        PERSON_NAME::VARCHAR(30) as SO_PERSON_NAME,
        RTL_NAME::VARCHAR(160) as SO_RTL_NAME,
        RTL_HO_CD::VARCHAR(64) as SO_RTL_HO_CD,
        RTL_ADDRESS_CD::VARCHAR(30) as SO_RTL_ADDRESS_CD,
        DATA_TYPE::VARCHAR(2) as SO_DATA_TYPE,
        OPT_FLD::VARCHAR(2) as SO_OPT_FLD,
        ITEM_NM::VARCHAR(160) as SO_ITEM_NM,
        ITEM_CD_TYP::VARCHAR(2) as SO_ITEM_CD_TYP,
        QTY_TYPE::VARCHAR(2) as SO_QTY_TYPE,
        PRICE::NUMBER(32,10) as SO_PRICE,
        PRICE_TYPE::VARCHAR(2) as SO_PRICE_TYPE,
        SHP_WS_CD::VARCHAR(16) as SO_SHP_WS_CD,
        REP_NAME_KANJI::VARCHAR(60) as SO_REP_NAME_KANJI,
        REP_INFO::VARCHAR(28) as SO_REP_INFO,
        RTL_NAME_KANJI::VARCHAR(160) as SO_RTL_NAME_KANJI,
        ITEM_NM_KANJI::VARCHAR(160) as SO_ITEM_NM_KANJI,
        UNT_PRC::NUMBER(32,10) as SO_UNT_PRC,
        NET_PRC::NUMBER(32,10) as SO_NET_PRC,
        SALES_CHAN_TYPE::VARCHAR(2) as SO_SALES_CHAN_TYPE,
        NULL::TIMESTAMP_NTZ(9) as SI_CALDAY,
        NULL::VARCHAR(42) as SI_FISCPER,
        NULL::VARCHAR(108) as SI_MATERIAL,
        NULL::TIMESTAMP_NTZ(9) as SI_JCP_CREATE_DT,
        NULL::VARCHAR(14) as SI_JCP_PAN_FLG,
        NULL::TIMESTAMP_NTZ(9) as SI_JCP_445_YMD_DT,
        NULL::VARCHAR(2) as SI_JCP_UPDATE_FLG,
        NULL::TIMESTAMP_NTZ(9) as SI_JCP_UPDATE_DT,
        NULL::VARCHAR(40) as PN_CDDFC,
        NULL::VARCHAR(40) as PN_CDDMP,
        NULL::VARCHAR(2) as PN_ACCOUNT_SUB_CD,
        NULL::VARCHAR(400) as PN_PLAN_TYPE,
        NULL::VARCHAR(400) as PN_PROMOTION_NM,
        NULL::VARCHAR(400) as PN_ITEM_GROUP,
        NULL::VARCHAR(20) as TP_PROMO_CD,
        NULL::VARCHAR(160) as TP_PROMO_NM,
        NULL::VARCHAR(20) as TP_CSTM_CD,
        NULL::VARCHAR(2) as TP_VAL_FIXED_TYPE,
        NULL::NUMBER(12,0) as TP_VAL_FIXED_APPL_CNT,
        NULL::TIMESTAMP_NTZ(9) as TP_SAP_CNT_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_SAP_CANCEL_DT,
        NULL::VARCHAR(4) as TP_PROMO_STATUS_CD,
        NULL::VARCHAR(4) as TP_APPROVE_STATUS_CD,
        NULL::VARCHAR(4) as TP_RSLT_STATUS_CD,
        NULL::VARCHAR(4) as TP_FILE_STATUS_CD,
        NULL::VARCHAR(8) as TP_CSTCTR_CD,
        NULL::VARCHAR(20) as TP_TSP_ACNT_CD,
        NULL::VARCHAR(20) as TP_BME_PROMO_CD,
        NULL::NUMBER(24,0) as TP_UNIT_COST,
        NULL::VARCHAR(16) as TP_APL_CREATE_EMP_CD,
        NULL::VARCHAR(16) as TP_APL_APPLY_EMP_CD,
        NULL::VARCHAR(16) as TP_APL_APPROVE_EMP_CD,
        NULL::VARCHAR(10) as TP_APL_TARGET_CHN_CD,
        NULL::VARCHAR(20) as TP_APL_BRANCH_CD,
        NULL::VARCHAR(2) as TP_APL_PAYEE_TYP_CD,
        NULL::VARCHAR(2) as TP_APL_DIRECT_FLG,
        NULL::VARCHAR(2) as TP_APL_MIDST_APPL_FLG,
        NULL::VARCHAR(8) as TP_APL_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) as TP_APL_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_APL_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_APL_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_APL_APPLY_BEGIN_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_APL_APPLY_END_DT,
        NULL::VARCHAR(1024) as TP_APL_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) as TP_APL_UPDATE_DT,
        NULL::VARCHAR(16) as TP_APL_FIX_EMP_CD,
        NULL::TIMESTAMP_NTZ(9) as TP_APL_FIX_DT,
        NULL::VARCHAR(16) as TP_APL_UPDATE_EMP_CD,
        NULL::VARCHAR(2) as TP_APL_APPL_REL_FLG,
        NULL::VARCHAR(20) as TP_APLD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(40) as TP_APLD_CONTRACT_NO,
        NULL::TIMESTAMP_NTZ(9) as TP_APLD_UPDATE_DT,
        NULL::VARCHAR(16) as TP_RES_APPLY_EMP_CD,
        NULL::VARCHAR(16) as TP_RES_APPROVE_EMP_CD,
        NULL::VARCHAR(16) as TP_RES_RECOGNIZE_EMP_CD,
        NULL::VARCHAR(20) as TP_RES_PAYEE_CD,
        NULL::VARCHAR(20) as TP_RES_BRANCH_CD,
        NULL::VARCHAR(2) as TP_RES_PAYEE_TYP_CD,
        NULL::VARCHAR(2) as TP_RES_DIRECT_FLG,
        NULL::VARCHAR(8) as TP_RES_FISCAL_YEAR,
        NULL::TIMESTAMP_NTZ(9) as TP_RES_CREATE_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_RES_APPLY_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_RES_APPROVE_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_RES_RECOGNIZE_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_RES_PAYMENT_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_RES_PAYMENT_PLAN_DT,
        NULL::VARCHAR(20) as TP_RES_TRNSFR_DTL_CD,
        NULL::VARCHAR(1024) as TP_RES_COMMENT1,
        NULL::TIMESTAMP_NTZ(9) as TP_RES_UPDATE_DT,
        NULL::VARCHAR(2) as TP_RES_TAX_FLG,
        NULL::VARCHAR(20) as TP_RESD_CSTM_HEAD_OFFICE_CD,
        NULL::VARCHAR(2) as TP_RESD_PAYEE_TYP_CD,
        NULL::VARCHAR(40) as TP_RESD_CONTRACT_NO,
        NULL::VARCHAR(1024) as TP_RESD_COMMENT1,
        NULL::VARCHAR(2) as TP_RESD_DIRECT_DTL_FLG,
        NULL::TIMESTAMP_NTZ(9) as TP_RESD_SERVEY_TOTAL_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_RESD_RSLT_INPUT_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_RESD_CANCEL_DT,
        NULL::TIMESTAMP_NTZ(9) as TP_RESD_UPDATE_DT,
        NULL::VARCHAR(4) as TP_RESD_REASON_CD,
        NULL::NUMBER(8,2) as TP_RESD_TAX_RATE,
        NULL::NUMBER(20,0) as TP_RESD_TAX_AMT,
        NULL::VARCHAR(10) as PLNT,
        NULL::VARCHAR(10) as SALES_GRP,
        ID::NUMBER(10,0) as SO_ID,
        JCP_REC_SEQ::NUMBER(10,0) as SO_JCP_REC_SEQ
    from trns
)
SELECT *
FROM final