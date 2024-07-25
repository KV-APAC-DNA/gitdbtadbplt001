{{
    config
    (
        pre_hook = "
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
                    FROM {{ ref('jpnwks_integration__dm_integration_dly_tmp3') }} USING {{ ref('jpnedw_integration__mt_constant_range') }}
                    WHERE JCP_DATA_SOURCE = 'SI'
                        AND JCP_DATA_SOURCE = IDENTIFY_CD
                        AND JCP_DATE BETWEEN MIN_DATE
                            AND MAX_DATE;
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
as
(
    SELECT * FROM {{ source('jpnedw_integration', 'mt_constant') }}
),

tmp3 AS
(
    select * from {{ ref('jpnwks_integration__dm_integration_dly_tmp3') }}
),

trns
AS  (
	SELECT 'SI' as JCP_DATA_SOURCE,
		'"Actual' as JCP_PLAN_TYPE,
		'Sell-In' as JCP_ANALYSIS_TYPE,
		'Sell-In' as JCP_DATA_CATEGORY,
		CASE 
			WHEN T_IN.CALDAY < T_IN.JCP_445_YMD_DT
				THEN T_IN.JCP_445_YMD_DT
			ELSE T_IN.CALDAY
			END as jcp_date,
		T_IN.CUSTOMER as jcp_cstm_cd,
		T_ITEM_M.JAN_CD,
		CASE 
			WHEN T_ITEM_M.PROM_GOODS_FLG IS NOT NULL
				THEN T_ITEM_M.PROM_GOODS_FLG
			ELSE 'N'
			END as jcp_plan_item_type,
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
			END as jcp_new_item_type,
		T_IN.ACCOUNT,
        T_IN.JCP_CREATE_DT,
		TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')) as jcp_load_date,
		ABS(T_IN.JCP_UNT_PRC) as JCP_UNT_PRC,
		T_IN.JCP_QTY * (- 1) as JCP_QTY,
		T_IN.AMOCCC * (- 1) as JCP_AMT,
		T_IN.CALDAY as si_calday,
		T_IN.FISCPER as si_fiscper,
		T_IN.MATERIAL as si_material,
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
tmp4 as
(   
    SELECT
		JCP_DATA_SOURCE::VARCHAR(40) as JCP_DATA_SOURCE,
		JCP_PLAN_TYPE::VARCHAR(20) as JCP_PLAN_TYPE,
		JCP_ANALYSIS_TYPE::VARCHAR(40) as JCP_ANALYSIS_TYPE,
		JCP_DATA_CATEGORY::VARCHAR(40) as JCP_DATA_CATEGORY,
		JCP_DATE::TIMESTAMP_NTZ(9) as JCP_DATE,
		JCP_CSTM_CD::VARCHAR(40) as JCP_CSTM_CD,
		NULL::VARCHAR(40) as JCP_STR_CD,
		NULL::VARCHAR(40) as JCP_CHN_CD,
		NULL::VARCHAR(40) as JCP_CHN_OFFC_CD,
		JAN_CD::VARCHAR(40) as JCP_JAN_CD,
		JCP_PLAN_ITEM_TYPE::VARCHAR(2) as JCP_PLAN_ITEM_TYPE,
		JCP_NEW_ITEM_TYPE::VARCHAR(2) as JCP_NEW_ITEM_TYPE,
		ACCOUNT::VARCHAR(40) as JCP_ACCOUNT,
		JCP_CREATE_DT::TIMESTAMP_NTZ(9) as JCP_CREATE_DATE,
		JCP_LOAD_DATE::TIMESTAMP_NTZ(9) as JCP_LOAD_DATE,
		JCP_UNT_PRC::NUMBER(32,10) as JCP_UNIT_PRC,
		JCP_QTY::NUMBER(32,10) as JCP_QTY,
		JCP_AMT::NUMBER(32,10) as JCP_AMT,
		NULL::TIMESTAMP_NTZ(9) as SO_RCV_DT,
		NULL::VARCHAR(32) as SO_WS_CD,
		NULL::VARCHAR(2) as SO_RTL_TYPE,
		NULL::VARCHAR(32) as SO_RTL_CD,
		NULL::VARCHAR(4) as SO_TRADE_TYPE,
		NULL::VARCHAR(20) as SO_SHP_NUM,
		NULL::VARCHAR(14) as SO_TRADE_CD,
		NULL::VARCHAR(6) as SO_DEP_CD,
		NULL::VARCHAR(2) as SO_CHG_CD,
		NULL::VARCHAR(10) as SO_PERSON_IN_CHARGE,
		NULL::VARCHAR(30) as SO_PERSON_NAME,
		NULL::VARCHAR(160) as SO_RTL_NAME,
		NULL::VARCHAR(64) as SO_RTL_HO_CD,
		NULL::VARCHAR(30) as SO_RTL_ADDRESS_CD,
		NULL::VARCHAR(2) as SO_DATA_TYPE,
		NULL::VARCHAR(2) as SO_OPT_FLD,
		NULL::VARCHAR(160) as SO_ITEM_NM,
		NULL::VARCHAR(2) as SO_ITEM_CD_TYP,
		NULL::VARCHAR(2) as SO_QTY_TYPE,
		NULL::NUMBER(32,10) as SO_PRICE,
		NULL::VARCHAR(2) as SO_PRICE_TYPE,
		NULL::VARCHAR(16) as SO_SHP_WS_CD,
		NULL::VARCHAR(60) as SO_REP_NAME_KANJI,
		NULL::VARCHAR(28) as SO_REP_INFO,
		NULL::VARCHAR(160) as SO_RTL_NAME_KANJI,
		NULL::VARCHAR(160) as SO_ITEM_NM_KANJI,
		NULL::NUMBER(32,10) as SO_UNT_PRC,
		NULL::NUMBER(32,10) as SO_NET_PRC,
		NULL::VARCHAR(2) as SO_SALES_CHAN_TYPE,
		SI_CALDAY::TIMESTAMP_NTZ(9) as SI_CALDAY,
		SI_FISCPER::VARCHAR(42) as SI_FISCPER,
		SI_MATERIAL::VARCHAR(108) as SI_MATERIAL,
		SI_JCP_CREATE_DT::TIMESTAMP_NTZ(9) as SI_JCP_CREATE_DT,
		JCP_PAN_FLG::VARCHAR(14) as SI_JCP_PAN_FLG,
		JCP_445_YMD_DT::TIMESTAMP_NTZ(9) as SI_JCP_445_YMD_DT,
		JCP_UPDATE_FLG::VARCHAR(2) as SI_JCP_UPDATE_FLG,
		JCP_UPDATE_DT::TIMESTAMP_NTZ(9) as SI_JCP_UPDATE_DT,
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
		PLNT::VARCHAR(10) as PLNT,
		SALES_GRP::VARCHAR(10) as SALES_GRP,
		NULL::NUMBER(10,0) as SO_ID,
		NULL::NUMBER(10,0) as SO_JCP_REC_SEQ
    FROM trns 

),

final
AS (
	SELECT *
	FROM tmp3
	
	UNION ALL
	
	SELECT *
	FROM tmp4
	)
SELECT *
FROM final