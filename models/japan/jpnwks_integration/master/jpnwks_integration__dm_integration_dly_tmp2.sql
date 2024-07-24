{{
    config(
        pre_hook = "
                    UPDATE {{ ref('jpnedw_integration__mt_constant_range') }}
                    SET MAX_DATE = 
                    (
                        SELECT MAX(YMD_DT)
                        FROM {{ ref('jpnedw_integration__mt_cld') }}
                        WHERE year_445 = (SELECT YEAR( current_timestamp()::timestamp_ntz(9)))
                    )
                    WHERE Identify_cd = 'SO';

                    UPDATE {{ ref('jpnedw_integration__mt_constant_range') }}
                    SET MIN_DATE = 
                    (
                        SELECT MAX(YMD_DT)
                        FROM {{ ref('jpnedw_integration__mt_cld') }}
                        WHERE to_number(year_445) = 
                        (
                            SELECT YEAR( current_timestamp()::timestamp_ntz(9))) - (SELECT  DELETE_RANGE FROM {{ ref('jpnedw_integration__mt_constant_range') }}  WHERE IDENTIFY_CD='SO')
                        )
                    WHERE Identify_cd = 'SO';

                    DELETE
                    FROM {{ ref('jpnwks_integration__dm_integration_dly_tmp1') }} ab USING {{ ref('jpnedw_integration__mt_constant_range') }} mt
                    WHERE ab.JCP_DATA_SOURCE = 'SO'
                    AND ab.JCP_DATA_SOURCE = mt.IDENTIFY_CD
                    AND ab.JCP_DATE BETWEEN mt.MIN_DATE
                    AND mt.MAX_DATE;
                    "
    )
}}

---add these update before merging in posthook before the delete
                    
--finish



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
tmp1 AS
(
    select * from {{ ref('jpnwks_integration__dm_integration_dly_tmp1') }}
),

tmp2
AS (
	SELECT 'SO' as JCP_DATA_SOURCE,
		'Actual' as JCP_PLAN_TYPE,
		'Sell-Out' as JCP_ANALYSIS_TYPE,
		'Sell-Out' as JCP_DATA_CATEGORY,
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
			END as jcp_plan_item_type,
		CASE 
			WHEN 0 <= months_between(t_in.shp_date, t_item_m.rel_dt)
				AND months_between(t_in.shp_date, t_item_m.rel_dt) < 12
				THEN 'Y'
			ELSE 'N'
			END as jcp_new_item_type,
		CASE 
			WHEN t_in.trade_type IN ('11', '12', '31', '32', '51', '52')
				THEN '402000'
			WHEN t_in.trade_type IN ('21', '22', '41', '42')
				THEN '402098'
			ELSE NULL
			END as jcp_account,
		T_IN.JCP_CREATE_DATE,
        TO_TIMESTAMP(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')) as jcp_load_date,
		CASE 
			WHEN t_in.qty <> 0
				THEN t_in.jcp_net_price / t_in.qty
			ELSE NULL
			END as jcp_unit_prc,
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

trns as
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
		NULL::NUMBER(10,0) as SO_ID,
		NULL::NUMBER(10,0) as SO_JCP_REC_SEQ
    FROM tmp2    
),

final
AS (
	SELECT *
	FROM tmp1
	
	UNION ALL
	
	SELECT *
	FROM trns
	)
SELECT *
FROM final