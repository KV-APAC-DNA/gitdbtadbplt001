{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
            DELETE FROM {{this}} WHERE JCP_PAN_FLG = (SELECT IDENTIFY_VALUE FROM DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_CONSTANT WHERE IDENTIFY_CD = 'JCP_PAN_FLG' AND DELETE_FLAG = '0');
        {% endif %}"
    )
}}

with dw_si_sell_in_dly_mod as(
	select * from {{ source('jpnedw_integration', 'dw_si_sell_in_dly_mod') }}
),
WK_SI_PAN_THIS_YEAR as(
	select * from {{ ref('jpnwks_integration__wk_iv_edi') }}
),
mt_cld as(
	select * from {{ source('jpnedw_integration', 'mt_cld') }}
),
EDI_ITEM_M as(
	select * from {{ ref('jpnwks_integration__edi_item_m') }}
),
mt_constant as(
	select * from {{ source('jpnedw_integration', 'mt_constant') }}
),
union1 as(
	SELECT
		LTRIM(T_PAN.ACCOUNT,'0') as ACCOUNT
		, CASE
			WHEN T_PAN.CALDAY <> '00000000' THEN TO_DATE(T_PAN.CALDAY,'YYYYMMDD')
			ELSE TO_DATE('19990101','YYYYMMDD')
		END as CALDAY
		, T_PAN.CHRT_ACCTS as CHRT_ACCTS
		, T_PAN.COMP_CODE as COMP_CODE
		, T_PAN.CURKEY_TC as CURKEY_TC
		, T_PAN.CURRENCY as CURRENCY
		, LTRIM(T_PAN.CUSTOMER,'0') as CUSTOMER
		, LTRIM(T_PAN.CUST_SALES,'0') as CUST_SALES
		, T_PAN.DISTR_CHAN as DISTR_CHAN
		, T_PAN.FISCPER as FISCPER
		, T_PAN.FISCVARNT as FISCVARNT
		, LTRIM(T_PAN.MATERIAL,'0') as MATERIAL
		, T_PAN.OBJ_CURR as OBJ_CURR
		, T_PAN.RECORDTP as RECORDTP
		, T_PAN.SALES_GRP as SALES_GRP
		, T_PAN.VTYPE as VTYPE
		, T_PAN.AMOCAC as AMOCAC
		, T_PAN.AMOCCC as AMOCCC
		, T_PAN.S016_0CUST_SALES as S016_0CUST_SALES
		, T_PAN.S017_0CUST_SALES as S017_0CUST_SALES
		, NVL(LTRIM(T_PAN.S001_0CUST_SALES,'0'),'0') as S001_0CUST_SALES
		, T_PAN.GROSSAMTTC as GROSSAMTTC
		, T_PAN.S023_0MATERIAL as S023_0MATERIAL
		, T_PAN.S024_0MATERIAL as S024_0MATERIAL
		, T_PAN.S025_0MATERIAL as S025_0MATERIAL
		, T_PAN.S026_0MATERIAL as S026_0MATERIAL
		, T_PAN.S003_0ACCOUNT as S003_0ACCOUNT
		, T_PAN.S004_0ACCOUNT as S004_0ACCOUNT
		, T_PAN.S005_0ACCOUNT as S005_0ACCOUNT
		, T_PAN.S006_0ACCOUNT as S006_0ACCOUNT
		, T_PAN.S007_0ACCOUNT as S007_0ACCOUNT
		, current_timestamp()::timestamp_ntz(9) as JCP_CREATE_DT
		, (SELECT IDENTIFY_VALUE FROM MT_CONSTANT WHERE IDENTIFY_CD = 'JCP_PAN_FLG' AND DELETE_FLAG = '0') as JCP_PAN_FLG
		, CASE
			WHEN (LTRIM(T_PAN.ACCOUNT,'0') IN ('402000' ,'402098'))
			THEN T_ITEM_M.UNT_PRC
			ELSE null
		END as JCP_UNT_PRC
		, CASE
			WHEN (LTRIM(T_PAN.ACCOUNT,'0') IN ('402000' ,'402098'))
			THEN CASE
				WHEN T_ITEM_M.UNT_PRC <> 0 THEN T_PAN.AMOCCC / T_ITEM_M.UNT_PRC
				ELSE null
				END
			ELSE null
		END as JCP_QTY
		, T_CAL.MIN_YMD_DT as JCP_445_YMD_DT
		, '0' as JCP_UPDATE_FLG
		, null as JCP_UPDATE_DT
		, T_PAN.plnt as plnt    -- added this column as part of Kizuna phase 2 DCL Integration to identify plants
	FROM WK_SI_PAN_THIS_YEAR T_PAN
	LEFT OUTER JOIN
	(
		SELECT YEAR_445,MONTH_445,MIN(YMD_DT) AS MIN_YMD_DT
		FROM MT_CLD
		GROUP BY YEAR_445,MONTH_445
	) T_CAL
	ON SUBSTRING(T_PAN.FISCPER,1,4) = T_CAL.YEAR_445
	AND LTRIM(SUBSTRING(T_PAN.FISCPER,5,3),'0') = T_CAL.MONTH_445
	LEFT OUTER JOIN EDI_ITEM_M T_ITEM_M
	ON LTRIM(T_PAN.MATERIAL,'0') = T_ITEM_M.ITEM_CD
),
union2 as(
	SELECT
		ACCOUNT as ACCOUNT
		, CALDAY as CALDAY
		, CHRT_ACCTS as CHRT_ACCTS
		, COMP_CODE as COMP_CODE
		, CURKEY_TC as CURKEY_TC
		, CURRENCY as CURRENCY
		, CUSTOMER as CUSTOMER
		, CUST_SALES as CUST_SALES
		, DISTR_CHAN as DISTR_CHAN
		, FISCPER as FISCPER
		, FISCVARNT as FISCVARNT
		, MATERIAL as MATERIAL
		, OBJ_CURR as OBJ_CURR
		, RECORDTP as RECORDTP
		, SALES_GRP as SALES_GRP
		, VTYPE as VTYPE
		, AMOCAC as AMOCAC
		, AMOCCC as AMOCCC
		, S016_0CUST_SALES as S016_0CUST_SALES
		, S017_0CUST_SALES as S017_0CUST_SALES
		, S001_0CUST_SALES as S001_0CUST_SALES
		, GROSSAMTTC as GROSSAMTTC
		, S023_0MATERIAL as S023_0MATERIAL
		, S024_0MATERIAL as S024_0MATERIAL
		, S025_0MATERIAL as S025_0MATERIAL
		, S026_0MATERIAL as S026_0MATERIAL
		, S003_0ACCOUNT as S003_0ACCOUNT
		, S004_0ACCOUNT as S004_0ACCOUNT
		, S005_0ACCOUNT as S005_0ACCOUNT
		, S006_0ACCOUNT as S006_0ACCOUNT
		, S007_0ACCOUNT as S007_0ACCOUNT
		, JCP_CREATE_DT as JCP_CREATE_DT
		, JCP_PAN_FLG as JCP_PAN_FLG
		, JCP_UNT_PRC as JCP_UNT_PRC
		, JCP_QTY as JCP_QTY
		, JCP_445_YMD_DT as JCP_445_YMD_DT
		, JCP_UPDATE_FLG as JCP_UPDATE_FLG
		, JCP_UPDATE_DT as JCP_UPDATE_DT,
        null as plnt
	FROM DW_SI_SELL_IN_DLY_MOD
	WHERE JCP_UPDATE_FLG <> 0
	AND JCP_UPDATE_DT IS NOT NULL

),
transformed as(
	select * from union1
	union all
	select * from union2
),
final as(
	select 
		account::varchar(30) as account,
		calday::timestamp_ntz(9) as calday,
		chrt_accts::varchar(12) as chrt_accts,
		comp_code::varchar(12) as comp_code,
		curkey_tc::varchar(15) as curkey_tc,
		currency::varchar(15) as currency,
		customer::varchar(30) as customer,
		cust_sales::varchar(30) as cust_sales,
		distr_chan::varchar(6) as distr_chan,
		fiscper::varchar(21) as fiscper,
		fiscvarnt::varchar(6) as fiscvarnt,
		material::varchar(54) as material,
		obj_curr::varchar(15) as obj_curr,
		recordtp::varchar(3) as recordtp,
		sales_grp::varchar(9) as sales_grp,
		vtype::varchar(9) as vtype,
		amocac::number(17,2) as amocac,
		amoccc::number(17,2) as amoccc,
		s016_0cust_sales::varchar(9) as s016_0cust_sales,
		s017_0cust_sales::varchar(12) as s017_0cust_sales,
		s001_0cust_sales::varchar(24) as s001_0cust_sales,
		grossamttc::number(17,2) as grossamttc,
		s023_0material::varchar(54) as s023_0material,
		s024_0material::varchar(54) as s024_0material,
		s025_0material::varchar(54) as s025_0material,
		s026_0material::varchar(54) as s026_0material,
		s003_0account::varchar(54) as s003_0account,
		s004_0account::varchar(54) as s004_0account,
		s005_0account::varchar(54) as s005_0account,
		s006_0account::varchar(54) as s006_0account,
		s007_0account::varchar(54) as s007_0account,
		jcp_create_dt::timestamp_ntz(9) as jcp_create_dt,
		jcp_pan_flg::varchar(7) as jcp_pan_flg,
		jcp_unt_prc::number(20,3) as jcp_unt_prc,
		jcp_qty::number(30,10) as jcp_qty,
		jcp_445_ymd_dt::timestamp_ntz(9) as jcp_445_ymd_dt,
		jcp_update_flg::varchar(1) as jcp_update_flg,
		jcp_update_dt::timestamp_ntz(9) as jcp_update_dt,
		plnt::varchar(10) as plnt
	from transformed
)
select * from final



