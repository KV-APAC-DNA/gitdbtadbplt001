{{
    config
    (
        pre_hook ="{% if is_incremental() %}
                DELETE
                FROM {{ ref('jpnedw_integration__dw_so_sell_out_dly') }}
                WHERE BGN_SNDR_CD = (
                        SELECT IDENTIFY_VALUE
                        FROM {{ source('jpnedw_integration', 'mt_constant') }}
                        WHERE IDENTIFY_CD = 'JCP_PAN_FLG'
                            AND DELETE_FLAG = '0'
                            );
				insert into {{ ref('jpnedw_integration__dw_so_sell_out')}}
				SELECT 
					id, 
					rcv_dt, 
					test_flag, 
					bgn_sndr_cd, 
					ws_cd, 
					rtl_type, 
					rtl_cd, 
					trade_type, 
					shp_date, 
					shp_num, 
					trade_cd, 
					dep_cd, 
					chg_cd, 
					person_in_charge, 
					person_name, 
					rtl_name, 
					rtl_ho_cd, 
					rtl_address_cd, 
					data_type, 
					opt_fld, 
					item_nm, 
					item_cd_typ, 
					item_cd, 
					qty, 
					qty_type, 
					price, 
					price_type, 
					bgn_sndr_cd_gln, 
					rcv_cd_gln, 
					ws_cd_gln, 
					shp_ws_cd, 
					shp_ws_cd_gln, 
					rep_name_kanji, 
					rep_info, 
					trade_cd_gln, 
					rtl_cd_gln, 
					rtl_name_kanji, 
					rtl_ho_cd_gln, 
					item_cd_gtin, 
					item_nm_kanji, 
					unt_prc, 
					net_prc, 
					sales_chan_type, 
					jcp_create_date, -- 2016/01/27 add start yamamura shp_to_cdを項目追加
					jcp_shp_to_cd, -- 2016/01/27 add end yamamura shp_to_cdを項目追加
					jcp_str_cd, 
					jcp_net_price 
					from {{this}}
                {% endif %}"
    )
}}

with dw_si_sell_in_dly as(
	select * from {{ ref('jpnedw_integration__dw_si_sell_in_dly') }}  
),
edi_item_m as (
	select * from {{ ref('jpnedw_integration__edi_item_m') }}  
),
mt_in_out_conv as (
	select * from {{ ref('jpnedw_integration__mt_in_out_conv') }} 
),
edi_jedpar as (
	select * from {{ ref('jpnedw_integration__edi_jedpar') }} 
),
mt_account_key as (
	select * from {{ ref('jpnedw_integration__mt_account_key') }} 
),
mt_constant as (
	select * from {{ source('jpnedw_integration', 'mt_constant') }}
),
dw_so_sell_out_dly as(
    select * from {{ ref('jpnedw_integration__dw_so_sell_out_dly') }}
),
mt_constant_range as(
   select * from {{ ref('jpnedw_integration__mt_constant_range') }}
),
transformed as(
SELECT NULL as ID,
	NULL as RCV_DT,
	NULL as TEST_FLAG,
	T_PAN.JCP_PAN_FLG as BGN_SNDR_CD,
	T_JEDPAR.PARTNER_CUSTOMER_CD as WS_CD,
	' ' as RTL_TYPE,
	T_CONV.STR_CD as RTL_CD,
	CASE 
		WHEN T_PAN.ACCOUNT = '402000'
			THEN '11'
		WHEN T_PAN.ACCOUNT = '402098'
			THEN '21'
		ELSE '00' -- エラー
		END as TRADE_TYPE,
	T_PAN.CALDAY as SHP_DATE,
	NULL as SHP_NUM,
	NULL as TRADE_CD,
	NULL as DEP_CD,
	NULL as CHG_CD,
	NULL as PERSON_IN_CHARGE,
	NULL as PERSON_NAME,
	NULL as RTL_NAME,
	NULL as RTL_HO_CD,
	NULL as RTL_ADDRESS_CD,
	NULL as DATA_TYPE,
	NULL as OPT_FLD,
	NULL as ITEM_NM,
	'J' as ITEM_CD_TYP,
	T_ITEM_M.JAN_CD as ITEM_CD,
	T_PAN.JCP_QTY * T_ACCOUNT_M.SIGN_OF_NUMBER as QTY,
	' ' as QTY_TYPE,
	NULL as PRICE,
	'K' as PRICE_TYPE,
	NULL as BGN_SNDR_CD_GLN,
	NULL as RCV_CD_GLN,
	NULL as WS_CD_GLN,
	NULL as SHP_WS_CD,
	NULL as SHP_WS_CD_GLN,
	NULL as REP_NAME_KANJI,
	NULL as REP_INFO,
	NULL as TRADE_CD_GLN,
	NULL as RTL_CD_GLN,
	NULL as RTL_NAME_KANJI,
	NULL as RTL_HO_CD_GLN,
	NULL as ITEM_CD_GTIN,
	NULL as ITEM_NM_KANJI,
	ABS(T_PAN.JCP_UNT_PRC) as UNT_PRC,
	T_PAN.AMOCCC * T_ACCOUNT_M.SIGN_OF_NUMBER as NET_PRC,
	NULL as SALES_CHAN_TYPE,
	T_PAN.JCP_CREATE_DT as JCP_CREATE_DATE
	-- 2016/01/27 ADD Start Yamamura SHP_TO_CDを項目追加
	,
	T_PAN.CUSTOMER as JCP_SHP_TO_CD
	-- 2016/01/27 ADD End Yamamura SHP_TO_CDを項目追加
	,
	T_CONV.STR_CD as JCP_STR_CD
	/*
		, CASE
			WHEN T_PAN.JCP_QTY <> 0
			THEN CASE
				WHEN T_PAN.JCP_UNT_PRC <> 0 THEN T_PAN.JCP_QTY * ABS(T_PAN.JCP_UNT_PRC)
				ELSE T_PAN.AMOCCC * T_ACCOUNT_M.SIGN_OF_NUMBER
				END
			ELSE CASE
				WHEN T_PAN.JCP_UNT_PRC <> 0 THEN T_PAN.JCP_UNT_PRC
				ELSE T_PAN.AMOCCC * T_ACCOUNT_M.SIGN_OF_NUMBER
				END
		END
		*/
	,
	T_PAN.AMOCCC * T_ACCOUNT_M.SIGN_OF_NUMBER as JCP_NET_PRICE
FROM DW_SI_SELL_IN_DLY T_PAN
LEFT OUTER JOIN EDI_ITEM_M T_ITEM_M ON T_PAN.MATERIAL = T_ITEM_M.ITEM_CD
LEFT OUTER JOIN MT_IN_OUT_CONV T_CONV ON T_PAN.CUSTOMER = T_CONV.SLD_TO
LEFT OUTER JOIN EDI_JEDPAR T_JEDPAR ON LPAD(T_PAN.CUSTOMER, 10, '0') = T_JEDPAR.CUSTOMER_NUMBER
LEFT OUTER JOIN MT_ACCOUNT_KEY T_ACCOUNT_M ON T_PAN.ACCOUNT = T_ACCOUNT_M.ACCOUNTING_CODE
WHERE T_PAN.JCP_PAN_FLG = (
		SELECT IDENTIFY_VALUE
		FROM MT_CONSTANT
		WHERE IDENTIFY_CD = 'JCP_PAN_FLG'
			AND DELETE_FLAG = '0'
		)
	AND (T_PAN.ACCOUNT IN ('402000', '402098'))
	AND T_CONV.SLD_TO IS NOT NULL
	AND T_JEDPAR.PARTNER_FN = 'WE'
	AND T_JEDPAR.VAN_TYPE = 'PLANET'
	AND T_JEDPAR.SAP_CSTM_TYPE = 'CON'
	AND T_ACCOUNT_M.DELETE_FLAG = '0'
),
final as(
	select 
		NULL::number(10,0) as jcp_rec_seq,
		id::number(10,0) as id,
		rcv_dt::timestamp_ntz(9) as rcv_dt,
		test_flag::varchar(256) as test_flag,
		bgn_sndr_cd::varchar(256) as bgn_sndr_cd,
		ws_cd::varchar(256) as ws_cd,
		rtl_type::varchar(256) as rtl_type,
		rtl_cd::varchar(256) as rtl_cd,
		trade_type::varchar(256) as trade_type,
		shp_date::timestamp_ntz(9) as shp_date,
		shp_num::varchar(256) as shp_num,
		trade_cd::varchar(256) as trade_cd,
		dep_cd::varchar(256) as dep_cd,
		chg_cd::varchar(256) as chg_cd,
		person_in_charge::varchar(256) as person_in_charge,
		person_name::varchar(256) as person_name,
		rtl_name::varchar(256) as rtl_name,
		rtl_ho_cd::varchar(256) as rtl_ho_cd,
		rtl_address_cd::varchar(256) as rtl_address_cd,
		data_type::varchar(256) as data_type,
		opt_fld::varchar(256) as opt_fld,
		item_nm::varchar(256) as item_nm,
		item_cd_typ::varchar(256) as item_cd_typ,
		item_cd::varchar(256) as item_cd,
		qty::number(30,0) as qty,
		qty_type::varchar(256) as qty_type,
		price::number(30,0) as price,
		price_type::varchar(256) as price_type,
		bgn_sndr_cd_gln::varchar(256) as bgn_sndr_cd_gln,
		rcv_cd_gln::varchar(256) as rcv_cd_gln,
		ws_cd_gln::varchar(256) as ws_cd_gln,
		shp_ws_cd::varchar(256) as shp_ws_cd,
		shp_ws_cd_gln::varchar(256) as shp_ws_cd_gln,
		rep_name_kanji::varchar(256) as rep_name_kanji,
		rep_info::varchar(256) as rep_info,
		trade_cd_gln::varchar(256) as trade_cd_gln,
		rtl_cd_gln::varchar(256) as rtl_cd_gln,
		rtl_name_kanji::varchar(256) as rtl_name_kanji,
		rtl_ho_cd_gln::varchar(256) as rtl_ho_cd_gln,
		item_cd_gtin::varchar(256) as item_cd_gtin,
		item_nm_kanji::varchar(256) as item_nm_kanji,
		unt_prc::number(30,0) as unt_prc,
		net_prc::number(30,0) as net_prc,
		sales_chan_type::varchar(256) as sales_chan_type,
		jcp_create_date::timestamp_ntz(9) as jcp_create_date,
		jcp_shp_to_cd::varchar(10) as jcp_shp_to_cd,
		jcp_str_cd::varchar(9) as jcp_str_cd,
		jcp_net_price::number(30,0) as jcp_net_price
	from transformed
)
select * from final