
WITH WK_SO_PLANET_CLEANSED
AS (
    SELECT *
    FROM {{ ref('jpnwks_integration__wk_so_planet_cleansed') }}
    ),
insert4
AS (
    SELECT *
    FROM {{ ref('jpnwks_integration__dw_so_planet_err_tmp2') }}
    ),
insert5
AS (
    SELECT wkpn.JCP_REC_SEQ,
        wkpn.ID,
        TO_CHAR(wkpn.RCV_DT, 'YYMMDD') as rcv_dt,
        wkpn.TEST_FLAG,
        wkpn.BGN_SNDR_CD,
        wkpn.WS_CD,
        wkpn.RTL_TYPE,
        wkpn.RTL_CD,
        wkpn.TRADE_TYPE,
        TO_CHAR(wkpn.SHP_DATE, 'YYMMDD') as shp_date,
        wkpn.SHP_NUM,
        wkpn.TRADE_CD,
        wkpn.DEP_CD,
        wkpn.CHG_CD,
        wkpn.PERSON_IN_CHARGE,
        wkpn.PERSON_NAME,
        wkpn.RTL_NAME,
        wkpn.RTL_HO_CD,
        wkpn.RTL_ADDRESS_CD,
        wkpn.DATA_TYPE,
        wkpn.OPT_FLD,
        wkpn.ITEM_NM,
        wkpn.ITEM_CD_TYP,
        wkpn.ITEM_CD,
        wkpn.QTY,
        wkpn.QTY_TYPE,
        wkpn.PRICE,
        wkpn.PRICE_TYPE,
        wkpn.BGN_SNDR_CD_GLN,
        wkpn.RCV_CD_GLN,
        wkpn.WS_CD_GLN,
        wkpn.SHP_WS_CD,
        wkpn.SHP_WS_CD_GLN,
        wkpn.REP_NAME_KANJI,
        wkpn.REP_INFO,
        wkpn.TRADE_CD_GLN,
        wkpn.RTL_CD_GLN,
        wkpn.RTL_NAME_KANJI,
        wkpn.RTL_HO_CD_GLN,
        wkpn.ITEM_CD_GTIN,
        wkpn.ITEM_NM_KANJI,
        wkpn.UNT_PRC,
        wkpn.NET_PRC,
        wkpn.SALES_CHAN_TYPE,
        wkpn.JCP_CREATE_DATE,
        NULL as  JCP_RCV_DT_DUPLI,
        NULL as JCP_TEST_FLAG_DUPLI,
        NULL as JCP_QTY_DUPLI,
        NULL as JCP_PRICE_DUPLI,
        NULL as JCP_UNT_PRC_DUPLI,
        NULL as JCP_NET_PRC_DUPLI,
        '0' as EXPORT_FLAG
    FROM WK_SO_PLANET_CLEANSED wkpn
    WHERE TO_CHAR(SHP_DATE, 'YYYYMM') > (
            SELECT TO_CHAR(ADD_MONTHS(MAX(RCV_DT), 6), 'YYYYMM')
            FROM WK_SO_PLANET_CLEANSED
            )
),

final as
(
    select
		jcp_rec_seq::number(10, 0) AS jcp_rec_seq,
		id::number(10, 0) AS id,
		rcv_dt::VARCHAR(256) AS rcv_dt,
		test_flag::VARCHAR(256) AS test_flag,
		bgn_sndr_cd::VARCHAR(256) AS bgn_sndr_cd,
		ws_cd::VARCHAR(256) AS ws_cd,
		rtl_type::VARCHAR(256) AS rtl_type,
		rtl_cd::VARCHAR(256) AS rtl_cd,
		trade_type::VARCHAR(256) AS trade_type,
		shp_date::VARCHAR(256) AS shp_date,
		shp_num::VARCHAR(256) AS shp_num,
		trade_cd::VARCHAR(256) AS trade_cd,
		dep_cd::VARCHAR(256) AS dep_cd,
		chg_cd::VARCHAR(256) AS chg_cd,
		person_in_charge::VARCHAR(256) AS person_in_charge,
		person_name::VARCHAR(256) AS person_name,
		rtl_name::VARCHAR(256) AS rtl_name,
		rtl_ho_cd::VARCHAR(256) AS rtl_ho_cd,
		rtl_address_cd::VARCHAR(256) AS rtl_address_cd,
		data_type::VARCHAR(256) AS data_type,
		opt_fld::VARCHAR(256) AS opt_fld,
		item_nm::VARCHAR(256) AS item_nm,
		item_cd_typ::VARCHAR(256) AS item_cd_typ,
		item_cd::VARCHAR(256) AS item_cd,
		qty::VARCHAR(256) AS qty,
		qty_type::VARCHAR(256) AS qty_type,
		price::VARCHAR(256) AS price,
		price_type::VARCHAR(256) AS price_type,
		bgn_sndr_cd_gln::VARCHAR(256) AS bgn_sndr_cd_gln,
		rcv_cd_gln::VARCHAR(256) AS rcv_cd_gln,
		ws_cd_gln::VARCHAR(256) AS ws_cd_gln,
		shp_ws_cd::VARCHAR(256) AS shp_ws_cd,
		shp_ws_cd_gln::VARCHAR(256) AS shp_ws_cd_gln,
		rep_name_kanji::VARCHAR(256) AS rep_name_kanji,
		rep_info::VARCHAR(256) AS rep_info,
		trade_cd_gln::VARCHAR(256) AS trade_cd_gln,
		rtl_cd_gln::VARCHAR(256) AS rtl_cd_gln,
		rtl_name_kanji::VARCHAR(256) AS rtl_name_kanji,
		rtl_ho_cd_gln::VARCHAR(256) AS rtl_ho_cd_gln,
		item_cd_gtin::VARCHAR(256) AS item_cd_gtin,
		item_nm_kanji::VARCHAR(256) AS item_nm_kanji,
		unt_prc::VARCHAR(256) AS unt_prc,
		net_prc::VARCHAR(256) AS net_prc,
		sales_chan_type::VARCHAR(256) AS sales_chan_type,
		jcp_create_date::timestamp_ntz(9) AS jcp_create_date,
		jcp_rcv_dt_dupli::VARCHAR(256) AS jcp_rcv_dt_dupli,
		jcp_test_flag_dupli::VARCHAR(256) AS jcp_test_flag_dupli,
		jcp_qty_dupli::VARCHAR(256) AS jcp_qty_dupli,
		jcp_price_dupli::VARCHAR(256) AS jcp_price_dupli,
		jcp_unt_prc_dupli::VARCHAR(256) AS jcp_unt_prc_dupli,
		jcp_net_prc_dupli::VARCHAR(256) AS jcp_net_prc_dupli,
		export_flag::VARCHAR(256) AS export_flag
    from insert5
),

ct3
AS (
    SELECT *
    FROM insert4
    
    UNION ALL
    
    SELECT *
    FROM final
    )
SELECT *
FROM ct3