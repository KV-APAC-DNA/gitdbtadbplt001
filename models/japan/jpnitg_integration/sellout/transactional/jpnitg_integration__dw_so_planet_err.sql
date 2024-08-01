{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append'
    )
}}

WITH wk_so_planet_today
AS (
    SELECT *
    FROM {{ ref('jpnwks_integration__wk_so_planet_today') }} 
    ),
t2
AS (
    SELECT COUNT(*),
        ID,
        BGN_SNDR_CD,
        WS_CD,
        RTL_TYPE,
        RTL_CD,
        TRADE_TYPE,
        SHP_DATE,
        SHP_NUM,
        TRADE_CD,
        DEP_CD,
        CHG_CD,
        PERSON_IN_CHARGE,
        PERSON_NAME,
        RTL_NAME,
        RTL_HO_CD,
        RTL_ADDRESS_CD,
        DATA_TYPE,
        OPT_FLD,
        ITEM_NM,
        ITEM_CD_TYP,
        ITEM_CD,
        QTY_TYPE,
        PRICE_TYPE,
        BGN_SNDR_CD_GLN,
        RCV_CD_GLN,
        WS_CD_GLN,
        SHP_WS_CD,
        SHP_WS_CD_GLN,
        REP_NAME_KANJI,
        REP_INFO,
        TRADE_CD_GLN,
        RTL_CD_GLN,
        RTL_NAME_KANJI,
        RTL_HO_CD_GLN,
        ITEM_CD_GTIN,
        ITEM_NM_KANJI,
        SALES_CHAN_TYPE
    FROM wk_so_planet_today
    GROUP BY ID,
        BGN_SNDR_CD,
        WS_CD,
        RTL_TYPE,
        RTL_CD,
        TRADE_TYPE,
        SHP_DATE,
        SHP_NUM,
        TRADE_CD,
        DEP_CD,
        CHG_CD,
        PERSON_IN_CHARGE,
        PERSON_NAME,
        RTL_NAME,
        RTL_HO_CD,
        RTL_ADDRESS_CD,
        DATA_TYPE,
        OPT_FLD,
        ITEM_NM,
        ITEM_CD_TYP,
        ITEM_CD,
        QTY_TYPE,
        PRICE_TYPE,
        BGN_SNDR_CD_GLN,
        RCV_CD_GLN,
        WS_CD_GLN,
        SHP_WS_CD,
        SHP_WS_CD_GLN,
        REP_NAME_KANJI,
        REP_INFO,
        TRADE_CD_GLN,
        RTL_CD_GLN,
        RTL_NAME_KANJI,
        RTL_HO_CD_GLN,
        ITEM_CD_GTIN,
        ITEM_NM_KANJI,
        SALES_CHAN_TYPE
    HAVING COUNT(*) > 1
    ),
insert1
AS (
    SELECT ERR.JCP_REC_SEQ,
        ERR.ID,
        ERR.RCV_DT,
        ERR.TEST_FLAG,
        ERR.BGN_SNDR_CD,
        ERR.WS_CD,
        ERR.RTL_TYPE,
        ERR.RTL_CD,
        ERR.TRADE_TYPE,
        ERR.SHP_DATE,
        ERR.SHP_NUM,
        ERR.TRADE_CD,
        ERR.DEP_CD,
        ERR.CHG_CD,
        ERR.PERSON_IN_CHARGE,
        ERR.PERSON_NAME,
        ERR.RTL_NAME,
        ERR.RTL_HO_CD,
        ERR.RTL_ADDRESS_CD,
        ERR.DATA_TYPE,
        ERR.OPT_FLD,
        ERR.ITEM_NM,
        ERR.ITEM_CD_TYP,
        ERR.ITEM_CD,
        ERR.QTY,
        ERR.QTY_TYPE,
        ERR.PRICE,
        ERR.PRICE_TYPE,
        ERR.BGN_SNDR_CD_GLN,
        ERR.RCV_CD_GLN,
        ERR.WS_CD_GLN,
        ERR.SHP_WS_CD,
        ERR.SHP_WS_CD_GLN,
        ERR.REP_NAME_KANJI,
        ERR.REP_INFO,
        ERR.TRADE_CD_GLN,
        ERR.RTL_CD_GLN,
        ERR.RTL_NAME_KANJI,
        ERR.RTL_HO_CD_GLN,
        ERR.ITEM_CD_GTIN,
        ERR.ITEM_NM_KANJI,
        ERR.UNT_PRC,
        ERR.NET_PRC,
        ERR.SALES_CHAN_TYPE,
        ERR.JCP_CREATE_DATE,
        NULL as JCP_RCV_DT_DUPLI,
        NULL as JCP_TEST_FLAG_DUPLI,
        NULL as JCP_QTY_DUPLI,
        NULL as JCP_PRICE_DUPLI,
        NULL as JCP_UNT_PRC_DUPLI,
        NULL as JCP_NET_PRC_DUPLI,
        0 as EXPORT_FLAG
    FROM wk_so_planet_today ERR,
        t2
    WHERE NVL(ERR.ID, 0) = NVL(t2.ID, 0)
        AND NVL(ERR.BGN_SNDR_CD, ' ') = NVL(t2.BGN_SNDR_CD, ' ')
        AND NVL(ERR.WS_CD, ' ') = NVL(t2.WS_CD, ' ')
        AND NVL(ERR.RTL_TYPE, ' ') = NVL(t2.RTL_TYPE, ' ')
        AND NVL(ERR.RTL_CD, ' ') = NVL(t2.RTL_CD, ' ')
        AND NVL(ERR.TRADE_TYPE, ' ') = NVL(t2.TRADE_TYPE, ' ')
        AND NVL(ERR.SHP_DATE, ' ') = NVL(t2.SHP_DATE, ' ')
        AND NVL(ERR.SHP_NUM, ' ') = NVL(t2.SHP_NUM, ' ')
        AND NVL(ERR.TRADE_CD, ' ') = NVL(t2.TRADE_CD, ' ')
        AND NVL(ERR.DEP_CD, ' ') = NVL(t2.DEP_CD, ' ')
        AND NVL(ERR.CHG_CD, ' ') = NVL(t2.CHG_CD, ' ')
        AND NVL(ERR.PERSON_IN_CHARGE, ' ') = NVL(t2.PERSON_IN_CHARGE, ' ')
        AND NVL(ERR.PERSON_NAME, ' ') = NVL(t2.PERSON_NAME, ' ')
        AND NVL(ERR.RTL_NAME, ' ') = NVL(t2.RTL_NAME, ' ')
        AND NVL(ERR.RTL_HO_CD, ' ') = NVL(t2.RTL_HO_CD, ' ')
        AND NVL(ERR.RTL_ADDRESS_CD, ' ') = NVL(t2.RTL_ADDRESS_CD, ' ')
        AND NVL(ERR.DATA_TYPE, ' ') = NVL(t2.DATA_TYPE, ' ')
        AND NVL(ERR.OPT_FLD, ' ') = NVL(t2.OPT_FLD, ' ')
        AND NVL(ERR.ITEM_NM, ' ') = NVL(t2.ITEM_NM, ' ')
        AND NVL(ERR.ITEM_CD_TYP, ' ') = NVL(t2.ITEM_CD_TYP, ' ')
        AND NVL(ERR.ITEM_CD, ' ') = NVL(t2.ITEM_CD, ' ')
        AND NVL(ERR.QTY_TYPE, ' ') = NVL(t2.QTY_TYPE, ' ')
        AND NVL(ERR.PRICE_TYPE, ' ') = NVL(t2.PRICE_TYPE, ' ')
        AND NVL(ERR.BGN_SNDR_CD_GLN, ' ') = NVL(t2.BGN_SNDR_CD_GLN, ' ')
        AND NVL(ERR.RCV_CD_GLN, ' ') = NVL(t2.RCV_CD_GLN, ' ')
        AND NVL(ERR.WS_CD_GLN, ' ') = NVL(t2.WS_CD_GLN, ' ')
        AND NVL(ERR.SHP_WS_CD, ' ') = NVL(t2.SHP_WS_CD, ' ')
        AND NVL(ERR.SHP_WS_CD_GLN, ' ') = NVL(t2.SHP_WS_CD_GLN, ' ')
        AND NVL(ERR.REP_NAME_KANJI, ' ') = NVL(t2.REP_NAME_KANJI, ' ')
        AND NVL(ERR.REP_INFO, ' ') = NVL(t2.REP_INFO, ' ')
        AND NVL(ERR.TRADE_CD_GLN, ' ') = NVL(t2.TRADE_CD_GLN, ' ')
        AND NVL(ERR.RTL_CD_GLN, ' ') = NVL(t2.RTL_CD_GLN, ' ')
        AND NVL(ERR.RTL_NAME_KANJI, ' ') = NVL(t2.RTL_NAME_KANJI, ' ')
        AND NVL(ERR.RTL_HO_CD_GLN, ' ') = NVL(t2.RTL_HO_CD_GLN, ' ')
        AND NVL(ERR.ITEM_CD_GTIN, ' ') = NVL(t2.ITEM_CD_GTIN, ' ')
        AND NVL(ERR.ITEM_NM_KANJI, ' ') = NVL(t2.ITEM_NM_KANJI, ' ')
        AND NVL(ERR.SALES_CHAN_TYPE, ' ') = NVL(t2.SALES_CHAN_TYPE, ' ')
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
    from insert1
)
SELECT *
FROM final