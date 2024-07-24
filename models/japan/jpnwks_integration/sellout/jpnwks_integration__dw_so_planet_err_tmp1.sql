{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'append',
        pre_hook = '{{build_dw_so_planet_err_temp()}}',
        post_hook = "
                    UPDATE {{this}}
                    SET EXPORT_FLAG = '1'
                    WHERE EXPORT_FLAG = '0'
                    AND JCP_REC_SEQ IN 
                    (
                        SELECT jcp_rec_seq
                        FROM DEV_DNA_CORE.JPNWKS_INTEGRATION.CONSISTENCY_ERROR_2
                        WHERE exec_flag IN ('MANUAL', 'AUTOCORRECT')
                    )
                    AND JCP_REC_SEQ IN 
                    (
                        SELECT jcp_rec_seq
                        FROM DEV_DNA_CORE.JPNWKS_INTEGRATION.WK_SO_PLANET_NO_DUP
                    );
                    "
    )
}}
WITH WK_SO_PLANET_NO_DUP
AS (
    SELECT *
    FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} 
    ),
EDI_EXCL_RTLR
AS (
    SELECT *
    FROM {{ ref('jpnedw_integration__edi_excl_rtlr') }}
    ),
dw_so_planet_err
as (
    select *
    from {{ source('jpnitg_integration', 'jpnitg_integration__dw_so_planet_err_temp') }}
    ),
consistency_error_2
as (
    select *
    from {{ ref('jpnwks_integration__consistency_error_2') }} 
    ),
temp_tbl
as (
    select *
    from {{ ref('jpnwks_integration__temp_tbl') }}
    ),
dw_so_planet_err_cd
as (
    select *
    from {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
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
    FROM WK_SO_PLANET_NO_DUP
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
    FROM WK_SO_PLANET_NO_DUP ERR,
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
c
AS (
    SELECT A.RTL_CD,
        COUNT(*) v_CNT
    FROM WK_SO_PLANET_NO_DUP A,
        EDI_EXCL_RTLR B
    WHERE B.RTLR_CD = A.RTL_CD
    GROUP BY A.RTL_CD
    HAVING COUNT(*) > 0
    ),
insert2
AS (
    SELECT A.JCP_REC_SEQ,
        A.ID,
        A.RCV_DT,
        A.TEST_FLAG,
        A.BGN_SNDR_CD,
        A.WS_CD,
        A.RTL_TYPE,
        A.RTL_CD,
        A.TRADE_TYPE,
        A.SHP_DATE,
        A.SHP_NUM,
        A.TRADE_CD,
        A.DEP_CD,
        A.CHG_CD,
        A.PERSON_IN_CHARGE,
        A.PERSON_NAME,
        A.RTL_NAME,
        A.RTL_HO_CD,
        A.RTL_ADDRESS_CD,
        A.DATA_TYPE,
        A.OPT_FLD,
        A.ITEM_NM,
        A.ITEM_CD_TYP,
        A.ITEM_CD,
        A.QTY,
        A.QTY_TYPE,
        A.PRICE,
        A.PRICE_TYPE,
        A.BGN_SNDR_CD_GLN,
        A.RCV_CD_GLN,
        A.WS_CD_GLN,
        A.SHP_WS_CD,
        A.SHP_WS_CD_GLN,
        A.REP_NAME_KANJI,
        A.REP_INFO,
        A.TRADE_CD_GLN,
        A.RTL_CD_GLN,
        A.RTL_NAME_KANJI,
        A.RTL_HO_CD_GLN,
        A.ITEM_CD_GTIN,
        A.ITEM_NM_KANJI,
        A.UNT_PRC,
        A.NET_PRC,
        A.SALES_CHAN_TYPE,
        A.JCP_CREATE_DATE,
        NULL as  JCP_RCV_DT_DUPLI,
        NULL as  JCP_TEST_FLAG_DUPLI,
        NULL as JCP_QTY_DUPLI,
        NULL as JCP_PRICE_DUPLI,
        NULL as JCP_UNT_PRC_DUPLI,
        NULL as  JCP_NET_PRC_DUPLI,
        '0' as EXPORT_FLAG
    FROM WK_SO_PLANET_NO_DUP A,
        C
    WHERE A.RTL_CD = C.RTL_CD
    ),
insert3
AS (
    SELECT A.JCP_REC_SEQ,
        A.ID,
        A.RCV_DT,
        A.TEST_FLAG,
        A.BGN_SNDR_CD,
        A.WS_CD,
        A.RTL_TYPE,
        A.RTL_CD,
        A.TRADE_TYPE,
        A.SHP_DATE,
        A.SHP_NUM,
        A.TRADE_CD,
        A.DEP_CD,
        A.CHG_CD,
        A.PERSON_IN_CHARGE,
        A.PERSON_NAME,
        A.RTL_NAME,
        A.RTL_HO_CD,
        A.RTL_ADDRESS_CD,
        A.DATA_TYPE,
        A.OPT_FLD,
        A.ITEM_NM,
        A.ITEM_CD_TYP,
        A.ITEM_CD,
        A.QTY,
        A.QTY_TYPE,
        A.PRICE,
        A.PRICE_TYPE,
        A.BGN_SNDR_CD_GLN,
        A.RCV_CD_GLN,
        A.WS_CD_GLN,
        A.SHP_WS_CD,
        A.SHP_WS_CD_GLN,
        A.REP_NAME_KANJI,
        A.REP_INFO,
        A.TRADE_CD_GLN,
        A.RTL_CD_GLN,
        A.RTL_NAME_KANJI,
        A.RTL_HO_CD_GLN,
        A.ITEM_CD_GTIN,
        A.ITEM_NM_KANJI,
        A.UNT_PRC,
        A.NET_PRC,
        A.SALES_CHAN_TYPE,
        A.JCP_CREATE_DATE,
        NULL as  JCP_RCV_DT_DUPLI,
        NULL as JCP_TEST_FLAG_DUPLI,
        NULL as JCP_QTY_DUPLI,
        NULL as JCP_PRICE_DUPLI,
        NULL as JCP_UNT_PRC_DUPLI,
        NULL as JCP_NET_PRC_DUPLI,
        '0' as EXPORT_FLAG
    FROM WK_SO_PLANET_NO_DUP A
    WHERE A.ITEM_CD = ' '
        AND A.ITEM_NM = ' '
        AND A.JCP_REC_SEQ NOT IN (
            SELECT DISTINCT JCP_REC_SEQ
            FROM DW_SO_PLANET_ERR
            )
    ),
ct1
AS (
    SELECT *
    FROM insert1
    
    UNION ALL
    
    SELECT *
    FROM insert2
    
    UNION ALL
    
    SELECT *
    FROM insert3
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
    from ct1
)
SELECT *
FROM final