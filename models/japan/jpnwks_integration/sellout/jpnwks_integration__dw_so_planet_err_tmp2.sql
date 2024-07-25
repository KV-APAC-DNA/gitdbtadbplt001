{{
    config
    (
        post_hook = "
                    UPDATE {{this}}
                    SET EXPORT_FLAG = '1'
                    WHERE EXPORT_FLAG = '0'
                    AND 
                    (
                        JCP_REC_SEQ IN 
                        (
                            SELECT jcp_rec_seq
                            FROM {{ ref('jpnwks_integration__consistency_error_2') }} 
                            WHERE exec_flag IN ('DELETE')
                        )
                        OR JCP_REC_SEQ IN 
                        (
                            SELECT jcp_rec_seq
                            FROM {{ ref('jpnwks_integration__wk_so_planet_cleansed') }} 
                        )
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
insert3
AS (
    SELECT *
    FROM {{ ref('jpnwks_integration__dw_so_planet_err_tmp1') }}
    ),
insert4
AS (
    SELECT DISTINCT JCP_REC_SEQ,
        ID,
        RCV_DT,
        TEST_FLAG,
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
        QTY,
        QTY_TYPE,
        PRICE,
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
        UNT_PRC,
        NET_PRC,
        SALES_CHAN_TYPE,
        JCP_CREATE_DATE,
        NULL as  JCP_RCV_DT_DUPLI,
        NULL as JCP_TEST_FLAG_DUPLI,
        NULL as JCP_QTY_DUPLI,
        NULL as JCP_PRICE_DUPLI,
        NULL as JCP_UNT_PRC_DUPLI,
        NULL as JCP_NET_PRC_DUPLI,
        '0' as EXPORT_FLAG
    FROM (
        SELECT SU.JCP_REC_SEQ,
            dup.ID,
            dup.RCV_DT,
            dup.TEST_FLAG,
            dup.BGN_SNDR_CD,
            dup.WS_CD,
            dup.RTL_TYPE,
            dup.RTL_CD,
            dup.TRADE_TYPE,
            dup.SHP_DATE,
            dup.SHP_NUM,
            dup.TRADE_CD,
            dup.DEP_CD,
            dup.CHG_CD,
            dup.PERSON_IN_CHARGE,
            dup.PERSON_NAME,
            dup.RTL_NAME,
            dup.RTL_HO_CD,
            dup.RTL_ADDRESS_CD,
            dup.DATA_TYPE,
            dup.OPT_FLD,
            dup.ITEM_NM,
            dup.ITEM_CD_TYP,
            dup.ITEM_CD,
            dup.QTY,
            dup.QTY_TYPE,
            dup.PRICE,
            dup.PRICE_TYPE,
            dup.BGN_SNDR_CD_GLN,
            dup.RCV_CD_GLN,
            dup.WS_CD_GLN,
            dup.SHP_WS_CD,
            dup.SHP_WS_CD_GLN,
            dup.REP_NAME_KANJI,
            dup.REP_INFO,
            dup.TRADE_CD_GLN,
            dup.RTL_CD_GLN,
            dup.RTL_NAME_KANJI,
            dup.RTL_HO_CD_GLN,
            dup.ITEM_CD_GTIN,
            dup.ITEM_NM_KANJI,
            dup.UNT_PRC,
            dup.NET_PRC,
            dup.SALES_CHAN_TYPE,
            dup.JCP_CREATE_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY SU.JCP_REC_SEQ ORDER BY tmp.priority
                ) AS rnk
        FROM CONSISTENCY_ERROR_2 SU
        INNER JOIN WK_SO_PLANET_NO_DUP dup ON SU.jcp_rec_seq = dup.jcp_rec_seq
        INNER JOIN temp_tbl tmp ON SU.exec_flag = tmp.exec_flag
        WHERE dup.jcp_rec_seq NOT IN (
                SELECT jcp_rec_seq
                FROM DW_SO_PLANET_ERR_CD
                WHERE error_cd = 'NRTL'
                )
        )
    WHERE rnk = 1
    ),
ct2
AS (
    SELECT *
    FROM insert3
    
    UNION ALL
    
    SELECT *
    FROM insert4
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
    from ct2
)
SELECT *
FROM final