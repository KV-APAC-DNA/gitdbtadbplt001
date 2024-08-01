{{
    config
    (   
        pre_hook = "{{build_wk_so_planet_revise_temp()}}",
        post_hook = "
                    INSERT INTO {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} (
                    jcp_rec_seq, id, rcv_dt, test_flag, 
                    bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, 
                    trade_type, shp_date, shp_num, trade_cd, 
                    dep_cd, chg_cd, person_in_charge, 
                    person_name, rtl_name, rtl_ho_cd, 
                    rtl_address_cd, data_type, opt_fld, 
                    item_nm, item_cd_typ, item_cd, qty, 
                    qty_type, price, price_type, bgn_sndr_cd_gln, 
                    rcv_cd_gln, ws_cd_gln, shp_ws_cd, 
                    shp_ws_cd_gln, rep_name_kanji, rep_info, 
                    trade_cd_gln, rtl_cd_gln, rtl_name_kanji, 
                    rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, 
                    unt_prc, net_prc, sales_chan_type, 
                    jcp_create_date
                    ) 
                    SELECT 
                    jcp_rec_seq, id, rcv_dt, test_flag, bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, trade_type, shp_date, shp_num, trade_cd, dep_cd, chg_cd, person_in_charge, person_name, rtl_name, rtl_ho_cd, rtl_address_cd_01 || rtl_address_cd_02, data_type, opt_fld, item_nm, item_cd_typ, item_cd, qty, qty_type, price, price_type, bgn_sndr_cd_gln, rcv_cd_gln, ws_cd_gln, shp_ws_cd, shp_ws_cd_gln, rep_name_kanji, rep_info, trade_cd_gln, rtl_cd_gln, rtl_name_kanji, rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, unt_prc, net_prc, sales_chan_type, jcp_create_date 
                    FROM 
                    {{this}} 
                    WHERE 
                    JCP_REC_SEQ IS NOT NULL;

                    INSERT INTO {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
                    SELECT (
                            SELECT MAX_VALUE::number AS max_value
                            FROM {{ ref('jpnedw_integration__mt_constant_seq')}}
                            WHERE IDENTIFY_CD = 'SEQUENCE_NO'
                            ) + ROW_NUMBER() OVER (
                            ORDER BY ID
                            ) AS jcp_rec_seq,
                        id, rcv_dt, test_flag, bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, trade_type, shp_date, shp_num, trade_cd, dep_cd, chg_cd, person_in_charge, person_name, rtl_name, rtl_ho_cd, rtl_address_cd_01, data_type, opt_fld, item_nm, item_cd_typ, item_cd, qty, qty_type, price, price_type, bgn_sndr_cd_gln, rcv_cd_gln, ws_cd_gln, shp_ws_cd, shp_ws_cd_gln, rep_name_kanji, rep_info, trade_cd_gln, rtl_cd_gln, rtl_name_kanji, rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, unt_prc, net_prc, sales_chan_type, CURRENT_TIMESTAMP()
                    FROM {{this}}
                    WHERE JCP_REC_SEQ IS NULL;

                    INSERT INTO {{ ref ('jpnitg_integration__dw_so_planet_err') }}
                    SELECT 
                    ERR.JCP_REC_SEQ, ERR.ID, ERR.RCV_DT, ERR.TEST_FLAG, ERR.BGN_SNDR_CD, ERR.WS_CD, ERR.RTL_TYPE, ERR.RTL_CD, ERR.TRADE_TYPE, ERR.SHP_DATE, ERR.SHP_NUM, ERR.TRADE_CD, ERR.DEP_CD, ERR.CHG_CD, ERR.PERSON_IN_CHARGE, ERR.PERSON_NAME, ERR.RTL_NAME, ERR.RTL_HO_CD, ERR.RTL_ADDRESS_CD, ERR.DATA_TYPE, ERR.OPT_FLD, ERR.ITEM_NM, ERR.ITEM_CD_TYP, ERR.ITEM_CD, ERR.QTY, ERR.QTY_TYPE, ERR.PRICE, ERR.PRICE_TYPE, ERR.BGN_SNDR_CD_GLN, ERR.RCV_CD_GLN, ERR.WS_CD_GLN, ERR.SHP_WS_CD, ERR.SHP_WS_CD_GLN, ERR.REP_NAME_KANJI, ERR.REP_INFO, ERR.TRADE_CD_GLN, ERR.RTL_CD_GLN, ERR.RTL_NAME_KANJI, ERR.RTL_HO_CD_GLN, ERR.ITEM_CD_GTIN, ERR.ITEM_NM_KANJI, ERR.UNT_PRC, ERR.NET_PRC, ERR.SALES_CHAN_TYPE, ERR.JCP_CREATE_DATE, NULL, NULL, NULL, NULL, NULL, NULL, 0
                    FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} ERR,
                    (
                        SELECT 
                        COUNT(*), ID, BGN_SNDR_CD, WS_CD, RTL_TYPE, RTL_CD, TRADE_TYPE, SHP_DATE, SHP_NUM, TRADE_CD, DEP_CD, CHG_CD, PERSON_IN_CHARGE, PERSON_NAME, RTL_NAME, RTL_HO_CD, RTL_ADDRESS_CD, DATA_TYPE, OPT_FLD, ITEM_NM, ITEM_CD_TYP, ITEM_CD, QTY_TYPE, PRICE_TYPE, BGN_SNDR_CD_GLN, RCV_CD_GLN, WS_CD_GLN, SHP_WS_CD, SHP_WS_CD_GLN, REP_NAME_KANJI, REP_INFO, TRADE_CD_GLN, RTL_CD_GLN, RTL_NAME_KANJI, RTL_HO_CD_GLN, ITEM_CD_GTIN, ITEM_NM_KANJI, SALES_CHAN_TYPE
                        FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
                        GROUP BY 
                        ID, BGN_SNDR_CD, WS_CD, RTL_TYPE, RTL_CD, TRADE_TYPE, SHP_DATE, SHP_NUM, TRADE_CD, DEP_CD, CHG_CD, PERSON_IN_CHARGE, PERSON_NAME, RTL_NAME, RTL_HO_CD, RTL_ADDRESS_CD, DATA_TYPE, OPT_FLD, ITEM_NM, ITEM_CD_TYP, ITEM_CD, QTY_TYPE, PRICE_TYPE, BGN_SNDR_CD_GLN, RCV_CD_GLN, WS_CD_GLN, SHP_WS_CD, SHP_WS_CD_GLN, REP_NAME_KANJI, REP_INFO, TRADE_CD_GLN, RTL_CD_GLN, RTL_NAME_KANJI, RTL_HO_CD_GLN, ITEM_CD_GTIN, ITEM_NM_KANJI, SALES_CHAN_TYPE
                        HAVING COUNT(*) > 1
                        ) KEY
                    WHERE NVL(ERR.ID, 0) = NVL(KEY.ID, 0) AND NVL(ERR.BGN_SNDR_CD, ' ') = NVL(KEY.BGN_SNDR_CD, ' ') AND NVL(ERR.WS_CD, ' ') = NVL(KEY.WS_CD, ' ') AND NVL(ERR.RTL_TYPE, ' ') = NVL(KEY.RTL_TYPE, ' ') AND NVL(ERR.RTL_CD, ' ') = NVL(KEY.RTL_CD, ' ') AND NVL(ERR.TRADE_TYPE, ' ') = NVL(KEY.TRADE_TYPE, ' ') AND NVL(ERR.SHP_DATE, ' ') = NVL(KEY.SHP_DATE, ' ') AND NVL(ERR.SHP_NUM, ' ') = NVL(KEY.SHP_NUM, ' ') AND NVL(ERR.TRADE_CD, ' ') = NVL(KEY.TRADE_CD, ' ') AND NVL(ERR.DEP_CD, ' ') = NVL(KEY.DEP_CD, ' ') AND NVL(ERR.CHG_CD, ' ') = NVL(KEY.CHG_CD, ' ') AND NVL(ERR.PERSON_IN_CHARGE, ' ') = NVL(KEY.PERSON_IN_CHARGE, ' ') AND NVL(ERR.PERSON_NAME, ' ') = NVL(KEY.PERSON_NAME, ' ') AND NVL(ERR.RTL_NAME, ' ') = NVL(KEY.RTL_NAME, ' ') AND NVL(ERR.RTL_HO_CD, ' ') = NVL(KEY.RTL_HO_CD, ' ') AND NVL(ERR.RTL_ADDRESS_CD, ' ') = NVL(KEY.RTL_ADDRESS_CD, ' ') AND NVL(ERR.DATA_TYPE, ' ') = NVL(KEY.DATA_TYPE, ' ') AND NVL(ERR.OPT_FLD, ' ') = NVL(KEY.OPT_FLD, ' ') AND NVL(ERR.ITEM_NM, ' ') = NVL(KEY.ITEM_NM, ' ') AND NVL(ERR.ITEM_CD_TYP, ' ') = NVL(KEY.ITEM_CD_TYP, ' ') AND NVL(ERR.ITEM_CD, ' ') = NVL(KEY.ITEM_CD, ' ') AND NVL(ERR.QTY_TYPE, ' ') = NVL(KEY.QTY_TYPE, ' ') AND NVL(ERR.PRICE_TYPE, ' ') = NVL(KEY.PRICE_TYPE, ' ') AND NVL(ERR.BGN_SNDR_CD_GLN, ' ') = NVL(KEY.BGN_SNDR_CD_GLN, ' ') AND NVL(ERR.RCV_CD_GLN, ' ') = NVL(KEY.RCV_CD_GLN, ' ') AND NVL(ERR.WS_CD_GLN, ' ') = NVL(KEY.WS_CD_GLN, ' ') AND NVL(ERR.SHP_WS_CD, ' ') = NVL(KEY.SHP_WS_CD, ' ') AND NVL(ERR.SHP_WS_CD_GLN, ' ') = NVL(KEY.SHP_WS_CD_GLN, ' ') AND NVL(ERR.REP_NAME_KANJI, ' ') = NVL(KEY.REP_NAME_KANJI, ' ') AND NVL(ERR.REP_INFO, ' ') = NVL(KEY.REP_INFO, ' ') AND NVL(ERR.TRADE_CD_GLN, ' ') = NVL(KEY.TRADE_CD_GLN, ' ') AND NVL(ERR.RTL_CD_GLN, ' ') = NVL(KEY.RTL_CD_GLN, ' ') AND NVL(ERR.RTL_NAME_KANJI, ' ') = NVL(KEY.RTL_NAME_KANJI, ' ') AND NVL(ERR.RTL_HO_CD_GLN, ' ') = NVL(KEY.RTL_HO_CD_GLN, ' ') AND NVL(ERR.ITEM_CD_GTIN, ' ') = NVL(KEY.ITEM_CD_GTIN, ' ') AND NVL(ERR.ITEM_NM_KANJI, ' ') = NVL(KEY.ITEM_NM_KANJI, ' ') AND NVL(ERR.SALES_CHAN_TYPE, ' ') = NVL(KEY.SALES_CHAN_TYPE, ' ');


                    INSERT INTO {{ ref ('jpnitg_integration__dw_so_planet_err_cd') }}
                    select JCP_REC_SEQ,	'DUPL',
                            '0' from {{ ref ('jpnitg_integration__dw_so_planet_err') }}
                    WHERE EXPORT_FLAG = '0' and JCP_REC_SEQ in (Select distinct JCP_REC_SEQ from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}); 
                    
                    delete from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
                    where JCP_REC_SEQ in 
                    (Select distinct JCP_REC_SEQ from  {{ ref ('jpnitg_integration__dw_so_planet_err') }} where EXPORT_FLAG = '0');

                    UPDATE {{ ref('jpnedw_integration__mt_constant_seq') }} SET MAX_VALUE=(select max(JCP_REC_SEQ) from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }});

                    INSERT INTO {{ ref ('jpnitg_integration__dw_so_planet_err') }} 
                        (
                        JCP_REC_SEQ, ID, RCV_DT, TEST_FLAG, BGN_SNDR_CD, WS_CD, RTL_TYPE, RTL_CD, TRADE_TYPE, SHP_DATE, SHP_NUM, TRADE_CD, DEP_CD, CHG_CD, PERSON_IN_CHARGE, PERSON_NAME, RTL_NAME, RTL_HO_CD, RTL_ADDRESS_CD, DATA_TYPE, OPT_FLD, ITEM_NM, ITEM_CD_TYP, ITEM_CD, QTY, QTY_TYPE, PRICE, PRICE_TYPE, BGN_SNDR_CD_GLN, RCV_CD_GLN, WS_CD_GLN, SHP_WS_CD, SHP_WS_CD_GLN, REP_NAME_KANJI, REP_INFO, TRADE_CD_GLN, RTL_CD_GLN, RTL_NAME_KANJI, RTL_HO_CD_GLN, ITEM_CD_GTIN, ITEM_NM_KANJI, UNT_PRC, NET_PRC, SALES_CHAN_TYPE, JCP_CREATE_DATE, JCP_RCV_DT_DUPLI, JCP_TEST_FLAG_DUPLI, JCP_QTY_DUPLI, JCP_PRICE_DUPLI, JCP_UNT_PRC_DUPLI, JCP_NET_PRC_DUPLI, EXPORT_FLAG
                        )
                    SELECT 
                        A.JCP_REC_SEQ, A.ID, A.RCV_DT, A.TEST_FLAG, A.BGN_SNDR_CD, A.WS_CD, A.RTL_TYPE, A.RTL_CD, A.TRADE_TYPE, A.SHP_DATE, A.SHP_NUM, A.TRADE_CD, A.DEP_CD, A.CHG_CD, A.PERSON_IN_CHARGE, A.PERSON_NAME, A.RTL_NAME, A.RTL_HO_CD, A.RTL_ADDRESS_CD, A.DATA_TYPE, A.OPT_FLD, A.ITEM_NM, A.ITEM_CD_TYP, A.ITEM_CD, A.QTY, A.QTY_TYPE, A.PRICE, A.PRICE_TYPE, A.BGN_SNDR_CD_GLN, A.RCV_CD_GLN, A.WS_CD_GLN, A.SHP_WS_CD, A.SHP_WS_CD_GLN, A.REP_NAME_KANJI, A.REP_INFO, A.TRADE_CD_GLN, A.RTL_CD_GLN, A.RTL_NAME_KANJI, A.RTL_HO_CD_GLN, A.ITEM_CD_GTIN, A.ITEM_NM_KANJI, A.UNT_PRC, A.NET_PRC, A.SALES_CHAN_TYPE, A.JCP_CREATE_DATE, NULL JCP_RCV_DT_DUPLI, NULL JCP_TEST_FLAG_DUPLI, NULL JCP_QTY_DUPLI, NULL JCP_PRICE_DUPLI, NULL JCP_UNT_PRC_DUPLI, NULL JCP_NET_PRC_DUPLI, '0' EXPORT_FLAG
                    FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} A,
                        (
                            SELECT A.RTL_CD,
                                COUNT(*) v_CNT
                            FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} A,
                                {{ ref('jpnedw_integration__edi_excl_rtlr') }} B
                            WHERE B.RTLR_CD = A.RTL_CD
                            GROUP BY A.RTL_CD
                            HAVING COUNT(*) > 0
                            ) C
                    WHERE A.RTL_CD = C.RTL_CD;


                    INSERT INTO {{ ref ('jpnitg_integration__dw_so_planet_err_cd') }}  (
                        JCP_REC_SEQ,
                        ERROR_CD,
                        EXPORT_FLAG
                        )
                    SELECT A.JCP_REC_SEQ,
                        'NRTL' ERROR_CD,
                        '0' EXPORT_FLAG
                    FROM {{ ref ('jpnitg_integration__dw_so_planet_err') }}  B,
                        {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} A,
                        (
                            SELECT A.RTL_CD,
                                COUNT(*) v_CNT
                            FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} A,
                                {{ ref('jpnedw_integration__edi_excl_rtlr') }} B
                            WHERE B.RTLR_CD = A.RTL_CD
                            GROUP BY A.RTL_CD
                            HAVING COUNT(*) > 0
                            ) C
                    WHERE A.RTL_CD = C.RTL_CD
                        AND B.JCP_REC_SEQ = A.JCP_REC_SEQ;

                    INSERT INTO {{ ref ('jpnitg_integration__dw_so_planet_err') }} (
                        JCP_REC_SEQ, ID, RCV_DT, TEST_FLAG, BGN_SNDR_CD, WS_CD, RTL_TYPE, RTL_CD, TRADE_TYPE, SHP_DATE, SHP_NUM, TRADE_CD, DEP_CD, CHG_CD, PERSON_IN_CHARGE, PERSON_NAME, RTL_NAME, RTL_HO_CD, RTL_ADDRESS_CD, DATA_TYPE, OPT_FLD, ITEM_NM, ITEM_CD_TYP, ITEM_CD, QTY, QTY_TYPE, PRICE, PRICE_TYPE, BGN_SNDR_CD_GLN, RCV_CD_GLN, WS_CD_GLN, SHP_WS_CD, SHP_WS_CD_GLN, REP_NAME_KANJI, REP_INFO, TRADE_CD_GLN, RTL_CD_GLN, RTL_NAME_KANJI, RTL_HO_CD_GLN, ITEM_CD_GTIN, ITEM_NM_KANJI, UNT_PRC, NET_PRC, SALES_CHAN_TYPE, JCP_CREATE_DATE, JCP_RCV_DT_DUPLI, JCP_TEST_FLAG_DUPLI, JCP_QTY_DUPLI, JCP_PRICE_DUPLI, JCP_UNT_PRC_DUPLI, JCP_NET_PRC_DUPLI, EXPORT_FLAG
                        )
                    SELECT 
                        A.JCP_REC_SEQ, A.ID, A.RCV_DT, A.TEST_FLAG, A.BGN_SNDR_CD, A.WS_CD, A.RTL_TYPE, A.RTL_CD, A.TRADE_TYPE, A.SHP_DATE, A.SHP_NUM, A.TRADE_CD, A.DEP_CD, A.CHG_CD, A.PERSON_IN_CHARGE, A.PERSON_NAME, A.RTL_NAME, A.RTL_HO_CD, A.RTL_ADDRESS_CD, A.DATA_TYPE, A.OPT_FLD, A.ITEM_NM, A.ITEM_CD_TYP, A.ITEM_CD, A.QTY, A.QTY_TYPE, A.PRICE, A.PRICE_TYPE, A.BGN_SNDR_CD_GLN, A.RCV_CD_GLN, A.WS_CD_GLN, A.SHP_WS_CD, A.SHP_WS_CD_GLN, A.REP_NAME_KANJI, A.REP_INFO, A.TRADE_CD_GLN, A.RTL_CD_GLN, A.RTL_NAME_KANJI, A.RTL_HO_CD_GLN, A.ITEM_CD_GTIN, A.ITEM_NM_KANJI, A.UNT_PRC, A.NET_PRC, A.SALES_CHAN_TYPE, A.JCP_CREATE_DATE, NULL JCP_RCV_DT_DUPLI, NULL JCP_TEST_FLAG_DUPLI, NULL JCP_QTY_DUPLI, NULL JCP_PRICE_DUPLI, NULL JCP_UNT_PRC_DUPLI, NULL JCP_NET_PRC_DUPLI, '0' EXPORT_FLAG
                    FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} A
                    WHERE A.ITEM_CD = ' '
                        AND A.ITEM_NM = ' '
                        AND A.JCP_REC_SEQ NOT IN (
                            SELECT DISTINCT JCP_REC_SEQ
                            FROM {{ ref('jpnitg_integration__dw_so_planet_err') }}
                            );

                    INSERT INTO {{ ref ('jpnitg_integration__dw_so_planet_err_cd') }} (
                        JCP_REC_SEQ,
                        ERROR_CD,
                        EXPORT_FLAG
                        )
                    SELECT A.JCP_REC_SEQ,
                        'NITM' ERROR_CD,
                        '0' EXPORT_FLAG
                    FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} A
                    WHERE A.ITEM_CD = ' '
                        AND A.ITEM_NM = ' '
                        AND A.JCP_REC_SEQ NOT IN (
                            SELECT DISTINCT JCP_REC_SEQ
                            FROM {{ ref('jpnitg_integration__dw_so_planet_err') }}
                            );

                    
                    
                    DELETE
                    FROM {{this}};

                    INSERT INTO {{this}}
                    (
                    jcp_rec_seq, id, rcv_dt, test_flag, bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, trade_type, shp_date, shp_num, trade_cd, dep_cd, chg_cd, person_in_charge, person_name, rtl_name, rtl_ho_cd, rtl_address_cd_01, data_type, opt_fld, item_nm, item_cd_typ, item_cd, qty, qty_type, price, price_type, bgn_sndr_cd_gln, rcv_cd_gln, ws_cd_gln, shp_ws_cd, shp_ws_cd_gln, rep_name_kanji, rep_info, trade_cd_gln, rtl_cd_gln, rtl_name_kanji, rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, unt_prc, net_prc, sales_chan_type, jcp_create_date
                    )
                    SELECT 
                    R.jcp_rec_seq, R.id, R.rcv_dt, R.test_flag, R.bgn_sndr_cd, R.ws_cd, R.rtl_type, R.rtl_cd, R.trade_type, R.shp_date, R.shp_num, R.trade_cd, R.dep_cd, R.chg_cd, R.person_in_charge, R.person_name, R.rtl_name, R.rtl_ho_cd, R.rtl_address_cd, R.data_type, R.opt_fld, R.item_nm, R.item_cd_typ, R.item_cd, R.qty, R.qty_type, R.price, R.price_type, R.bgn_sndr_cd_gln, R.rcv_cd_gln, R.ws_cd_gln, R.shp_ws_cd, R.shp_ws_cd_gln, R.rep_name_kanji, R.rep_info, R.trade_cd_gln, R.rtl_cd_gln, R.rtl_name_kanji, R.rtl_ho_cd_gln, R.item_cd_gtin, R.item_nm_kanji, R.unt_prc, R.net_prc, R.sales_chan_type, R.jcp_create_date
                    FROM {{ source('jpnwks_integration', 'wk_so_planet_revise_temp') }} R;

                    INSERT INTO {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
                    ( 
                    jcp_rec_seq, id, rcv_dt, test_flag, bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, trade_type, shp_date, shp_num, trade_cd, dep_cd, chg_cd, person_in_charge, person_name, rtl_name, rtl_ho_cd, rtl_address_cd, data_type, opt_fld, item_nm, item_cd_typ, item_cd, qty, qty_type, price, price_type, bgn_sndr_cd_gln, rcv_cd_gln, ws_cd_gln, shp_ws_cd, shp_ws_cd_gln, rep_name_kanji, rep_info, trade_cd_gln, rtl_cd_gln, rtl_name_kanji, rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, unt_prc, net_prc, sales_chan_type, jcp_create_date
                    )
                    SELECT
                    jcp_rec_seq, id, rcv_dt, test_flag, bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, trade_type, shp_date, shp_num, trade_cd, dep_cd, chg_cd, person_in_charge, person_name, rtl_name, rtl_ho_cd, rtl_address_cd_01, data_type, opt_fld, item_nm, item_cd_typ, item_cd, qty, qty_type, price, price_type, bgn_sndr_cd_gln, rcv_cd_gln, ws_cd_gln, shp_ws_cd, shp_ws_cd_gln, rep_name_kanji, rep_info, trade_cd_gln, rtl_cd_gln, rtl_name_kanji, rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, unt_prc, net_prc, sales_chan_type, jcp_create_date
                    FROM 
                    {{this}}
                    WHERE JCP_REC_SEQ IS NOT NULL;


                    update {{ ref('jpnitg_integration__dw_so_planet_err') }}
                    SET EXPORT_FLAG='1'
                    where EXPORT_FLAG='0'
                    AND JCP_REC_SEQ IN(select jcp_rec_seq from {{ ref('jpnwks_integration__consistency_error_2') }} where exec_flag IN('MANUAL','AUTOCORRECT'))
                    AND JCP_REC_SEQ IN(select jcp_rec_seq from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }});

                    update {{ ref('jpnitg_integration__dw_so_planet_err_cd_2') }}
                    SET EXPORT_FLAG='1'
                    where EXPORT_FLAG='0'
                    AND JCP_REC_SEQ IN(select jcp_rec_seq from {{ ref('jpnwks_integration__consistency_error_2') }} where exec_flag IN('MANUAL','AUTOCORRECT'))
                    AND JCP_REC_SEQ IN(select jcp_rec_seq from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }});

                    update {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
                    SET EXPORT_FLAG='1'
                    where EXPORT_FLAG='0'
                    AND JCP_REC_SEQ IN(select jcp_rec_seq from {{ ref('jpnwks_integration__consistency_error_2') }} where exec_flag IN('MANUAL','AUTOCORRECT'))
                    AND JCP_REC_SEQ IN(select jcp_rec_seq from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }});


                    INSERT INTO {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
                    SELECT (SELECT MAX_VALUE::number as max_value FROM {{ ref('jpnedw_integration__mt_constant_seq') }} WHERE 
                    IDENTIFY_CD='SEQUENCE_NO') + ROW_NUMBER() OVER (
                                ORDER BY ID
                                )AS jcp_rec_seq,
                    id, rcv_dt, test_flag, bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, trade_type, shp_date, shp_num, trade_cd, dep_cd, chg_cd, person_in_charge, person_name, rtl_name, rtl_ho_cd, rtl_address_cd_01, data_type, opt_fld, item_nm, item_cd_typ, item_cd, qty, qty_type, price, price_type, bgn_sndr_cd_gln, rcv_cd_gln, ws_cd_gln, shp_ws_cd, shp_ws_cd_gln, rep_name_kanji, rep_info, trade_cd_gln, rtl_cd_gln, rtl_name_kanji, rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, unt_prc, net_prc, sales_chan_type, CURRENT_TIMESTAMP()::timestamp_ntz(9)
                    FROM  {{this}}
                    WHERE JCP_REC_SEQ IS NULL ;

                    UPDATE {{ ref('jpnedw_integration__mt_constant_seq') }}
                    SET MAX_VALUE=(select max(JCP_REC_SEQ) from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }});
                    "
    )
}}



WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'sdl_so_planet_no_dup') }}
),

final AS
(
    SELECT 
        jcp_rec_seq,
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
        COLUMN_17,
        rtl_name,
        rtl_ho_cd,
        rtl_address_cd_01,
        rtl_address_cd_02,
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
        jcp_create_date
    FROM source 
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.jcp_create_date > (select max(jcp_create_date) from {{ this }}) 
    {% endif %}
)

SELECT * FROM final