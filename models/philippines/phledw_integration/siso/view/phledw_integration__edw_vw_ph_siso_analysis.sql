with edw_ph_sellout_analysis as (
    select * from snaposeedw_integration.edw_ph_sellout_analysis
),
edw_product_key_attributes as (
    select * from aspedw_integration.edw_product_key_attributes
),
itg_query_parameters as (
    select * from snaposeitg_integration.itg_query_parameters
),
edw_vw_ph_sellout_inventory_fact as (
    select * from phledw_integration.edw_vw_os_sellout_inventory_fact
    where cntry_cd = 'PH'
),
edw_vw_os_time_dim as (
    select * from snenav01_workspace.SGPEDW_INTEGRATION__EDW_VW_OS_TIME_DIM
),
edw_vw_ph_dstrbtr_customer_dim as (
    select * from phledw_integration.edw_vw_os_dstrbtr_customer_dim
    where cntry_cd = 'PH'
),
edw_vw_ph_dstrbtr_material_dim as (
    select * from phledw_integration.edw_vw_os_dstrbtr_material_dim
    where cntry_cd = 'PH'
),
edw_vw_ph_material_dim as (
    select * from phledw_integration.edw_vw_os_material_dim
    where cntry_key = 'PH'
),
itg_mds_ph_lav_product as (
    select * from snaposeitg_integration.itg_mds_ph_lav_product
),
itg_mds_ph_pos_pricelist as (
    select * from snaposeitg_integration.itg_mds_ph_pos_pricelist
),
edw_mv_ph_customer_dim as (
    select * from snaposeedw_integration.edw_mv_ph_customer_dim
),
edw_vw_ph_sellin_sales_fact as (
    select * from phledw_integration.edw_vw_os_sellin_sales_fact
    where cntry_nm = 'PH'
),
edw_vw_ph_customer_dim as (
    select * from phledw_integration.edw_vw_os_customer_dim
    where sap_cntry_cd = 'PH'
),
dstrbtr_matl as
(
    select
        vosinv1.*,
        veomd.sap_mat_desc,
        veomd.sap_mat_type_cd,
        veomd.sap_mat_type_desc,
        veomd.sap_base_uom_cd,
        veomd.sap_prchse_uom_cd,
        veomd.sap_prod_sgmt_cd,
        veomd.sap_prod_sgmt_desc,
        veomd.sap_base_prod_cd,
        veomd.sap_base_prod_desc,
        veomd.sap_mega_brnd_cd,
        veomd.sap_mega_brnd_desc,
        veomd.sap_brnd_cd,
        veomd.sap_brnd_desc,
        veomd.sap_vrnt_cd,
        veomd.sap_vrnt_desc,
        veomd.sap_put_up_cd,
        veomd.sap_put_up_desc,
        veomd.sap_grp_frnchse_cd,
        veomd.sap_grp_frnchse_desc,
        veomd.sap_frnchse_cd,
        veomd.sap_frnchse_desc,
        veomd.sap_prod_frnchse_cd,
        veomd.sap_prod_frnchse_desc,
        veomd.sap_prod_mjr_cd,
        veomd.sap_prod_mjr_desc,
        veomd.sap_prod_mnr_cd,
        veomd.sap_prod_mnr_desc,
        veomd.sap_prod_hier_cd,
        veomd.sap_prod_hier_desc,
        veomd.gph_region,
        veomd.gph_prod_frnchse,
        veomd.gph_prod_brnd,
        veomd.gph_prod_vrnt,
        veomd.gph_prod_put_up_cd,
        veomd.gph_prod_put_up_desc,
        veomd.gph_prod_sub_brnd,
        veomd.gph_prod_needstate,
        veomd.gph_prod_ctgry,
        veomd.gph_prod_subctgry,
        veomd.gph_prod_sgmnt,
        veomd.gph_prod_subsgmnt,
        veomd.gph_prod_size,
        veomd.gph_prod_size_uom,
        veomd.pka_productkey
    FROM
        (
            select a.jj_year as jj_year,
                a.jj_qrtr as jj_qrtr,
                a.jj_mnth_id as jj_mnth_id,
                a.jj_mnth_no as jj_mnth_no,
                a.jj_mnth_wk_no as jj_mnth_wk_no,
                a.warehse_cd,
                a.dstrbtr_grp_cd,
                veodcd.sap_soldto_code,
                veodmd.sap_matl_num,
                a.dstrbtr_matl_num,
                veodmd.is_npi,
                veodmd.npi_str_period,
                veodmd.npi_end_period,
                veodmd.is_reg,
                veodmd.is_promo,
                veodmd.promo_strt_period,
                veodmd.promo_end_period,
                veodmd.is_mcl,
                veodmd.is_hero,
                end_stock_qty as end_stock_qty,
                end_stock_val as end_stock_val
            from
                (
                    select *
                    from
                        (
                            select *
                            from edw_vw_ph_sellout_inventory_fact
                            where cntry_cd = 'PH'
                        ) as b,
                        (
                            select distinct veotd.year as jj_year,
                                veotd.qrtr as jj_qrtr,
                                veotd.mnth_id as jj_mnth_id,
                                veotd.mnth_no as jj_mnth_no,
                                veotd.mnth_wk_no as jj_mnth_wk_no,
                                max(a.inv_dt) over (
                                    partition by jj_mnth_wk_no,
                                    jj_mnth_id
                                ) as last_day_wk
                            from edw_vw_ph_sellout_inventory_fact a,
                            (
                                select distinct 
                                "year" as year,
                                    qrtr,
                                    mnth_id,
                                    mnth_no,
                                    wk,
                                    mnth_wk_no,
                                    cal_date,
                                    cal_date_id
                                from edw_vw_os_time_dim
                            ) veotd
                            where to_date(veotd.cal_date) = to_date(a.inv_dt)
                            and a.cntry_cd = 'PH'
                        ) veostd
                    where to_date(veostd.last_day_wk) = to_date(b.inv_dt)
                ) a,
                (
                    select distinct dstrbtr_grp_cd,
                        sap_soldto_code
                    from edw_vw_ph_dstrbtr_customer_dim
                    where cntry_cd = 'PH'
                ) veodcd,
                (
                    select dstrbtr_matl_num,
                        dstrbtr_grp_cd,
                        sap_soldto_code,
                        sap_matl_num,
                        is_npi,
                        npi_str_period,
                        npi_end_period,
                        is_reg,
                        is_promo,
                        promo_strt_period,
                        promo_end_period,
                        is_mcl,
                        is_hero
                    from edw_vw_ph_dstrbtr_material_dim
                    where cntry_cd = 'PH'
                ) VEODMD
            where trim(veodcd.dstrbtr_grp_cd(+)) = trim(a.dstrbtr_grp_cd)
                and trim(veodmd.dstrbtr_grp_cd(+)) = trim(a.dstrbtr_grp_cd)
                and upper(trim(veodmd.dstrbtr_matl_num(+))) = upper(trim(a.dstrbtr_matl_num))
        ) vosinv1,
        (
            select mat.*,
                prod.pka_productkey
            from edw_vw_ph_material_dim mat
                left join edw_product_key_attributes prod on ltrim(mat.sap_matl_num, '0') = ltrim(prod.matl_num, '0')
            where cntry_key = 'PH'
                AND UPPER(prod.ctry_nm) = 'PHILIPPINES'
        ) VEOMD
    WHERE trim(LTRIM(VEOMD.SAP_MATL_NUM(+), '0')) = trim(LTRIM(VOSINV1.SAP_MATL_NUM, '0'))
        AND LTRIM(NVL(trim(VOSINV1.DSTRBTR_MATL_NUM),'NA'), '0') NOT IN (
            SELECT DISTINCT LTRIM(NVL(trim(ITEM_CD), 'NA'), '0')
            FROM ITG_MDS_PH_LAV_PRODUCT
            WHERE ACTIVE = 'Y'
        )
),
set_1 as
(
    SELECT
        SELL.JJ_YEAR AS JJ_YEAR,
        SELL.JJ_QRTR AS JJ_QRTR,
        SELL.JJ_MNTH_ID AS JJ_MNTH_ID,
        SELL.JJ_MNTH_NO AS JJ_MNTH_NO,
        SELL.JJ_MNTH_WK_NO AS JJ_MNTH_WK_NO,
        NULL AS WAREHSE_CD,
        SELL.SAP_CNTRY_NM AS CNTRY_NM,
        SELL.DSTRBTR_GRP_CD,
        --   VEOSF.SAP_SOLDTO_CODE,
        --   EOCD.CUST_NM AS SAP_SOLDTO_NM,
        SELL.REGION,
        SELL.CHNL_CD AS SI_CHNL_CD,
        SELL.CHNL_DESC AS SI_CHNL_DESC,
        SELL.SUB_CHNL_CD AS SI_SUB_CHNL_CD,
        SELL.SUB_CHNL_DESC AS SI_SUB_CHNL_DESC,
        SELL.PARENT_CUSTOMER_CD,
        SELL.PARENT_CUSTOMER,
        SELL.ACCOUNT_GRP,
        'GENERAL TRADE' AS TRADE_TYPE,
        SELL.SLS_GRP_DESC,
        SELL.SAP_STATE_CD,
        SELL.SAP_SLS_ORG,
        SELL.SAP_CMP_ID,
        SELL.SAP_CNTRY_CD,
        SELL.SAP_CNTRY_NM,
        SELL.SAP_ADDR,
        SELL.SAP_REGION,
        SELL.SAP_CITY,
        SELL.SAP_POST_CD,
        SELL.SAP_CHNL_CD,
        SELL.SAP_CHNL_DESC,
        SELL.SAP_SLS_OFFICE_CD,
        SELL.SAP_SLS_OFFICE_DESC,
        SELL.SAP_SLS_GRP_CD,
        SELL.SAP_SLS_GRP_DESC,
        SELL.SAP_CURR_CD,
        SELL.GCH_REGION,
        SELL.GCH_CLUSTER,
        SELL.GCH_SUBCLUSTER,
        SELL.GCH_MARKET,
        SELL.GCH_RETAIL_BANNER,
        SELL.SKU,
        SELL.SKU_DESC,
        SELL.SAP_MAT_TYPE_CD,
        SELL.SAP_MAT_TYPE_DESC,
        SELL.SAP_BASE_UOM_CD,
        SELL.SAP_PRCHSE_UOM_CD,
        SELL.SAP_PROD_SGMT_CD,
        SELL.SAP_PROD_SGMT_DESC,
        SELL.SAP_BASE_PROD_CD,
        SELL.SAP_BASE_PROD_DESC,
        SELL.SAP_MEGA_BRND_CD,
        SELL.SAP_MEGA_BRND_DESC,
        SELL.SAP_BRND_CD,
        SELL.SAP_BRND_DESC,
        SELL.SAP_VRNT_CD,
        SELL.SAP_VRNT_DESC,
        SELL.SAP_PUT_UP_CD,
        SELL.SAP_PUT_UP_DESC,
        SELL.SAP_GRP_FRNCHSE_CD,
        SELL.SAP_GRP_FRNCHSE_DESC,
        SELL.SAP_FRNCHSE_CD,
        SELL.SAP_FRNCHSE_DESC,
        SELL.SAP_PROD_FRNCHSE_CD,
        SELL.SAP_PROD_FRNCHSE_DESC,
        SELL.SAP_PROD_MJR_CD,
        SELL.SAP_PROD_MJR_DESC,
        SELL.SAP_PROD_MNR_CD,
        SELL.SAP_PROD_MNR_DESC,
        SELL.SAP_PROD_HIER_CD,
        SELL.SAP_PROD_HIER_DESC,
        SELL.GLOBAL_MAT_REGION,
        SELL.GLOBAL_PROD_FRANCHISE,
        SELL.GLOBAL_PROD_BRAND,
        SELL.GLOBAL_PROD_VARIANT,
        SELL.GLOBAL_PROD_PUT_UP_CD,
        SELL.GLOBAL_PUT_UP_DESC,
        SELL.GLOBAL_PROD_SUB_BRAND,
        SELL.GLOBAL_PROD_NEED_STATE,
        SELL.GLOBAL_PROD_CATEGORY,
        SELL.GLOBAL_PROD_SUBCATEGORY,
        SELL.GLOBAL_PROD_SEGMENT,
        SELL.GLOBAL_PROD_SUBSEGMENT,
        SELL.GLOBAL_PROD_SIZE,
        SELL.GLOBAL_PROD_SIZE_UOM,
        SELL.BILL_DOC AS SO_BILL_DOC,
        SELL.SLS_QTY AS SO_SLS_QTY,
        SELL.RET_QTY AS SO_RET_QTY,
        SELL.SLS_QTY_PC AS SO_SLS_QTY_PC,
        SELL.RET_QTY_PC AS SO_RET_QTY_PC,
        SELL.GRS_TRD_SLS AS SO_GRS_TRD_SLS,
        SELL.RET_VAL AS SO_RET_VAL,
        SELL.TRD_DISCNT AS SO_TRD_DISCNT,
        SELL.TRD_SLS AS SO_TRD_SLS,
        SELL.NET_TRD_SLS AS SO_NET_TRD_SLS,
        SELL.JJ_GRS_TRD_SLS AS SO_JJ_GRS_TRD_SLS,
        SELL.JJ_RET_VAL AS SO_JJ_RET_VAL,
        SELL.JJ_TRD_SLS AS SO_JJ_TRD_SLS,
        SELL.JJ_NET_TRD_SLS AS SO_JJ_NET_TRD_SLS,
        SELL.IS_NPI,
        SELL.NPI_STR_PERIOD,
        SELL.NPI_END_PERIOD,
        SELL.IS_REG,
        SELL.IS_PROMO,
        SELL.PROMO_STRT_PERIOD,
        SELL.PROMO_END_PERIOD,
        SELL.IS_MCL,
        SELL.IS_HERO,
        0 AS LOCAL_MAT_LISTPRICEUNIT,
        0 AS SI_SLS_QTY,
        0 AS SI_RET_QTY,
        0 AS SI_SLS_LESS_RTN_QTY,
        0 AS SI_GTS_VAL,
        0 AS SI_RET_VAL,
        0 AS SI_GTS_LESS_RTN_VAL,
        0 AS SI_NTS_QTY,
        0 AS SI_NTS_VAL,
        0 AS SI_TP_VAL,
        0 AS END_STOCK_QTY,
        0 AS END_STOCK_VAL,
        prod.pka_productkey
    FROM EDW_PH_SELLOUT_ANALYSIS SELL
    LEFT JOIN EDW_PRODUCT_KEY_ATTRIBUTES PROD ON trim(SELL.SKU) = trim(ltrim(prod.matl_num, '0'))
    AND upper(prod.ctry_nm) = 'PHILIPPINES'
    WHERE SELL.rka_cd IS NULL
    OR (
        SELL.rka_cd IS NOT NULL
        AND (
            SELL.rka_cd = ' '
            OR SELL.rka_cd NOT IN (
                SELECT DISTINCT parameter_value FROM itg_query_parameters
                WHERE country_code = 'PH'
                    AND parameter_name = 'rka_cd'
            )
        )
    )
),
set_2 as
(
    SELECT
        VOSINV.JJ_YEAR AS JJ_YEAR,
        VOSINV.JJ_QRTR AS JJ_QRTR,
        VOSINV.JJ_MNTH_ID AS JJ_MNTH_ID,
        VOSINV.JJ_MNTH_NO AS JJ_MNTH_NO,
        VOSINV.JJ_MNTH_WK_NO AS JJ_MNTH_WK_NO,
        VOSINV.WAREHSE_CD,
        VEOCD.SAP_CNTRY_NM AS CNTRY_NM,
        VOSINV.DSTRBTR_GRP_CD AS DSTRBTR_GRP_CD,
        EOCD.REGION,
        EOCD.CHANNEL_CD AS SI_CHNL_CD,
        EOCD.CHANNEL_DESC AS SI_CHNL_DESC,
        EOCD.SUB_CHNL_CD AS SI_SUB_CHNL_CD,
        EOCD.SUB_CHNL_DESC AS SI_SUB_CHNL_DESC,
        EOCD.PARENT_CUST_CD AS PARENT_CUSTOMER_CD,
        EOCD.PARENT_CUST_NM AS PARENT_CUSTOMER,
        EOCD.RPT_GRP_6_DESC AS ACCOUNT_GRP,
        'GENERAL TRADE' AS TRADE_TYPE,
        EOCD.RPT_GRP_2_DESC AS SLS_GRP_DESC,
        VEOCD.SAP_STATE_CD,
        VEOCD.SAP_SLS_ORG,
        VEOCD.SAP_CMP_ID,
        VEOCD.SAP_CNTRY_CD,
        VEOCD.SAP_CNTRY_NM,
        VEOCD.SAP_ADDR,
        VEOCD.SAP_REGION,
        VEOCD.SAP_CITY,
        VEOCD.SAP_POST_CD,
        VEOCD.SAP_CHNL_CD,
        VEOCD.SAP_CHNL_DESC,
        VEOCD.SAP_SLS_OFFICE_CD,
        VEOCD.SAP_SLS_OFFICE_DESC,
        VEOCD.SAP_SLS_GRP_CD,
        VEOCD.SAP_SLS_GRP_DESC,
        VEOCD.SAP_CURR_CD,
        VEOCD.GCH_REGION,
        VEOCD.GCH_CLUSTER,
        VEOCD.GCH_SUBCLUSTER,
        VEOCD.GCH_MARKET,
        VEOCD.GCH_RETAIL_BANNER,
        LTRIM(VOSINV.SAP_MATL_NUM, '0') AS SKU,
        VOSINV.SAP_MAT_DESC AS SKU_DESC,
        VOSINV.SAP_MAT_TYPE_CD,
        VOSINV.SAP_MAT_TYPE_DESC,
        VOSINV.SAP_BASE_UOM_CD,
        VOSINV.SAP_PRCHSE_UOM_CD,
        VOSINV.SAP_PROD_SGMT_CD,
        VOSINV.SAP_PROD_SGMT_DESC,
        VOSINV.SAP_BASE_PROD_CD,
        VOSINV.SAP_BASE_PROD_DESC,
        VOSINV.SAP_MEGA_BRND_CD,
        VOSINV.SAP_MEGA_BRND_DESC,
        VOSINV.SAP_BRND_CD,
        VOSINV.SAP_BRND_DESC,
        VOSINV.SAP_VRNT_CD,
        VOSINV.SAP_VRNT_DESC,
        VOSINV.SAP_PUT_UP_CD,
        VOSINV.SAP_PUT_UP_DESC,
        VOSINV.SAP_GRP_FRNCHSE_CD,
        VOSINV.SAP_GRP_FRNCHSE_DESC,
        VOSINV.SAP_FRNCHSE_CD,
        VOSINV.SAP_FRNCHSE_DESC,
        VOSINV.SAP_PROD_FRNCHSE_CD,
        VOSINV.SAP_PROD_FRNCHSE_DESC,
        VOSINV.SAP_PROD_MJR_CD,
        VOSINV.SAP_PROD_MJR_DESC,
        VOSINV.SAP_PROD_MNR_CD,
        VOSINV.SAP_PROD_MNR_DESC,
        VOSINV.SAP_PROD_HIER_CD,
        VOSINV.SAP_PROD_HIER_DESC,
        VOSINV.GPH_REGION AS GLOBAL_MAT_REGION,
        VOSINV.GPH_PROD_FRNCHSE AS GLOBAL_PROD_FRANCHISE,
        VOSINV.GPH_PROD_BRND AS GLOBAL_PROD_BRAND,
        VOSINV.GPH_PROD_VRNT AS GLOBAL_PROD_VARIANT,
        VOSINV.GPH_PROD_PUT_UP_CD AS GLOBAL_PROD_PUT_UP_CD,
        VOSINV.GPH_PROD_PUT_UP_DESC AS GLOBAL_PUT_UP_DESC,
        VOSINV.GPH_PROD_SUB_BRND AS GLOBAL_PROD_SUB_BRAND,
        VOSINV.GPH_PROD_NEEDSTATE AS GLOBAL_PROD_NEED_STATE,
        VOSINV.GPH_PROD_CTGRY AS GLOBAL_PROD_CATEGORY,
        VOSINV.GPH_PROD_SUBCTGRY AS GLOBAL_PROD_SUBCATEGORY,
        VOSINV.GPH_PROD_SGMNT AS GLOBAL_PROD_SEGMENT,
        VOSINV.GPH_PROD_SUBSGMNT AS GLOBAL_PROD_SUBSEGMENT,
        VOSINV.GPH_PROD_SIZE AS GLOBAL_PROD_SIZE,
        VOSINV.GPH_PROD_SIZE_UOM AS GLOBAL_PROD_SIZE_UOM,
        NULL AS SO_BILL_DOC,
        0 AS SO_SLS_QTY,
        0 AS SO_RET_QTY,
        0 AS SO_SLS_QTY_PC,
        0 AS SO_RET_QTY_PC,
        0 AS SO_GRS_TRD_SLS,
        0 AS SO_RET_VAL,
        0 AS SO_TRD_DISCNT,
        0 AS SO_TRD_SLS,
        0 AS SO_NET_TRD_SLS,
        0 AS SO_JJ_GRS_TRD_SLS,
        0 AS SO_JJ_RET_VAL,
        0 AS SO_JJ_TRD_SLS,
        0 AS SO_JJ_NET_TRD_SLS,
        VOSINV.IS_NPI,
        VOSINV.NPI_STR_PERIOD,
        VOSINV.NPI_END_PERIOD,
        VOSINV.IS_REG,
        VOSINV.IS_PROMO,
        VOSINV.PROMO_STRT_PERIOD,
        VOSINV.PROMO_END_PERIOD,
        VOSINV.IS_MCL,
        VOSINV.IS_HERO,
        0 AS LOCAL_MAT_LISTPRICEUNIT,
        0 AS SI_SLS_QTY,
        0 AS SI_RET_QTY,
        0 AS SI_SLS_LESS_RTN_QTY,
        0 AS SI_GTS_VAL,
        0 AS SI_RET_VAL,
        0 AS SI_GTS_LESS_RTN_VAL,
        0 AS SI_NTS_QTY,
        0 AS SI_NTS_VAL,
        0 AS SI_TP_VAL,
        VOSINV.END_STOCK_QTY AS END_STOCK_QTY,
        VOSINV.END_STOCK_VAL AS END_STOCK_VAL,
        VOSINV.pka_productkey
    FROM
    (
        SELECT * FROM DSTRBTR_MATL
        UNION ALL
        SELECT
            VOSINV1.*,
            VEOMD.SAP_MAT_DESC,
            VEOMD.SAP_MAT_TYPE_CD,
            VEOMD.SAP_MAT_TYPE_DESC,
            VEOMD.SAP_BASE_UOM_CD,
            VEOMD.SAP_PRCHSE_UOM_CD,
            VEOMD.SAP_PROD_SGMT_CD,
            VEOMD.SAP_PROD_SGMT_DESC,
            VEOMD.SAP_BASE_PROD_CD,
            VEOMD.SAP_BASE_PROD_DESC,
            VEOMD.SAP_MEGA_BRND_CD,
            VEOMD.SAP_MEGA_BRND_DESC,
            VEOMD.SAP_BRND_CD,
            VEOMD.SAP_BRND_DESC,
            VEOMD.SAP_VRNT_CD,
            VEOMD.SAP_VRNT_DESC,
            VEOMD.SAP_PUT_UP_CD,
            VEOMD.SAP_PUT_UP_DESC,
            VEOMD.SAP_GRP_FRNCHSE_CD,
            VEOMD.SAP_GRP_FRNCHSE_DESC,
            VEOMD.SAP_FRNCHSE_CD,
            VEOMD.SAP_FRNCHSE_DESC,
            VEOMD.SAP_PROD_FRNCHSE_CD,
            VEOMD.SAP_PROD_FRNCHSE_DESC,
            VEOMD.SAP_PROD_MJR_CD,
            VEOMD.SAP_PROD_MJR_DESC,
            VEOMD.SAP_PROD_MNR_CD,
            VEOMD.SAP_PROD_MNR_DESC,
            VEOMD.SAP_PROD_HIER_CD,
            VEOMD.SAP_PROD_HIER_DESC,
            VEOMD.GPH_REGION,
            VEOMD.GPH_PROD_FRNCHSE,
            VEOMD.GPH_PROD_BRND,
            VEOMD.GPH_PROD_VRNT,
            VEOMD.GPH_PROD_PUT_UP_CD,
            VEOMD.GPH_PROD_PUT_UP_DESC,
            VEOMD.GPH_PROD_SUB_BRND,
            VEOMD.GPH_PROD_NEEDSTATE,
            VEOMD.GPH_PROD_CTGRY,
            VEOMD.GPH_PROD_SUBCTGRY,
            VEOMD.GPH_PROD_SGMNT,
            VEOMD.GPH_PROD_SUBSGMNT,
            VEOMD.GPH_PROD_SIZE,
            VEOMD.GPH_PROD_SIZE_UOM,
            VEOMD.pka_productkey
        FROM
            (
                SELECT A.JJ_YEAR AS JJ_YEAR,
                    A.JJ_QRTR AS JJ_QRTR,
                    A.JJ_MNTH_ID AS JJ_MNTH_ID,
                    A.JJ_MNTH_NO AS JJ_MNTH_NO,
                    A.JJ_MNTH_WK_NO AS JJ_MNTH_WK_NO,
                    A.WAREHSE_CD,
                    A.DSTRBTR_GRP_CD,
                    VEODCD.SAP_SOLDTO_CODE,
                    A.DSTRBTR_MATL_NUM AS SAP_MATL_NUM,
                    A.DSTRBTR_MATL_NUM,
                    VEODMD.IS_NPI,
                    VEODMD.NPI_STR_PERIOD,
                    VEODMD.NPI_END_PERIOD,
                    VEODMD.IS_REG,
                    VEODMD.IS_PROMO,
                    VEODMD.PROMO_STRT_PERIOD,
                    VEODMD.PROMO_END_PERIOD,
                    VEODMD.IS_MCL,
                    VEODMD.IS_HERO,
                    END_STOCK_QTY AS END_STOCK_QTY,
                    END_STOCK_VAL AS END_STOCK_VAL
                FROM
                    (
                        SELECT * FROM
                        (
                            SELECT *
                            FROM EDW_vw_ph_SELLOUT_INVENTORY_FACT
                            WHERE CNTRY_CD = 'PH'
                        ) AS B,
                        (
                            SELECT DISTINCT VEOTD.YEAR AS JJ_YEAR,
                                VEOTD.QRTR AS JJ_QRTR,
                                VEOTD.MNTH_ID AS JJ_MNTH_ID,
                                VEOTD.MNTH_NO AS JJ_MNTH_NO,
                                VEOTD.MNTH_WK_NO AS JJ_MNTH_WK_NO,
                                MAX(A.INV_DT) OVER (
                                    PARTITION BY JJ_MNTH_WK_NO,
                                    JJ_MNTH_ID
                                ) AS LAST_DAY_WK
                            FROM EDW_vw_ph_SELLOUT_INVENTORY_FACT A,
                                (
                                    SELECT DISTINCT "year" as year,
                                        QRTR,
                                        MNTH_ID,
                                        MNTH_NO,
                                        WK,
                                        MNTH_WK_NO,
                                        CAL_DATE,
                                        CAL_DATE_ID
                                    FROM edw_vw_os_time_dim
                                ) VEOTD
                            WHERE to_date(VEOTD.CAL_DATE) = to_date(A.INV_DT)
                                AND A.CNTRY_CD = 'PH'
                        ) VEOSTD
                        WHERE to_date(VEOSTD.LAST_DAY_WK) = to_date(B.INV_DT)
                    ) A,
                    (
                        SELECT DISTINCT DSTRBTR_GRP_CD,
                            SAP_SOLDTO_CODE
                        FROM EDW_vw_ph_DSTRBTR_CUSTOMER_DIM
                        WHERE CNTRY_CD = 'PH'
                    ) VEODCD,
                    (
                        SELECT IMPLP.ITEM_CD,
                            CASE
                                WHEN EPP.STATUS = '**'
                                AND (
                                    VEOTD.MNTH_ID BETWEEN EPP.LAUNCH_PERIOD AND EPP.END_PERIOD
                                ) THEN 'Y'
                                ELSE 'N'
                            END AS IS_NPI,
                            CAST(EPP.LAUNCH_PERIOD AS VARCHAR(52)) AS NPI_STR_PERIOD,
                            CAST(EPP.END_PERIOD AS VARCHAR(52)) AS NPI_END_PERIOD,
                            CASE
                                WHEN UPPER(IMPLP.PROMO_REG_IND) = 'REG' THEN 'Y'
                                ELSE 'N'
                            END AS IS_REG,
                            CASE
                                WHEN UPPER(IMPLP.PROMO_REG_IND) = 'PROMO' THEN 'Y'
                                ELSE 'N'
                            END AS IS_PROMO,
                            CAST(IMPLP.PROMO_STRT_PERIOD AS VARCHAR(10)) AS PROMO_STRT_PERIOD,
                            CAST(IMPLP.PROMO_END_PERIOD AS VARCHAR(10)) AS PROMO_END_PERIOD,
                            NULL AS IS_MCL,
                            CASE
                                WHEN UPPER(IMPLP.HERO_SKU_IND) = 'Y' THEN 'HERO'
                                ELSE 'NA'
                            END AS IS_HERO
                        FROM
                            (
                                SELECT *
                                FROM ITG_MDS_PH_LAV_PRODUCT
                                WHERE ACTIVE = 'Y'
                            ) AS IMPLP,
                            (
                                SELECT MNTH_ID FROM edw_vw_os_time_dim
                                WHERE CAL_DATE = to_date(current_timestamp)
                            ) VEOTD,
                            (
                                SELECT STATUS,
                                    ITEM_CD,
                                    MIN(JJ_MNTH_ID) AS LAUNCH_PERIOD,
                                    MIN(left(replace(add_months(to_date(JJ_MNTH_ID||'01','yyyymmdd'),11),'-',''),6)) AS END_PERIOD
                                FROM ITG_MDS_PH_POS_PRICELIST
                                WHERE STATUS = '**'
                                    AND ACTIVE = 'Y'
                                GROUP BY STATUS,
                                    ITEM_CD
                            ) EPP
                        WHERE LTRIM(EPP.ITEM_CD(+), '0') = LTRIM(IMPLP.ITEM_CD, '0')
                    ) VEODMD
                WHERE trim(VEODCD.DSTRBTR_GRP_CD(+)) = trim(A.DSTRBTR_GRP_CD) --AND   VEODMD.DSTRBTR_GRP_CD(+) = A.DSTRBTR_GRP_CD
                    AND trim(LTRIM(VEODMD.ITEM_CD(+))) = trim(LTRIM(A.DSTRBTR_MATL_NUM, '0'))
            ) VOSINV1,
            (
                SELECT MAT.*,
                    PROD.pka_productkey
                FROM EDW_vw_ph_MATERIAL_DIM MAT
                    LEFT JOIN EDW_PRODUCT_KEY_ATTRIBUTES PROD ON LTRIM(MAT.sap_matl_num, '0') = LTRIM(prod.matl_num, '0')
                WHERE MAT.CNTRY_KEY = 'PH'
                    AND UPPER(prod.ctry_nm) = 'PHILIPPINES'
            ) VEOMD
        WHERE trim(LTRIM(VEOMD.SAP_MATL_NUM(+), '0')) = trim(LTRIM(VOSINV1.DSTRBTR_MATL_NUM, '0'))
            AND LTRIM(nvl(trim(VOSINV1.DSTRBTR_MATL_NUM),'NA'), '0') NOT IN
            (
                SELECT DISTINCT LTRIM(NVL(trim(DSTRBTR_MATL_NUM), 'NA'), '0')
                FROM DSTRBTR_MATL
            )
    ) AS VOSINV,
    EDW_MV_PH_CUSTOMER_DIM EOCD,
    (
        SELECT *
        FROM EDW_vw_ph_CUSTOMER_DIM
        WHERE SAP_CNTRY_CD = 'PH'
    ) VEOCD
    WHERE TRIM(EOCD.CUST_ID(+)) = TRIM(VOSINV.SAP_SOLDTO_CODE)
    AND trim(LTRIM(VEOCD.SAP_CUST_ID(+), '0')) = trim(LTRIM(VOSINV.SAP_SOLDTO_CODE, '0'))
),

set_3 as
(
    SELECT
        VEOSSF.JJ_YEAR AS JJ_YEAR,
        VEOSSF.JJ_QTR AS JJ_QRTR,
        VEOSSF.JJ_MNTH_ID AS JJ_MNTH_ID,
        VEOSSF.JJ_MNTH_NO AS JJ_MNTH_NO,
        VEOSSF.JJ_MNTH_WK_NO AS JJ_MNTH_WK_NO,
        NULL AS WAREHSE_CD,
        VEOCD.SAP_CNTRY_NM AS CNTRY_NM,
        VEOSSF.DSTRBTR_GRP_CD AS DSTRBTR_GRP_CD,
        --  VEOSSF.CUST_ID AS SAP_SOLDTO_CODE,
        --  EOCD.CUST_NM AS SAP_SOLDTO_NM,
        EOCD.REGION,
        EOCD.CHANNEL_CD AS SI_CHNL_CD,
        EOCD.CHANNEL_DESC AS SI_CHNL_DESC,
        EOCD.SUB_CHNL_CD AS SI_SUB_CHNL_CD,
        EOCD.SUB_CHNL_DESC AS SI_SUB_CHNL_DESC,
        EOCD.PARENT_CUST_CD AS PARENT_CUSTOMER_CD,
        EOCD.PARENT_CUST_NM AS PARENT_CUSTOMER,
        EOCD.RPT_GRP_6_DESC AS ACCOUNT_GRP,
        'GENERAL TRADE' AS TRADE_TYPE,
        EOCD.RPT_GRP_2_DESC AS SLS_GRP_DESC,
        VEOCD.SAP_STATE_CD,
        VEOCD.SAP_SLS_ORG,
        VEOCD.SAP_CMP_ID,
        VEOCD.SAP_CNTRY_CD,
        VEOCD.SAP_CNTRY_NM,
        VEOCD.SAP_ADDR,
        VEOCD.SAP_REGION,
        VEOCD.SAP_CITY,
        VEOCD.SAP_POST_CD,
        VEOCD.SAP_CHNL_CD,
        VEOCD.SAP_CHNL_DESC,
        VEOCD.SAP_SLS_OFFICE_CD,
        VEOCD.SAP_SLS_OFFICE_DESC,
        VEOCD.SAP_SLS_GRP_CD,
        VEOCD.SAP_SLS_GRP_DESC,
        VEOCD.SAP_CURR_CD,
        VEOCD.GCH_REGION,
        VEOCD.GCH_CLUSTER,
        VEOCD.GCH_SUBCLUSTER,
        VEOCD.GCH_MARKET,
        VEOCD.GCH_RETAIL_BANNER,
        --   VOSINV.DSTRBTR_MATL_NUM AS DSTRBTR_MATL_NUM,
        LTRIM(VEOMD.SAP_MATL_NUM, '0') AS SKU,
        VEOMD.SAP_MAT_DESC AS SKU_DESC,
        VEOMD.SAP_MAT_TYPE_CD,
        VEOMD.SAP_MAT_TYPE_DESC,
        VEOMD.SAP_BASE_UOM_CD,
        VEOMD.SAP_PRCHSE_UOM_CD,
        VEOMD.SAP_PROD_SGMT_CD,
        VEOMD.SAP_PROD_SGMT_DESC,
        VEOMD.SAP_BASE_PROD_CD,
        VEOMD.SAP_BASE_PROD_DESC,
        VEOMD.SAP_MEGA_BRND_CD,
        VEOMD.SAP_MEGA_BRND_DESC,
        VEOMD.SAP_BRND_CD,
        VEOMD.SAP_BRND_DESC,
        VEOMD.SAP_VRNT_CD,
        VEOMD.SAP_VRNT_DESC,
        VEOMD.SAP_PUT_UP_CD,
        VEOMD.SAP_PUT_UP_DESC,
        VEOMD.SAP_GRP_FRNCHSE_CD,
        VEOMD.SAP_GRP_FRNCHSE_DESC,
        VEOMD.SAP_FRNCHSE_CD,
        VEOMD.SAP_FRNCHSE_DESC,
        VEOMD.SAP_PROD_FRNCHSE_CD,
        VEOMD.SAP_PROD_FRNCHSE_DESC,
        VEOMD.SAP_PROD_MJR_CD,
        VEOMD.SAP_PROD_MJR_DESC,
        VEOMD.SAP_PROD_MNR_CD,
        VEOMD.SAP_PROD_MNR_DESC,
        VEOMD.SAP_PROD_HIER_CD,
        VEOMD.SAP_PROD_HIER_DESC,
        VEOMD.GPH_REGION AS GLOBAL_MAT_REGION,
        VEOMD.GPH_PROD_FRNCHSE AS GLOBAL_PROD_FRANCHISE,
        VEOMD.GPH_PROD_BRND AS GLOBAL_PROD_BRAND,
        VEOMD.GPH_PROD_VRNT AS GLOBAL_PROD_VARIANT,
        VEOMD.GPH_PROD_PUT_UP_CD AS GLOBAL_PROD_PUT_UP_CD,
        VEOMD.GPH_PROD_PUT_UP_DESC AS GLOBAL_PUT_UP_DESC,
        VEOMD.GPH_PROD_SUB_BRND AS GLOBAL_PROD_SUB_BRAND,
        VEOMD.GPH_PROD_NEEDSTATE AS GLOBAL_PROD_NEED_STATE,
        VEOMD.GPH_PROD_CTGRY AS GLOBAL_PROD_CATEGORY,
        VEOMD.GPH_PROD_SUBCTGRY AS GLOBAL_PROD_SUBCATEGORY,
        VEOMD.GPH_PROD_SGMNT AS GLOBAL_PROD_SEGMENT,
        VEOMD.GPH_PROD_SUBSGMNT AS GLOBAL_PROD_SUBSEGMENT,
        VEOMD.GPH_PROD_SIZE AS GLOBAL_PROD_SIZE,
        VEOMD.GPH_PROD_SIZE_UOM AS GLOBAL_PROD_SIZE_UOM,
        NULL AS SO_BILL_DOC,
        0 AS SO_SLS_QTY,
        0 AS SO_RET_QTY,
        0 AS SO_SLS_QTY_PC,
        0 AS SO_RET_QTY_PC,
        0 AS SO_GRS_TRD_SLS,
        0 AS SO_RET_VAL,
        0 AS SO_TRD_DISCNT,
        0 AS SO_TRD_SLS,
        0 AS SO_NET_TRD_SLS,
        0 AS SO_JJ_GRS_TRD_SLS,
        0 AS SO_JJ_RET_VAL,
        0 AS SO_JJ_TRD_SLS,
        0 AS SO_JJ_NET_TRD_SLS,
        NULL AS IS_NPI,
        NULL AS NPI_STR_PERIOD,
        NULL AS NPI_END_PERIOD,
        NULL AS IS_REG,
        NULL AS IS_PROMO,
        NULL AS PROMO_STRT_PERIOD,
        NULL AS PROMO_END_PERIOD,
        NULL AS IS_MCL,
        NULL AS IS_HERO,
        SUM(EPP.LST_PRICE_UNIT) AS LOCAL_MAT_LISTPRICEUNIT,
        SUM(VEOSSF.SLS_QTY) AS SI_SLS_QTY,
        SUM(VEOSSF.RET_QTY) AS SI_RET_QTY,
        SUM(VEOSSF.SLS_LESS_RTN_QTY) AS SI_SLS_LESS_RTN_QTY,
        SUM(VEOSSF.GTS_VAL) AS SI_GTS_VAL,
        SUM(VEOSSF.RET_VAL) AS SI_RET_VAL,
        SUM(VEOSSF.GTS_LESS_RTN_VAL) AS SI_GTS_LESS_RTN_VAL,
        SUM(NTS_QTY) AS SI_NTS_QTY,
        SUM(VEOSSF.NTS_VAL) AS SI_NTS_VAL,
        SUM(VEOSSF.TP_VAL) AS SI_TP_VAL,
        0 AS END_STOCK_QTY,
        0 AS END_STOCK_VAL,
        VEOMD.PKA_PRODUCTKEY
    FROM
        (
            SELECT
                VEOTD.MNTH_ID AS JJ_MNTH_ID,
                VEOTD.MNTH_NO AS JJ_MNTH_NO,
                VEOTD.YEAR AS JJ_YEAR,
                VEOTD.QRTR AS JJ_QTR,
                VEOTD.MNTH_WK_NO AS JJ_MNTH_WK_NO,
                EOCD1.DSTRBTR_GRP_CD,
                LTRIM(VEOSSF1.CUST_ID, '0') AS CUST_ID,
                ITEM_CD,
                SUM(SLS_QTY) AS SLS_QTY,
                SUM(RET_QTY) AS RET_QTY,
                SUM(SLS_LESS_RTN_QTY) AS SLS_LESS_RTN_QTY,
                SUM(GTS_VAL) AS GTS_VAL,
                SUM(RET_VAL) AS RET_VAL,
                SUM(GTS_LESS_RTN_VAL) AS GTS_LESS_RTN_VAL,
                SUM(NTS_QTY) AS NTS_QTY,
                SUM(NTS_VAL) AS NTS_VAL,
                SUM(TP_VAL) AS TP_VAL
            FROM EDW_vw_ph_SELLIN_SALES_FACT VEOSSF1,
            (
                SELECT DISTINCT CUST_ID,
                    DSTRBTR_GRP_CD
                FROM EDW_MV_PH_CUSTOMER_DIM
            ) EOCD1,
            (
                SELECT YEAR,
                    QRTR_NO,
                    QRTR,
                    MNTH_ID,
                    MNTH_DESC,
                    MNTH_NO,
                    MNTH_SHRT,
                    MNTH_LONG,
                    WK,
                    MNTH_WK_NO,
                    CASE
                        WHEN MNTH_WK_NO = 1 THEN '19000101'
                        ELSE FRST_DAY
                    END AS FIRST_DAY,
                    CASE
                        WHEN MNTH_WK_NO = LST_WK THEN '20991231'
                        ELSE LST_DAY
                    END AS LAST_DAY
                FROM
                    (
                        SELECT
                            DISTINCT "year" as year,
                            QRTR_NO,
                            QRTR,
                            MNTH_ID,
                            MNTH_DESC,
                            MNTH_NO,
                            MNTH_SHRT,
                            MNTH_LONG,
                            WK,
                            MNTH_WK_NO,
                            MIN(CAL_DATE_ID) AS FRST_DAY,
                            MAX(CAL_DATE_ID) AS LST_DAY,
                            MAX(MNTH_WK_NO) OVER (PARTITION BY MNTH_ID) AS LST_WK
                        FROM edw_vw_os_time_dim T
                        GROUP BY "year",
                            QRTR_NO,
                            QRTR,
                            MNTH_ID,
                            MNTH_DESC,
                            MNTH_NO,
                            MNTH_SHRT,
                            MNTH_LONG,
                            WK,
                            MNTH_WK_NO
                    ) A
            ) VEOTD
            WHERE
                (
                    SLS_QTY <> 0
                    OR RET_QTY <> 0
                    OR GTS_VAL <> 0
                    OR RET_VAL <> 0
                    OR NTS_VAL <> 0
                    OR TP_VAL <> 0
                )
                AND VEOSSF1.CNTRY_NM = 'PH'
                AND TRIM(VEOSSF1.JJ_MNTH_ID) = CAST((VEOTD.MNTH_ID) AS VARCHAR)
                AND VEOSSF1.PSTNG_DT BETWEEN VEOTD.FIRST_DAY AND VEOTD.LAST_DAY
                AND UPPER(TRIM(EOCD1.CUST_ID(+))) = trim(UPPER(LTRIM(VEOSSF1.CUST_ID, '0')))
            GROUP BY VEOTD.MNTH_ID,
                VEOTD.MNTH_NO,
                VEOTD.YEAR,
                VEOTD.QRTR,
                VEOTD.MNTH_WK_NO,
                EOCD1.DSTRBTR_GRP_CD,
                VEOSSF1.CUST_ID,
                ITEM_CD
        ) VEOSSF,
        (
            SELECT *
            FROM EDW_MV_PH_CUSTOMER_DIM
        ) EOCD,
        (
            SELECT *
            FROM ITG_MDS_PH_POS_PRICELIST
            WHERE ACTIVE = 'Y'
        ) AS EPP,
        (
            SELECT MAT.*,
                PROD.PKA_PRODUCTKEY
            FROM EDW_vw_ph_MATERIAL_DIM MAT
                LEFT JOIN EDW_PRODUCT_KEY_ATTRIBUTES PROD ON  trim(LTRIM(MAT.sap_matl_num, '0')) = trim(ltrim(prod.matl_num, '0'))
            WHERE CNTRY_KEY = 'PH'
                AND upper(prod.ctry_nm) = 'PHILIPPINES'
        ) VEOMD,
        (
            SELECT *
            FROM EDW_vw_ph_CUSTOMER_DIM
            WHERE SAP_CNTRY_CD = 'PH'
        ) VEOCD
    WHERE UPPER(TRIM(EOCD.CUST_ID(+))) = trim(UPPER(LTRIM(VEOSSF.CUST_ID, '0')))
        AND trim(UPPER(LTRIM(VEOMD.SAP_MATL_NUM(+), '0'))) = trim(UPPER(LTRIM(VEOSSF.ITEM_CD, '0')))
        AND trim(UPPER(LTRIM(VEOCD.SAP_CUST_ID(+), '0'))) = trim(UPPER(LTRIM(VEOSSF.CUST_ID, '0')))
        AND trim(UPPER(TRIM(EPP.ITEM_CD(+)))) = UPPER(TRIM(VEOSSF.ITEM_CD))
        AND TRIM(EPP.JJ_MNTH_ID(+)) = TRIM(VEOSSF.JJ_MNTH_ID)
        AND TRIM(UPPER(EOCD.RPT_GRP_1_DESC)) = 'GENERAL TRADE'
    GROUP BY
        VEOSSF.JJ_YEAR,
        VEOSSF.JJ_QTR,
        VEOSSF.JJ_MNTH_ID,
        VEOSSF.JJ_MNTH_NO,
        VEOSSF.JJ_MNTH_WK_NO,
        VEOCD.SAP_CNTRY_NM,
        VEOSSF.DSTRBTR_GRP_CD,
        EOCD.REGION,
        EOCD.CHANNEL_CD,
        EOCD.CHANNEL_DESC,
        EOCD.SUB_CHNL_CD,
        EOCD.SUB_CHNL_DESC,
        EOCD.PARENT_CUST_CD,
        EOCD.PARENT_CUST_NM,
        EOCD.RPT_GRP_6_DESC,
        EOCD.RPT_GRP_2_DESC,
        VEOCD.SAP_STATE_CD,
        VEOCD.SAP_SLS_ORG,
        VEOCD.SAP_CMP_ID,
        VEOCD.SAP_CNTRY_CD,
        VEOCD.SAP_CNTRY_NM,
        VEOCD.SAP_ADDR,
        VEOCD.SAP_REGION,
        VEOCD.SAP_CITY,
        VEOCD.SAP_POST_CD,
        VEOCD.SAP_CHNL_CD,
        VEOCD.SAP_CHNL_DESC,
        VEOCD.SAP_SLS_OFFICE_CD,
        VEOCD.SAP_SLS_OFFICE_DESC,
        VEOCD.SAP_SLS_GRP_CD,
        VEOCD.SAP_SLS_GRP_DESC,
        VEOCD.SAP_CURR_CD,
        VEOCD.GCH_REGION,
        VEOCD.GCH_CLUSTER,
        VEOCD.GCH_SUBCLUSTER,
        VEOCD.GCH_MARKET,
        VEOCD.GCH_RETAIL_BANNER,
        VEOMD.SAP_MATL_NUM,
        VEOMD.SAP_MAT_DESC,
        VEOMD.SAP_MAT_TYPE_CD,
        VEOMD.SAP_MAT_TYPE_DESC,
        VEOMD.SAP_BASE_UOM_CD,
        VEOMD.SAP_PRCHSE_UOM_CD,
        VEOMD.SAP_PROD_SGMT_CD,
        VEOMD.SAP_PROD_SGMT_DESC,
        VEOMD.SAP_BASE_PROD_CD,
        VEOMD.SAP_BASE_PROD_DESC,
        VEOMD.SAP_MEGA_BRND_CD,
        VEOMD.SAP_MEGA_BRND_DESC,
        VEOMD.SAP_BRND_CD,
        VEOMD.SAP_BRND_DESC,
        VEOMD.SAP_VRNT_CD,
        VEOMD.SAP_VRNT_DESC,
        VEOMD.SAP_PUT_UP_CD,
        VEOMD.SAP_PUT_UP_DESC,
        VEOMD.SAP_GRP_FRNCHSE_CD,
        VEOMD.SAP_GRP_FRNCHSE_DESC,
        VEOMD.SAP_FRNCHSE_CD,
        VEOMD.SAP_FRNCHSE_DESC,
        VEOMD.SAP_PROD_FRNCHSE_CD,
        VEOMD.SAP_PROD_FRNCHSE_DESC,
        VEOMD.SAP_PROD_MJR_CD,
        VEOMD.SAP_PROD_MJR_DESC,
        VEOMD.SAP_PROD_MNR_CD,
        VEOMD.SAP_PROD_MNR_DESC,
        VEOMD.SAP_PROD_HIER_CD,
        VEOMD.SAP_PROD_HIER_DESC,
        VEOMD.GPH_REGION,
        VEOMD.GPH_PROD_FRNCHSE,
        VEOMD.GPH_PROD_BRND,
        VEOMD.GPH_PROD_VRNT,
        VEOMD.GPH_PROD_PUT_UP_CD,
        VEOMD.GPH_PROD_PUT_UP_DESC,
        VEOMD.GPH_PROD_SUB_BRND,
        VEOMD.GPH_PROD_NEEDSTATE,
        VEOMD.GPH_PROD_CTGRY,
        VEOMD.GPH_PROD_SUBCTGRY,
        VEOMD.GPH_PROD_SGMNT,
        VEOMD.GPH_PROD_SUBSGMNT,
        VEOMD.GPH_PROD_SIZE,
        VEOMD.GPH_PROD_SIZE_UOM,
        VEOMD.PKA_PRODUCTKEY
),
transformed as
(
    select * from set_1
    union all
    select * from set_2
    union all
    select * from set_3
),
final as
(
    select
        jj_year,
        jj_qrtr,
        jj_mnth_id,
        jj_mnth_no,
        jj_mnth_wk_no,
        warehse_cd,
        cntry_nm,
        dstrbtr_grp_cd,
        region,
        si_chnl_cd,
        si_chnl_desc,
        si_sub_chnl_cd,
        si_sub_chnl_desc,
        parent_customer_cd,
        parent_customer,
        account_grp,
        trade_type,
        sls_grp_desc,
        sap_state_cd,
        sap_sls_org,
        sap_cmp_id,
        sap_cntry_cd,
        sap_cntry_nm,
        sap_addr,
        sap_region,
        sap_city,
        sap_post_cd,
        sap_chnl_cd,
        sap_chnl_desc,
        sap_sls_office_cd,
        sap_sls_office_desc,
        sap_sls_grp_cd,
        sap_sls_grp_desc,
        sap_curr_cd,
        gch_region,
        gch_cluster,
        gch_subcluster,
        gch_market,
        gch_retail_banner,
        sku,
        sku_desc,
        sap_mat_type_cd,
        sap_mat_type_desc,
        sap_base_uom_cd,
        sap_prchse_uom_cd,
        sap_prod_sgmt_cd,
        sap_prod_sgmt_desc,
        sap_base_prod_cd,
        sap_base_prod_desc,
        sap_mega_brnd_cd,
        sap_mega_brnd_desc,
        sap_brnd_cd,
        sap_brnd_desc,
        sap_vrnt_cd,
        sap_vrnt_desc,
        sap_put_up_cd,
        sap_put_up_desc,
        sap_grp_frnchse_cd,
        sap_grp_frnchse_desc,
        sap_frnchse_cd,
        sap_frnchse_desc,
        sap_prod_frnchse_cd,
        sap_prod_frnchse_desc,
        sap_prod_mjr_cd,
        sap_prod_mjr_desc,
        sap_prod_mnr_cd,
        sap_prod_mnr_desc,
        sap_prod_hier_cd,
        sap_prod_hier_desc,
        global_mat_region,
        global_prod_franchise,
        global_prod_brand,
        global_prod_variant,
        global_prod_put_up_cd,
        global_put_up_desc,
        global_prod_sub_brand,
        global_prod_need_state,
        global_prod_category,
        global_prod_subcategory,
        global_prod_segment,
        global_prod_subsegment,
        global_prod_size,
        global_prod_size_uom,
        so_bill_doc,
        so_sls_qty,
        so_ret_qty,
        so_sls_qty_pc,
        so_ret_qty_pc,
        so_grs_trd_sls,
        so_ret_val,
        so_trd_discnt,
        so_trd_sls,
        so_net_trd_sls,
        so_jj_grs_trd_sls,
        so_jj_ret_val,
        so_jj_trd_sls,
        so_jj_net_trd_sls,
        is_npi,
        npi_str_period,
        npi_end_period,
        is_reg,
        is_promo,
        promo_strt_period,
        promo_end_period,
        is_mcl,
        is_hero,
        local_mat_listpriceunit,
        si_sls_qty,
        si_ret_qty,
        si_sls_less_rtn_qty,
        si_gts_val,
        si_ret_val,
        si_gts_less_rtn_val,
        si_nts_qty,
        si_nts_val,
        si_tp_val,
        end_stock_qty,
        end_stock_val,
        pka_productkey
    from transformed
)
select * from final