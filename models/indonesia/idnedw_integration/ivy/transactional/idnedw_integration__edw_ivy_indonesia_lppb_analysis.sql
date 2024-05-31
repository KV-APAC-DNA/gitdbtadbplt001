with edw_ivy_all_distributor_lppb_fact as
(
    select * from {{ ref('idnedw_integration__edw_ivy_all_distributor_lppb_fact') }}
),
edw_distributor_dim as
(
    select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
edw_product_dim as
(
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
edw_time_dim as
(
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
itg_target_bp_s_op as
(
    select * from {{ ref('idnitg_integration__itg_target_bp_s_op') }}
),
itg_target_dist_brand_channel as
(
    select * from {{ ref('idnitg_integration__itg_target_dist_brand_channel') }}
),
final as
(
    SELECT
        ETD.JJ_YEAR,
        ETD.JJ_QRTR,
        ETD.JJ_MNTH_ID,
        ETD.JJ_MNTH,
        ETD.JJ_MNTH_DESC,
        ETD.JJ_MNTH_NO,
        TRIM(UPPER(EDD.DSTRBTR_GRP_CD)),
        EDD.DSTRBTR_GRP_NM,
        TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID)),
        EDD.JJ_SAP_DSTRBTR_NM,
        EDD.JJ_SAP_DSTRBTR_NM || ' ^' || EDD.JJ_SAP_DSTRBTR_ID AS DSTRBTR_CD_NM,
        EDD.AREA,
        EDD.REGION,
        EDD.BDM_NM,
        EDD.RBM_NM,
        EDD.STATUS AS DSTRBTR_STATUS,
        TRIM(UPPER(EPD.JJ_SAP_PROD_ID)),
        EPD.JJ_SAP_PROD_DESC,
        EPD.JJ_SAP_UPGRD_PROD_ID,
        EPD.JJ_SAP_UPGRD_PROD_DESC,
        EPD.JJ_SAP_CD_MP_PROD_ID,
        EPD.JJ_SAP_CD_MP_PROD_DESC,
        EPD.JJ_SAP_UPGRD_PROD_DESC || ' ^' || EPD.JJ_SAP_UPGRD_PROD_ID AS SAP_PROD_CODE_NAME,
        EPD.FRANCHISE,
        EPD.BRAND,
        EPD.VARIANT1 AS SKU_GRP_OR_VARIANT,
        EPD.VARIANT2 AS SKU_GRP1_OR_VARIANT1,
        EPD.VARIANT3 AS SKU_GRP2_OR_VARIANT2,
        EPD.VARIANT3 || ' ' || NVL(CAST(EPD.PUT_UP AS VARCHAR),'') AS SKU_GRP3_OR_VARIANT3,
        EPD.STATUS AS PROD_STATUS,
        EADLF.STRT_INV_QTY,
        EADLF.SELLIN_QTY,
        EADLF.SELLOUT_QTY,
        EADLF.END_INV_QTY,
        EADLF.STRT_INV_VAL,
        EADLF.SELLIN_VAL,
        EADLF.SELLOUT_VAL,
        EADLF.END_INV_VAL,
        EADLF.SELLOUT_LAST_TWO_MNTHS_QTY,
        EADLF.SELLOUT_LAST_TWO_MNTHS_VAL,
        NULL as variant,
        ETD.JJ_MNTH_LONG AS jj_mnth_long,
        0 as bp_qtn,
        0 AS bp_val,
        0 as s_op_qtn,
        0 AS s_op_val,
        0 as TRGT_HNA,
        0 as TRGT_NIV,
        NULL AS TRGT_BP_S_OP_FLAG,
        NULL AS TRGT_DIST_BRND_CHNL_FLAG
        FROM
        (SELECT *
        FROM EDW_IVY_ALL_DISTRIBUTOR_LPPB_FACT
        WHERE JJ_SAP_DSTRBTR_ID NOT IN (SELECT DISTINCT JJ_SAP_DSTRBTR_ID
                                        FROM EDW_IVY_ALL_DISTRIBUTOR_LPPB_FACT
                                        WHERE JJ_SAP_DSTRBTR_ID IN ('131677','123881','123877','123878','123879','123880')
                                        AND   JJ_MNTH_ID = '201711')) AS EADLF,
        EDW_DISTRIBUTOR_DIM AS EDD,
        EDW_PRODUCT_DIM AS EPD,
        (
        SELECT DISTINCT
            JJ_YEAR,
            JJ_QRTR_NO,
            JJ_QRTR,
            JJ_MNTH_ID,
            JJ_MNTH,
            JJ_MNTH_DESC,
            JJ_MNTH_NO,
            JJ_MNTH_SHRT,
            JJ_MNTH_LONG
        FROM EDW_TIME_DIM
        ) AS ETD
        WHERE
        TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+)))=TRIM(UPPER(EADLF.JJ_SAP_DSTRBTR_ID)) AND
        ETD.JJ_MNTH_ID=EADLF.JJ_MNTH_ID AND
        EADLF.JJ_MNTH_ID between EDD.effective_from(+) and EDD.effective_to(+) and 
        TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+)))=TRIM(UPPER(EADLF.JJ_SAP_PROD_ID)) AND
        EADLF.JJ_MNTH_ID between EPD.effective_from(+) and EPD.effective_to(+) and
        TRIM(UPPER(EADLF.jj_sap_prod_id))!='REBATE' AND
        TRIM(UPPER(EADLF.JJ_SAP_DSTRBTR_ID))!='119683' AND
        TRIM(UPPER(EDD.DSTRBTR_GRP_CD))!='SPLD'
        AND   TRIM(UPPER(EDD.DSTRBTR_GRP_CD))!='JYM' 
        AND   TRIM(UPPER(EADLF.JJ_SAP_PROD_ID))!='DAOG20'
        UNION ALL
        SELECT CAST(YEAR AS INTEGER) AS JJ_YEAR,
            ETD.JJ_QRTR AS JJ_QRTR,
            ETD.jj_mnth_id AS jj_mnth_id,
            ETD.jj_mnth AS jj_mnth,
            ETD.jj_mnth_desc AS jj_mnth_desc,
            ETD.jj_mnth_no AS jj_mnth_no,
            NULL AS dstrbtr_grp_cd,
            NULL AS dstrbtr_grp_nm,
            NULL AS jj_sap_dstrbtr_id,
            NULL AS jj_sap_dstrbtr_nm,
            NULL AS dstrbtr_cd_nm,
            NULL AS area,
            NULL AS region,
            NULL AS bdm_mn,
            NULL AS rbm_nm,
            NULL AS DSTRBTR_STATUS,
            NULL AS jj_sap_prod_id,
            NULL AS jj_sap_prod_desc,
            NULL AS jj_sap_upgrd_prod_id,
            NULL AS jj_sap_upgrd_prod_desc,
            NULL AS jj_sap_cd_mp_prod_id,
            NULL AS jj_sap_cd_mp_prod_desc,
            NULL AS sap_prod_code_name,
            ITBSP.franchise AS franchise,
            ITBSP.brand AS brand,
            NULL AS sku_grp_or_variant,
            NULL AS sku_grp1_or_variant1,
            NULL AS sku_grp2_or_variant2,
            NULL AS sku_grp3_or_variant3,
            NULL AS prod_status,
            0 AS strt_inv_qty,
            0 AS sellin_qty,
            0 AS sellout_qty,
            0 AS end_inv_qty,
            0 AS strt_inv_val,
            0 AS sellin_val,
            0 AS sellout_val,
            0 AS end_inv_val,
            0 AS sellout_last_two_mnths_qty,
            0 AS sellout_last_two_mnths_val,
            variant as variant,
            ITBSP.jj_mnth_long AS jj_mnth_long,
            bp_qtn as bp_qtn,
            bp_val AS bp_val,
            s_op_qtn as s_op_qtn,
            s_op_val AS s_op_val,
            0 as TRGT_HNA,
            0 as TRGT_NIV,
            'Y' AS TRGT_BP_S_OP_FLAG,
            NULL AS TRGT_DIST_BRND_CHNL_FLAG
        FROM itg_target_bp_s_op ITBSP,
        (
        SELECT DISTINCT
            JJ_YEAR,
            JJ_QRTR_NO,
            JJ_QRTR,
            JJ_MNTH_ID,
            JJ_MNTH,
            JJ_MNTH_DESC,
            JJ_MNTH_NO,
            JJ_MNTH_SHRT,
            JJ_MNTH_LONG
        FROM EDW_TIME_DIM
        ) AS ETD,
        (SELECT DISTINCT brand,franchise
        ,effective_from,effective_to
                                        FROM edw_product_dim) EPD
        WHERE TRIM(UPPER(EPD.brand(+))) = TRIM(UPPER(ITBSP.brand)) AND
            TRIM(UPPER(EPD.franchise(+)))=TRIM(UPPER(ITBSP.franchise)) and
            concat(ITBSP.year,decode(ITBSP.jj_mnth_long,'January','01','February','02','March','03','April','04','May','05','June','06','July','07','August','08','September','09','October','10','November','11','December','12','00')) between EPD.effective_from(+) and effective_to(+) and 
            ETD.jj_mnth_long(+) = ITBSP.jj_mnth_long and
                ETD.jj_year(+) = ITBSP.year
            UNION ALL	  
            SELECT CAST(YEAR AS INTEGER) AS JJ_YEAR,
            ITDBC.JJ_QRTR AS JJ_QRTR,
            ITDBC.jj_mnth_id AS jj_mnth_id,
            ITDBC.jj_mnth AS jj_mnth,
            ITDBC.jj_mnth_desc AS jj_mnth_desc,
            ITDBC.jj_mnth_no AS jj_mnth_no,
            NULL AS dstrbtr_grp_cd,
            NULL AS dstrbtr_grp_nm,
            TRIM(UPPER(ITDBC.jj_sap_dstrbtr_id)) AS jj_sap_dstrbtr_id,
            ITDBC.jj_sap_dstrbtr_nm AS jj_sap_dstrbtr_nm,
            EDD.JJ_SAP_DSTRBTR_NM || ' ^' || EDD.JJ_SAP_DSTRBTR_ID AS dstrbtr_cd_nm,
            EDD.area AS area,
            EDD.region as region,
            EDD.bdm_nm AS bdm_nm,
            EDD.rbm_nm AS rbm_nm,
            EDD.status AS DSTRBTR_STATUS,
            NULL AS jj_sap_prod_id,
            NULL AS jj_sap_prod_desc,
            NULL AS jj_sap_upgrd_prod_id,
            NULL AS jj_sap_upgrd_prod_desc,
            NULL AS jj_sap_cd_mp_prod_id,
            NULL AS jj_sap_cd_mp_prod_desc,
            NULL AS sap_prod_code_name,
            CASE
                WHEN ITDBC.Brand IS NOT NULL THEN EPD.Franchise
                ELSE ITDBC.Franchise
            END AS franchise,
            ITDBC.brand AS brand,
            NULL AS sku_grp_or_variant,
            NULL AS sku_grp1_or_variant1,
            NULL AS sku_grp2_or_variant2,
            NULL AS sku_grp3_or_variant3,
            NULL AS prod_status,
            0 AS strt_inv_qty,
            0 AS sellin_qty,
            0 AS sellout_qty,
            0 AS end_inv_qty,
            0 AS strt_inv_val,
            0 AS sellin_val,
            0 AS sellout_val,
            0 AS end_inv_val,
            0 AS sellout_last_two_mnths_qty,
            0 AS sellout_last_two_mnths_val,
                NULL as variant,
            ITDBC.jj_mnth_long AS jj_mnth_long,	   
                0 as bp_qtn,
                0 as bp_val,
                0 as s_op_qtn,
                0 as s_op_val,
                ITDBC.trgt_hna AS trgt_hna,
                ITDBC.trgt_niv AS trgt_niv,
                NULL AS TRGT_BP_S_OP_FLAG,
                'Y' AS TRGT_DIST_BRND_CHNL_FLAG
        FROM (select T1.*,ETD.JJ_QRTR,ETD.JJ_MNTH_ID,ETD.JJ_MNTH,ETD.JJ_MNTH_DESC,ETD.JJ_MNTH_NO from itg_target_dist_brand_channel T1 LEFT JOIN (SELECT DISTINCT JJ_YEAR,
                          
                          JJ_QRTR,
                          JJ_MNTH_ID,
                          JJ_MNTH,
                          JJ_MNTH_DESC,
                          JJ_MNTH_NO,
                          JJ_MNTH_LONG
                   FROM EDW_TIME_DIM) AS ETD
               ON T1.year = ETD.jj_year
              AND UPPER (TRIM (T1.jj_mnth_long)) = UPPER (TRIM (ETD.JJ_MNTH_LONG))
              ) ITDBC
              LEFT JOIN edw_distributor_dim EDD ON TRIM (UPPER (ITDBC.jj_sap_dstrbtr_id)) = TRIM (UPPER (EDD.jj_sap_dstrbtr_id))
		      and ITDBC.jj_mnth_id between EDD.effective_from and EDD.effective_to 
              LEFT JOIN (SELECT DISTINCT brand, franchise,effective_from,effective_to FROM edw_product_dim) EPD ON
		                CASE WHEN ITDBC.brand IS NOT NULL
              AND UPPER (TRIM (ITDBC.brand)) = UPPER (TRIM (EPD.brand)) and ITDBC.jj_mnth_id between EPD.effective_from and EPD.effective_to  THEN 1 END = 1
              
			  
)

select * from final