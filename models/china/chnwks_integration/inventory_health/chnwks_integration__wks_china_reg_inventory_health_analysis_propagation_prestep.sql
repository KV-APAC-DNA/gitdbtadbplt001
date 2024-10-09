with wks_china_siso_propagate_final as
(
    select * from {{ ref('chnwks_integration__wks_china_siso_propagate_final') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as 
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_material_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_customer_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as 
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_gch_customerhierarchy as 
(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_dstrbtn_chnl as 
(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as 
(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_company_dim as 
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_code_descriptions as 
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as 
(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
vw_edw_reg_exch_rate as
(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
TIME as 
(
    SELECT 
    DISTINCT "year" as CAL_YEAR, 
    QRTR_NO as cal_qrtr_no, 
    MNTH_ID as cal_mnth_id, 
    MNTH_NO as cal_mnth_no 
    FROM 
    EDW_VW_OS_TIME_DIM
), 
SISO AS 
(
    SELECT 
    country_name, 
    sap_prnt_cust_key as parent_code, 
    sap_bnr_key, 
    CAST('UNASSIGNED' AS VARCHAR) AS payer_code, 
    sold_to, 
    CAST('UNASSIGNED' AS VARCHAR) AS customer_code, 
    matl_num, 
    bu, 
    CAST('UNASSIGNED' AS VARCHAR) AS region, 
    CAST('UNASSIGNED' AS VARCHAR) AS zone_or_area, 
    LEFT (month, 4) AS year, 
    month, 
    sell_in_qty, 
    sell_in_value, 
    inv_qty, 
    inv_value, 
    so_qty, 
    so_value, 
    last_3months_so, 
    last_6months_so, 
    last_12months_so, 
    last_3months_so_value, 
    last_6months_so_value, 
    last_12months_so_value, 
    last_36months_so_value, 
    propagate_flag, 
    propagate_from, 
    reason, 
    replicated_flag, 
    existing_so_qty, 
    existing_so_value, 
    existing_inv_qty, 
    existing_inv_value, 
    existing_sell_in_qty, 
    existing_sell_in_value, 
    existing_last_3months_so, 
    existing_last_6months_so, 
    existing_last_12months_so, 
    existing_last_3months_so_value, 
    existing_last_6months_so_value, 
    existing_last_12months_so_value 
    FROM 
    wks_china_siso_propagate_final 
    where 
    left(month, 4)>= (DATE_PART(YEAR, current_timestamp()) -2 )
), 
CUSTOMER as 
(
    SELECT 
    DISTINCT SAP_CUST_ID, 
    SAP_PRNT_CUST_KEY, 
    SAP_PRNT_CUST_DESC, 
    SAP_CUST_CHNL_KEY, 
    SAP_CUST_CHNL_DESC, 
    SAP_CUST_SUB_CHNL_KEY, 
    SAP_SUB_CHNL_DESC, 
    SAP_GO_TO_MDL_KEY, 
    SAP_GO_TO_MDL_DESC, 
    SAP_BNR_KEY, 
    SAP_BNR_DESC, 
    SAP_BNR_FRMT_KEY, 
    SAP_BNR_FRMT_DESC, 
    retail_env, 
    GCH_REGION, 
    GCH_CLUSTER, 
    GCH_SUBCLUSTER, 
    GCH_MARKET, 
    GCH_RETAIL_BANNER 
    FROM 
    (
        SELECT 
        LTRIM(CBD.CUST_NUM, 0) AS SAP_CUST_ID, 
        CBD.CUST_NM AS SAP_CUST_NM, 
        CSD.SLS_ORG AS SAP_SLS_ORG, 
        CSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY, 
        cddes_pck.code_desc AS SAP_PRNT_CUST_DESC, 
        LTRIM(CD.COMPANY, 0) AS SAP_CMP_ID, 
        CD.CTRY_KEY AS SAP_CNTRY_CD, 
        CD.CTRY_NM AS SAP_CNTRY_NM, 
        CBD.ADDR AS SAP_ADDR, 
        CBD.RGN AS SAP_REGION, 
        CBD.DSTRC AS SAP_STATE_CD, 
        CBD.CITY AS SAP_CITY, 
        CBD.PSTL_CD AS SAP_POST_CD, 
        CSD.DSTR_CHNL AS SAP_CHNL_CD, 
        DC.TXTSH AS SAP_CHNL_DESC, 
        CSD.SLS_OFC AS SAP_SLS_OFFICE_CD, 
        CSD.SLS_OFC_DESC AS SAP_SLS_OFFICE_DESC, 
        CSD.SLS_GRP AS SAP_SLS_GRP_CD, 
        CSD.SLS_GRP_DESC AS SAP_SLS_GRP_DESC, 
        CSD.CRNCY_KEY AS SAP_CURR_CD, 
        CSD.CHNL_KEY AS SAP_CUST_CHNL_KEY, 
        cddes_chnl.code_desc AS SAP_CUST_CHNL_DESC, 
        CSD.SUB_CHNL_KEY AS SAP_CUST_SUB_CHNL_KEY, 
        cddes_subchnl.code_desc AS SAP_SUB_CHNL_DESC, 
        CSD.GO_TO_MDL_KEY AS SAP_GO_TO_MDL_KEY, 
        cddes_gtm.code_desc AS SAP_GO_TO_MDL_DESC, 
        CSD.BNR_KEY AS SAP_BNR_KEY, 
        cddes_bnrkey.code_desc AS SAP_BNR_DESC, 
        CSD.BNR_FRMT_KEY AS SAP_BNR_FRMT_KEY, 
        cddes_bnrfmt.code_desc AS SAP_BNR_FRMT_DESC, 
        subchnl_retail_env.retail_env, 
        GCH.GCGH_REGION AS GCH_REGION, 
        GCH.GCGH_CLUSTER AS GCH_CLUSTER, 
        GCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER, 
        GCH.GCGH_MARKET AS GCH_MARKET, 
        GCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER, 
        ROW_NUMBER() OVER (
            PARTITION BY LTRIM(csd.CUST_NUM, 0) 
            ORDER BY 
            sap_prnt_cust_desc ASC, 
            DECODE(
                CSD.CUST_DEL_FLAG, NULL, 'O', '', 'O', 
                CSD.CUST_DEL_FLAG
            ) ASC, 
            csd.sls_org, 
            csd.dstr_chnl
        ) AS rnk 
        FROM 
        EDW_CUSTOMER_SALES_DIM CSD 
        INNER JOIN EDW_CUSTOMER_BASE_DIM CBD ON LTRIM (CSD.CUST_NUM, 0) = LTRIM (CBD.CUST_NUM, 0) 
        LEFT OUTER JOIN EDW_GCH_CUSTOMERHIERARCHY GCH ON LTRIM (GCH.CUSTOMER, 0) = LTRIM (CBD.CUST_NUM, 0) 
        INNER JOIN EDW_DSTRBTN_CHNL DC ON LTRIM (CSD.DSTR_CHNL, 0) = LTRIM (DC.DISTR_CHAN, 0) 
        INNER JOIN EDW_SALES_ORG_DIM SOD ON TRIM (CSD.SLS_ORG) = TRIM (SOD.SLS_ORG) -- AND SOD.ctry_key = 'TH'
        AND SOD.SLS_ORG IN ('1500', '8888', '100A') 
        INNER JOIN EDW_COMPANY_DIM CD ON SOD.SLS_ORG_CO_CD = CD.CO_CD ---------- code BY SG TEAM
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_PCK ON TRIM (
            UPPER (CDDES_PCK.CODE_TYPE)
        ) = 'PARENT CUSTOMER KEY' 
        AND CDDES_PCK.CODE = CSD.PRNT_CUST_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_BNRKEY ON TRIM (
            UPPER (cddes_bnrkey.code_type)
        ) = 'BANNER KEY' 
        AND CDDES_BNRKEY.CODE = CSD.BNR_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_BNRFMT ON TRIM (
            UPPER (cddes_bnrfmt.code_type)
        ) = 'BANNER FORMAT KEY' 
        AND CDDES_BNRFMT.CODE = CSD.BNR_FRMT_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_CHNL ON TRIM (
            UPPER (cddes_chnl.code_type)
        ) = 'CHANNEL KEY' 
        AND CDDES_CHNL.CODE = CSD.CHNL_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_GTM ON TRIM (
            UPPER (cddes_gtm.code_type)
        ) = 'GO TO MODEL KEY' 
        AND CDDES_GTM.CODE = CSD.GO_TO_MDL_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_SUBCHNL ON TRIM (
            UPPER (
            cddes_subchnl.code_type
            )
        ) = 'SUB CHANNEL KEY' 
        AND CDDES_SUBCHNL.CODE = CSD.SUB_CHNL_KEY 
        LEFT JOIN EDW_SUBCHNL_RETAIL_ENV_MAPPING SUBCHNL_RETAIL_ENV ON UPPER (SUBCHNL_RETAIL_ENV.SUB_CHANNEL) = UPPER (CDDES_SUBCHNL.CODE_DESC)
    ) 
    WHERE 
    RNK = 1 
    and nvl(sap_prnt_cust_key, '#') not IN('PC0004', 'PC0013', 'PC0014') 
    UNION ALL 
    SELECT 
    DISTINCT sap_prnt_cust_key as SAP_CUST_ID, 
    SAP_PRNT_CUST_KEY, 
    SAP_PRNT_CUST_DESC, 
    SAP_CUST_CHNL_KEY, 
    SAP_CUST_CHNL_DESC, 
    SAP_CUST_SUB_CHNL_KEY, 
    SAP_SUB_CHNL_DESC, 
    SAP_GO_TO_MDL_KEY, 
    SAP_GO_TO_MDL_DESC, 
    SAP_BNR_KEY, 
    SAP_BNR_DESC, 
    SAP_BNR_FRMT_KEY, 
    SAP_BNR_FRMT_DESC, 
    retail_env, 
    GCH_REGION, 
    GCH_CLUSTER, 
    GCH_SUBCLUSTER, 
    GCH_MARKET, 
    GCH_RETAIL_BANNER 
    FROM 
    (
        SELECT 
        LTRIM(CBD.CUST_NUM, 0) AS SAP_CUST_ID, 
        CBD.CUST_NM AS SAP_CUST_NM, 
        CSD.SLS_ORG AS SAP_SLS_ORG, 
        CSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY, 
        cddes_pck.code_desc AS SAP_PRNT_CUST_DESC, 
        LTRIM(CD.COMPANY, 0) AS SAP_CMP_ID, 
        CD.CTRY_KEY AS SAP_CNTRY_CD, 
        CD.CTRY_NM AS SAP_CNTRY_NM, 
        CBD.ADDR AS SAP_ADDR, 
        CBD.RGN AS SAP_REGION, 
        CBD.DSTRC AS SAP_STATE_CD, 
        CBD.CITY AS SAP_CITY, 
        CBD.PSTL_CD AS SAP_POST_CD, 
        CSD.DSTR_CHNL AS SAP_CHNL_CD, 
        DC.TXTSH AS SAP_CHNL_DESC, 
        CSD.SLS_OFC AS SAP_SLS_OFFICE_CD, 
        CSD.SLS_OFC_DESC AS SAP_SLS_OFFICE_DESC, 
        CSD.SLS_GRP AS SAP_SLS_GRP_CD, 
        CSD.SLS_GRP_DESC AS SAP_SLS_GRP_DESC, 
        CSD.CRNCY_KEY AS SAP_CURR_CD, 
        CSD.CHNL_KEY AS SAP_CUST_CHNL_KEY, 
        cddes_chnl.code_desc AS SAP_CUST_CHNL_DESC, 
        CSD.SUB_CHNL_KEY AS SAP_CUST_SUB_CHNL_KEY, 
        cddes_subchnl.code_desc AS SAP_SUB_CHNL_DESC, 
        CSD.GO_TO_MDL_KEY AS SAP_GO_TO_MDL_KEY, 
        cddes_gtm.code_desc AS SAP_GO_TO_MDL_DESC, 
        CSD.BNR_KEY AS SAP_BNR_KEY, 
        cddes_bnrkey.code_desc AS SAP_BNR_DESC, 
        CSD.BNR_FRMT_KEY AS SAP_BNR_FRMT_KEY, 
        cddes_bnrfmt.code_desc AS SAP_BNR_FRMT_DESC, 
        subchnl_retail_env.retail_env, 
        GCH.GCGH_REGION AS GCH_REGION, 
        GCH.GCGH_CLUSTER AS GCH_CLUSTER, 
        GCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER, 
        GCH.GCGH_MARKET AS GCH_MARKET, 
        GCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER, 
        -- ROW_NUMBER() OVER (PARTITION BY LTRIM(csd.CUST_NUM,0) ORDER BY sap_prnt_cust_desc ASC,DECODE(CSD.CUST_DEL_FLAG,NULL,'O','','O',CSD.CUST_DEL_FLAG) ASC,csd.sls_org,csd.dstr_chnl) AS rnk
        ROW_NUMBER() OVER (
            PARTITION BY sap_prnt_cust_key 
            ORDER BY 
            sap_prnt_cust_key ASC
        ) AS rnk 
        FROM 
        EDW_CUSTOMER_SALES_DIM CSD 
        INNER JOIN EDW_CUSTOMER_BASE_DIM CBD ON LTRIM (CSD.CUST_NUM, 0) = LTRIM (CBD.CUST_NUM, 0) 
        LEFT OUTER JOIN EDW_GCH_CUSTOMERHIERARCHY GCH ON LTRIM (GCH.CUSTOMER, 0) = LTRIM (CBD.CUST_NUM, 0) 
        INNER JOIN EDW_DSTRBTN_CHNL DC ON LTRIM (CSD.DSTR_CHNL, 0) = LTRIM (DC.DISTR_CHAN, 0) 
        INNER JOIN EDW_SALES_ORG_DIM SOD ON TRIM (CSD.SLS_ORG) = TRIM (SOD.SLS_ORG) -- AND SOD.ctry_key = 'TH'
        AND SOD.SLS_ORG IN ('1500', '8888', '100A') 
        INNER JOIN EDW_COMPANY_DIM CD ON SOD.SLS_ORG_CO_CD = CD.CO_CD ---------- code BY SG TEAM
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_PCK ON TRIM (
            UPPER (CDDES_PCK.CODE_TYPE)
        ) = 'PARENT CUSTOMER KEY' 
        AND CDDES_PCK.CODE = CSD.PRNT_CUST_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_BNRKEY ON TRIM (
            UPPER (cddes_bnrkey.code_type)
        ) = 'BANNER KEY' 
        AND CDDES_BNRKEY.CODE = CSD.BNR_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_BNRFMT ON TRIM (
            UPPER (cddes_bnrfmt.code_type)
        ) = 'BANNER FORMAT KEY' 
        AND CDDES_BNRFMT.CODE = CSD.BNR_FRMT_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_CHNL ON TRIM (
            UPPER (cddes_chnl.code_type)
        ) = 'CHANNEL KEY' 
        AND CDDES_CHNL.CODE = CSD.CHNL_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_GTM ON TRIM (
            UPPER (cddes_gtm.code_type)
        ) = 'GO TO MODEL KEY' 
        AND CDDES_GTM.CODE = CSD.GO_TO_MDL_KEY 
        LEFT JOIN EDW_CODE_DESCRIPTIONS CDDES_SUBCHNL ON TRIM (
            UPPER (
            cddes_subchnl.code_type
            )
        ) = 'SUB CHANNEL KEY' 
        AND CDDES_SUBCHNL.CODE = CSD.SUB_CHNL_KEY 
        LEFT JOIN EDW_SUBCHNL_RETAIL_ENV_MAPPING SUBCHNL_RETAIL_ENV ON UPPER (SUBCHNL_RETAIL_ENV.SUB_CHANNEL) = UPPER (CDDES_SUBCHNL.CODE_DESC)
    ) 
    WHERE 
    RNK = 1 
    and NVL(sap_prnt_cust_key, '#') IN('PC0004', 'PC0013', 'PC0014') --and NVL(gch_retail_banner,'#') IN ('Watsons','Tesco','Wal-mart')
), 
CURRENCY AS 
(
    Select * from 
    vw_edw_reg_exch_rate 
    where cntry_key = 'CN'
    AND TO_CCY = 'USD' 
    AND JJ_MNTH_ID = (SELECT MAX(JJ_MNTH_ID) FROM vw_edw_reg_exch_rate )
), 
PRODUCT AS 
(
    Select 
    * 
    from 
    (
        SELECT 
        DISTINCT EMD.matl_num AS SAP_MATL_NUM, 
        EMD.MATL_DESC AS SAP_MAT_DESC, 
        EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD, 
        EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC, 
        -- EMD.SAP_BASE_UOM_CD AS SAP_BASE_UOM_CD,
        --  EMD.SAP_PRCHSE_UOM_CD AS SAP_PRCHSE_UOM_CD,
        EMD.PRODH1 AS SAP_PROD_SGMT_CD, 
        EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC, 
        -- EMD.SAP_BASE_PROD_CD AS SAP_BASE_PROD_CD,
        EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC, 
        --  EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
        EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC, 
        --   EMD.SAP_BRND_CD AS SAP_BRND_CD,
        EMD.BRND_DESC AS SAP_BRND_DESC, 
        -- EMD.SAP_VRNT_CD AS SAP_VRNT_CD,
        EMD.VARNT_DESC AS SAP_VRNT_DESC, 
        --  EMD.SAP_PUT_UP_CD AS SAP_PUT_UP_CD,
        EMD.PUT_UP_DESC AS SAP_PUT_UP_DESC, 
        EMD.PRODH2 AS SAP_GRP_FRNCHSE_CD, 
        EMD.PRODH2_TXTMD AS SAP_GRP_FRNCHSE_DESC, 
        EMD.PRODH3 AS SAP_FRNCHSE_CD, 
        EMD.PRODH3_TXTMD AS SAP_FRNCHSE_DESC, 
        EMD.PRODH4 AS SAP_PROD_FRNCHSE_CD, 
        EMD.PRODH4_TXTMD AS SAP_PROD_FRNCHSE_DESC, 
        EMD.PRODH5 AS SAP_PROD_MJR_CD, 
        EMD.PRODH5_TXTMD AS SAP_PROD_MJR_DESC, 
        EMD.PRODH5 AS SAP_PROD_MNR_CD, 
        EMD.PRODH5_TXTMD AS SAP_PROD_MNR_DESC, 
        EMD.PRODH6 AS SAP_PROD_HIER_CD, 
        EMD.PRODH6_TXTMD AS SAP_PROD_HIER_DESC, 
        -- EMD.greenlight_sku_flag as greenlight_sku_flag,
        EMD.pka_product_key as pka_product_key, 
        EMD.pka_size_desc as pka_size_desc, 
        EMD.pka_product_key_description as pka_product_key_description, 
        EMD.pka_product_key as product_key, 
        EMD.pka_product_key_description as product_key_description, 
        EGPH."region" AS GPH_REGION, 
        EGPH.regional_franchise AS GPH_REG_FRNCHSE, 
        EGPH.regional_franchise_group AS GPH_REG_FRNCHSE_GRP, 
        EGPH.GCPH_FRANCHISE AS GPH_PROD_FRNCHSE, 
        EGPH.GCPH_BRAND AS GPH_PROD_BRND, 
        EGPH.GCPH_SUBBRAND AS GPH_PROD_SUB_BRND, 
        EGPH.GCPH_VARIANT AS GPH_PROD_VRNT, 
        EGPH.GCPH_NEEDSTATE AS GPH_PROD_NEEDSTATE, 
        EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY, 
        EGPH.GCPH_SUBCATEGORY AS GPH_PROD_SUBCTGRY, 
        EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT, 
        EGPH.GCPH_SUBSEGMENT AS GPH_PROD_SUBSGMNT, 
        EGPH.PUT_UP_CODE AS GPH_PROD_PUT_UP_CD, 
        EGPH.PUT_UP_DESCRIPTION AS GPH_PROD_PUT_UP_DESC, 
        EGPH.SIZE AS GPH_PROD_SIZE, 
        EGPH.UNIT_OF_MEASURE AS GPH_PROD_SIZE_UOM, 
        row_number() over(
            partition by sap_matl_num 
            order by 
            sap_matl_num
        ) rnk 
        FROM 
        --(Select * from  edw_vw_greenlight_skus where  SLS_ORG IN ('1500', '8888', '100A') ) EMD,
        (
            Select 
            * 
            from 
            edw_material_dim
        ) EMD, 
        EDW_GCH_PRODUCTHIERARCHY EGPH 
        WHERE 
        LTRIM(EMD.MATL_NUM, '0') = ltrim(
            EGPH.MATERIALNUMBER(+), 
            0
        ) 
        AND EMD.PROD_HIER_CD <> '' 
        AND LTRIM(EMD.MATL_NUM, '0') IN (
            SELECT 
            DISTINCT LTRIM(MATL_NUM, '0') 
            FROM 
            edw_material_sales_dim 
            WHERE 
            sls_org in ('1500', '8888', '100A')
        )
    ) 
    where 
    rnk = 1
),
onsesea as 
(
    SELECT  
          T5.cal_YEAR as YEAR, 
          CAST(T5.cal_qrtr_no AS VARCHAR) AS YEAR_QUARTER, 
          CAST(T5.cal_mnth_id AS VARCHAR) AS MONTH_YEAR, 
          T5.cal_mnth_no as mnth_no, 
          TRIM(
            NVL(
              NULLIF(t1.country_name, ''), 
              'NA'
            )
          ) AS country_name, 
          TRIM(
            NVL(
              NULLIF(T1.sold_to, ''), 
              'NA'
            )
          ) AS sold_to_ref, 
          'NA' as DSTRBTR_GRP_CD, 
          'NA' as DSTRBTR_GRP_CD_name, 
          TRIM(
            NVL(
              NULLIF(T1.bu, ''), 
              'NA'
            )
          ) as bu, 
          TRIM(
            NVL(
              NULLIF(T3.GPH_PROD_FRNCHSE, ''), 
              'NA'
            )
          ) AS GLOBAL_PROD_FRANCHISE, 
          TRIM(
            NVL(
              NULLIF(T3.GPH_PROD_BRND, ''), 
              'NA'
            )
          ) AS GLOBAL_PROD_BRAND, 
          TRIM(
            NVL(
              NULLIF(T3.GPH_PROD_SUB_BRND, ''), 
              'NA'
            )
          ) AS GLOBAL_PROD_SUB_BRAND, 
          TRIM(
            NVL(
              NULLIF(T3.GPH_PROD_VRNT, ''), 
              'NA'
            )
          ) AS GLOBAL_PROD_VARIANT, 
          TRIM(
            NVL(
              NULLIF(T3.GPH_PROD_SGMNT, ''), 
              'NA'
            )
          ) AS GLOBAL_PROD_SEGMENT, 
          TRIM(
            NVL(
              NULLIF(T3.GPH_PROD_SUBSGMNT, ''), 
              'NA'
            )
          ) AS GLOBAL_PROD_SUBSEGMENT, 
          TRIM(
            NVL(
              NULLIF(T3.GPH_PROD_CTGRY, ''), 
              'NA'
            )
          ) AS GLOBAL_PROD_CATEGORY, 
          TRIM(
            NVL(
              NULLIF(T3.GPH_PROD_SUBCTGRY, ''), 
              'NA'
            )
          ) AS GLOBAL_PROD_SUBCATEGORY, 
          --TRIM(NVL(NULLIF(T3.GPH_PROD_PUT_UP_DESC,''),'NA')) AS GLOBAL_PUT_UP_DESC,
          TRIM(
            NVL(
              NULLIF(T3.SAP_MATL_NUM, ''), 
              'NA'
            )
          ) AS SKU_CD, 
          TRIM(
            NVL(
              NULLIF(T3.SAP_MAT_DESC, ''), 
              'NA'
            )
          ) AS SKU_DESCRIPTION, 
          --TRIM(NVL(NULLIF(T3.greenlight_sku_flag,''),'NA')) AS greenlight_sku_flag,
          TRIM(
            NVL(
              NULLIF(T3.pka_product_key, ''), 
              'NA'
            )
          ) AS pka_product_key, 
          TRIM(
            NVL(
              NULLIF(T3.pka_size_desc, ''), 
              'NA'
            )
          ) AS pka_size_desc, 
          TRIM(
            NVL(
              NULLIF(
                T3.pka_product_key_description, 
                ''
              ), 
              'NA'
            )
          ) AS pka_product_key_description, 
          TRIM(
            NVL(
              NULLIF(T3.product_key, ''), 
              'NA'
            )
          ) AS product_key, 
          TRIM(
            NVL(
              NULLIF(T3.product_key_description, ''), 
              'NA'
            )
          ) AS product_key_description, 
          CAST('RMB' AS VARCHAR) AS FROM_CCY, 
          CAST('USD' AS VARCHAR) AS TO_CCY, 
          T2.EXCH_RATE, 
          case when t4.SAP_PRNT_CUST_KEY = '' 
          or t4.SAP_PRNT_CUST_KEY is null then 'Not Assigned' else TRIM(t4.SAP_PRNT_CUST_KEY) end as SAP_PRNT_CUST_KEY, 
          case when t4.SAP_PRNT_CUST_DESC = '' 
          or t4.SAP_PRNT_CUST_DESC is null then 'Not Assigned' else TRIM(t4.SAP_PRNT_CUST_DESC) end as SAP_PRNT_CUST_DESC, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_CUST_CHNL_KEY, ''), 
              'NA'
            )
          ) AS SAP_CUST_CHNL_KEY, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_CUST_CHNL_DESC, ''), 
              'NA'
            )
          ) AS SAP_CUST_CHNL_DESC, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_CUST_SUB_CHNL_KEY, ''), 
              'NA'
            )
          ) AS SAP_CUST_SUB_CHNL_KEY, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_SUB_CHNL_DESC, ''), 
              'NA'
            )
          ) AS SAP_SUB_CHNL_DESC, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_GO_TO_MDL_KEY, ''), 
              'NA'
            )
          ) AS SAP_GO_TO_MDL_KEY, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_GO_TO_MDL_DESC, ''), 
              'NA'
            )
          ) AS SAP_GO_TO_MDL_DESC, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_BNR_KEY, ''), 
              'NA'
            )
          ) AS SAP_BNR_KEY, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_BNR_DESC, ''), 
              'NA'
            )
          ) AS SAP_BNR_DESC, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_BNR_FRMT_KEY, ''), 
              'NA'
            )
          ) AS SAP_BNR_FRMT_KEY, 
          TRIM(
            NVL(
              NULLIF(T4.SAP_BNR_FRMT_DESC, ''), 
              'NA'
            )
          ) AS SAP_BNR_FRMT_DESC, 
          TRIM(
            NVL(
              NULLIF(T4.RETAIL_ENV, ''), 
              'NA'
            )
          ) AS RETAIL_ENV, 
          TRIM(
            NVL(
              NULLIF(T1.REGION, ''), 
              'NA'
            )
          ) AS REGION, 
          TRIM(
            NVL(
              NULLIF(T1.ZONE_OR_AREA, ''), 
              'NA'
            )
          ) AS ZONE_OR_AREA, 
          propagate_flag, 
          propagate_from, 
          case when propagate_flag = 'N' then 'Not propagate' else reason end as reason, 
          replicated_flag, 
          sum(last_3months_so) as last_3months_so_qty, 
          sum(last_6months_so) as last_6months_so_qty, 
          sum(last_12months_so) as last_12months_so_qty, 
          sum(last_3months_so_value) as last_3months_so_val, 
          sum(last_6months_so_value) as last_6months_so_val, 
          sum(last_12months_so_value) as last_12months_so_val, 
          sum(last_36months_so_value) as last_36months_so_val, 
          cast(
            (
              sum(last_3months_so_value * T2.Exch_rate)
            ) as numeric(38, 5)
          ) as last_3months_so_val_usd, 
          cast(
            (
              sum(last_6months_so_value * T2.Exch_rate)
            ) as numeric(38, 5)
          ) as last_6months_so_val_usd, 
          cast(
            (
              sum(last_12months_so_value * T2.Exch_rate)
            ) as numeric(38, 5)
          ) as last_12months_so_val_usd, 
          
          SUM(T1.sell_in_qty) AS SI_SLS_QTY, 
          SUM(T1.sell_in_value) AS SI_GTS_VAL, 
          SUM(T1.sell_in_value * T2.EXCH_RATE) AS SI_GTS_VAL_USD, 
          SUM(T1.inv_qty) AS INVENTORY_QUANTITY, 
          SUM(T1.inv_value) AS INVENTORY_VAL, 
          SUM(T1.inv_value * T2.EXCH_RATE) AS INVENTORY_VAL_USD, 
          SUM(T1.SO_QTY) AS SO_SLS_QTY, 
          SUM(T1.SO_VALue) AS SO_TRD_SLS, 
          Round(SUM(T1.SO_VALue * T2.EXCH_RATE)) AS SO_TRD_SLS_USD
        FROM 
        SISO T1 CROSS JOIN 
          CURRENCY T2 CROSS JOIN
          PRODUCT T3 CROSS JOIN 
          TIME T5 CROSS JOIN 
          customer T4 
        WHERE 
          LTRIM(T3.SAP_MATL_NUM(+),'0') = ltrim(T1.matl_num, 0) 
          and LTRIM(T4.SAP_CUST_ID(+),'0')= ltrim(T1.sold_to, 0) 
          AND T1.month = T5.cal_mnth_id 
          AND LEFT(T1.MONTH, 4) >= (DATE_PART(year, current_timestamp()) -2) 
        GROUP BY 
          T5.Cal_YEAR, 
          T5.cal_QRTR_NO, 
          T5.cal_MNTH_ID, 
          T5.cal_MNTH_NO, 
          t1.country_name, 
          t1.sold_to, 
          DSTRBTR_GRP_CD, 
          DSTRBTR_GRP_CD_name, 
          T1.bu, 
          T3.GPH_PROD_FRNCHSE, 
          T3.GPH_PROD_BRND, 
          T3.GPH_PROD_SUB_BRND, 
          T3.GPH_PROD_VRNT, 
          T3.GPH_PROD_SGMNT, 
          T3.GPH_PROD_SUBSGMNT, 
          T3.GPH_PROD_CTGRY, 
          T3.GPH_PROD_SUBCTGRY, 
          --GLOBAL_PUT_UP_DESC,
          T3.SAP_MATL_NUM, 
          T3.SAP_MAT_DESC, 
          -- greenlight_sku_flag,
          T3.pka_product_key, 
          T3.pka_size_desc, 
          T3.pka_product_key_description, 
          T3.product_key, 
          T3.product_key_description, 
          --FROM_CCY, 
          --TO_CCY, 
          T2.EXCH_RATE,
          T4.SAP_PRNT_CUST_KEY, 
          T4.SAP_PRNT_CUST_DESC, 
          T4.SAP_CUST_CHNL_KEY, 
          T4.SAP_CUST_CHNL_DESC, 
          T4.SAP_CUST_SUB_CHNL_KEY, 
          T4.SAP_SUB_CHNL_DESC, 
          T4.SAP_GO_TO_MDL_KEY, 
          T4.SAP_GO_TO_MDL_DESC, 
          T4.SAP_BNR_KEY, 
          T4.SAP_BNR_DESC, 
          T4.SAP_BNR_FRMT_KEY, 
          T4.SAP_BNR_FRMT_DESC, 
          T4.RETAIL_ENV, 
          T1.REGION, 
          T1.ZONE_OR_AREA, 
          propagate_flag, 
          propagate_from, 
          reason, 
          replicated_flag
),
regional as
(
    SELECT 
    ONSESEA.*, 
    SUM(SI_GTS_VAL) OVER (
      PARTITION BY COUNTRY_NAME, YEAR, MONTH_YEAR
    ) AS SI_INV_DB_VAL, 
    SUM(SI_GTS_VAL_USD) OVER (
      PARTITION BY COUNTRY_NAME, YEAR, MONTH_YEAR
    ) AS SI_INV_DB_VAL_USD 
  FROM 
    ONSESEA 
  where 
    COUNTRY_NAME || SAP_PRNT_CUST_DESC IN (
      SELECT 
        COUNTRY_NAME || SAP_PRNT_CUST_DESC AS INCLUSION 
      FROM 
        (
          SELECT 
            COUNTRY_NAME, 
            SAP_PRNT_CUST_DESC 
          FROM 
            ONSESEA -- WHERE bu in (Select parameter_value  from rg_itg.itg_parameter_reg_inventory 
            --    where country_name='China' and parameter_name='BU')
            --and SAP_PRNT_CUST_DESC IS NOT NULL 
          GROUP BY 
            COUNTRY_NAME, 
            SAP_PRNT_CUST_DESC)
    )
),
final as
(
    Select 
    cast(year as int) as year, 
    year_quarter, 
    month_year, 
    mnth_no, 
    country_name, 
    sold_to_ref, 
    dstrbtr_grp_cd, 
    dstrbtr_grp_cd_name, 
    global_prod_franchise, 
    global_prod_brand, 
    global_prod_sub_brand, 
    global_prod_variant, 
    global_prod_segment, 
    global_prod_subsegment, 
    global_prod_category, 
    global_prod_subcategory, 
    pka_size_desc as global_put_up_desc, 
    sku_cd, 
    sku_description, 
    -- greenlight_sku_flag,
    pka_product_key, 
    pka_product_key_description, 
    product_key, 
    product_key_description, 
    from_ccy, 
    to_ccy, 
    exch_rate, 
    sap_prnt_cust_key, 
    sap_prnt_cust_desc, 
    sap_cust_chnl_key, 
    sap_cust_chnl_desc, 
    sap_cust_sub_chnl_key, 
    sap_sub_chnl_desc, 
    sap_go_to_mdl_key, 
    sap_go_to_mdl_desc, 
    sap_bnr_key, 
    sap_bnr_desc, 
    sap_bnr_frmt_key, 
    sap_bnr_frmt_desc, 
    retail_env, 
    region, 
    zone_or_area, 
    round(
        cast(
        si_sls_qty as numeric(38, 5)
        ), 
        5
    ) as si_sls_qty, 
    round(
        cast(
        si_gts_val as numeric (38, 5)
        ), 
        5
    ) as si_gts_val, 
    cast(
        si_gts_val_usd as numeric(38, 5)
    ) as si_gts_val_usd, 
    round(
        cast (
        inventory_quantity as numeric(38, 5)
        ), 
        5
    ) as inventory_quantity, 
    round(
        cast(
        inventory_val as numeric(38, 5)
        ), 
        5
    ) as inventory_val, 
    cast(
        inventory_val_usd as numeric(38, 5)
    ) as inventory_val_usd, 
    round(
        cast (
        so_sls_qty as numeric(38, 5)
        ), 
        5
    ) as so_sls_qty, 
    round(
        cast (
        so_trd_sls as numeric(38, 5)
        ), 
        5
    ) as so_trd_sls, 
    so_trd_sls_usd, 
    last_3months_so_qty, 
    last_6months_so_qty, 
    last_12months_so_qty, 
    last_3months_so_val, 
    last_3months_so_val_usd, 
    last_6months_so_val, 
    last_6months_so_val_usd, 
    last_12months_so_val, 
    last_12months_so_val_usd, 
    propagate_flag, 
    propagate_from, 
    reason, 
    last_36months_so_val 
    from 
    regional
)
select * from final