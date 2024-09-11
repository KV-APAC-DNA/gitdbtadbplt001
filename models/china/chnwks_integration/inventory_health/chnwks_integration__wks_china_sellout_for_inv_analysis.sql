with edw_material_dim as
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
itg_parameter_reg_inventory as 
(
    select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
),
v_rpt_inventory_cube_new_inventory_analysis as 
(
    select * from {{ source('chnedw_integration', 'v_rpt_inventory_cube_new_inventory_analysis') }}
),
v_rpt_pos_sales as 
(
    select * from {{ source('chnedw_integration', 'v_rpt_pos_sales') }}
),
final as
(
    WITH PRODUCT AS (
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
            --  EMD.SAP_BRND_CD AS SAP_BRND_CD,
            EMD.BRND_DESC AS SAP_BRND_DESC, 
            --  EMD.SAP_VRNT_CD AS SAP_VRNT_CD,
            EMD.VARNT_DESC AS SAP_VRNT_DESC, 
            -- EMD.SAP_PUT_UP_CD AS SAP_PUT_UP_CD,
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
    CUSTOMER as (
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
        and NVL(sap_prnt_cust_key, '#') IN('PC0004', 'PC0013', 'PC0014') 
        and NVL(gch_retail_banner, '#') IN ('Watsons', 'Tesco', 'Wal-mart')
    ) 
    SELECT 
    min(bill_dt) as min_date, 
    TRIM(
        NVL(
        NULLIF(T3.GPH_PROD_BRND, ''), 
        'NA'
        )
    ) AS GLOBAL_PROD_BRAND, 
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
        NULLIF(T3.GPH_PROD_CTGRY, ''), 
        'NA'
        )
    ) AS GLOBAL_PROD_CATEGORY, 
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
    case when t4.SAP_PRNT_CUST_KEY = '' 
    or t4.SAP_PRNT_CUST_KEY is null then 'Not Assigned' else TRIM(t4.SAP_PRNT_CUST_KEY) end as SAP_PRNT_CUST_KEY 
    FROM 
    (
        select 
        case when yyyymm is null then null else cast(
            (
            left(yyyymm, 4)|| '-' || substring(yyyymm, 5, 6)|| '-01'
            ) as date
        ) end as bill_dt, 
        nvl(
            nullif(sku_code, ''), 
            'NA'
        ) AS matl_num, 
        sold_to_code AS sold_to, 
        current_month_sellout_gts AS so_val 
        FROM 
        v_rpt_inventory_cube_new_inventory_analysis 
        WHERE 
        sales_category_level1 NOT IN (
            Select 
            parameter_value 
            from 
            itg_parameter_reg_inventory 
            where 
            country_name = 'China' 
            and parameter_name = 'sales_category_level1'
        ) 
        AND current_month_sellout_gts > 0 
        UNION ALL 
        select 
        case when month is null then null else cast(
            (
            left(month, 4)|| '-' || substring(month, 5, 6)|| '-01'
            ) as date
        ) end as bill_dt, 
        nvl(
            nullif(p_code, ''), 
            'NA'
        ) AS matl_num, 
        CASE WHEN ka = '屈臣氏' THEN 'PC0004' WHEN ka = '沃尔玛' THEN 'PC0014' WHEN ka = '乐购' THEN 'PC0013' END AS sold_to, 
        pos_cost AS so_val 
        FROM 
        v_rpt_pos_sales 
        WHERE 
        NVL(KA, 'NA') IN (
            '屈臣氏', '沃尔玛', '乐购'
        ) 
        AND LEFT(month, 4)>= (
            DATE_PART(YEAR, current_timestamp()) -6
        ) 
        AND pos_cost > 0
    ) T1, 
    PRODUCT T3, 
    (
        SELECT 
        * 
        FROM 
        customer
    ) T4 
    WHERE 
    LTRIM(
        T3.SAP_MATL_NUM(+), 
        '0'
    ) = ltrim(T1.matl_num, 0) 
    and LTRIM(
        T4.SAP_CUST_ID(+), 
        '0'
    )= ltrim(T1.sold_to, 0) 
    GROUP BY 
    T3.GPH_PROD_BRND, 
    T3.GPH_PROD_VRNT, 
    T3.GPH_PROD_SGMNT, 
    T3.GPH_PROD_CTGRY, 
    T3.pka_size_desc, 
    T3.pka_product_key, 
    T4.SAP_PRNT_CUST_KEY
)
select * from final