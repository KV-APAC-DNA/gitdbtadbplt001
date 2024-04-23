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
edw_gch_customerhierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as
(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as(
select * from {{ref('aspedw_integration__edw_sales_org_dim')}}
),
edw_code_descriptions as
(
select * from {{ref('aspedw_integration__edw_code_descriptions')}}
),
edw_subchnl_retail_env_mapping as
(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
edw_list_price as
(
    select * from {{ref('aspedw_integration__edw_list_price')}}
),
itg_parameter_reg_inventory as
(
    select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
),
vw_customer_dim as
(
    select * from {{ref('pcfedw_integration__vw_customer_dim')}}
),
vw_iri_scan_sales as
(
    select * from {{ref('pcfedw_integration__vw_iri_scan_sales')}}
),
vw_iri_scan_sales_analysis as
(
    select * from {{ref('pcfedw_integration__vw_iri_scan_sales_analysis')}}
),
edw_material_dim_pcf as
(
    select * from {{ref('pcfedw_integration__edw_material_dim')}}
),
vw_sapbw_ciw_fact as
(
    select * from {{ref('pcfedw_integration__vw_sapbw_ciw_fact')}}
),
vw_apo_parent_child_dim as
(
    select * from {{ref('pcfedw_integration__vw_apo_parent_child_dim')}}
),
edw_metcash_ind_grocery_fact as
(
    select * from {{ref('pcfedw_integration__edw_metcash_ind_grocery_fact')}}
),
edw_perenso_prod_dim as
(
    select * from {{ref('pcfedw_integration__edw_perenso_prod_dim')}}
),
itg_customer_sellout as
(
    select * from {{ref('pcfitg_integration__itg_customer_sellout')}}
),
itg_mds_pacific_prod_mapping_cwh as
(
    select * from {{ref('pcfitg_integration__itg_mds_pacific_prod_mapping_cwh')}}
),
product AS 
(
    Select *
    from (
            SELECT DISTINCT EMD.matl_num AS SAP_MATL_NUM,
                EMD.MATL_DESC AS SAP_MAT_DESC,
                EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
                EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
                --  EMD.SAP_BASE_UOM_CD AS SAP_BASE_UOM_CD,
                --  EMD.SAP_PRCHSE_UOM_CD AS SAP_PRCHSE_UOM_CD,
                EMD.PRODH1 AS SAP_PROD_SGMT_CD,
                EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
                --  EMD.SAP_BASE_PROD_CD AS SAP_BASE_PROD_CD,
                EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
                --  EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
                EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
                --   EMD.SAP_BRND_CD AS SAP_BRND_CD,
                EMD.BRND_DESC AS SAP_BRND_DESC,
                --  EMD.SAP_VRNT_CD AS SAP_VRNT_CD,
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
                EMD.pka_product_key_description as pka_product_key_description,
                EMD.pka_product_key as product_key,
                EMD.pka_size_desc as pka_size_desc,
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
                    order by sap_matl_num
                ) rnk
            FROM --  (Select * from edw_vw_greenlight_skus WHERE sls_org in ( '3300', '330B','330H')) EMD,
                (
                    Select *
                    from edw_material_dim
                ) EMD,
                EDW_GCH_PRODUCTHIERARCHY EGPH
            WHERE LTRIM(EMD.MATL_NUM, '0') = ltrim(EGPH.MATERIALNUMBER(+), 0)
                AND EMD.PROD_HIER_CD <> ''
                AND LTRIM(EMD.MATL_NUM, '0') IN (
                    SELECT DISTINCT LTRIM(MATL_NUM, '0')
                    FROM edw_material_sales_dim
                    WHERE sls_org in ('3300', '330B', '330H')
                )
        )
    where rnk = 1
),
CUSTOMER AS (
    SELECT DISTINCT ECBD.CUST_NUM AS SAP_CUST_ID,
        ECBD.CUST_NM AS SAP_CUST_NM,
        ECSD.SLS_ORG AS SAP_SLS_ORG,
        ECD.COMPANY AS SAP_CMP_ID,
        ECD.CTRY_KEY AS SAP_CNTRY_CD,
        ECD.CTRY_NM AS SAP_CNTRY_NM,
        ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
        CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC,
        ECSD.CHNL_KEY AS SAP_CUST_CHNL_KEY,
        CDDES_CHNL.CODE_DESC AS SAP_CUST_CHNL_DESC,
        ECSD.SUB_CHNL_KEY AS SAP_CUST_SUB_CHNL_KEY,
        CDDES_SUBCHNL.CODE_DESC AS SAP_SUB_CHNL_DESC,
        ECSD.GO_TO_MDL_KEY AS SAP_GO_TO_MDL_KEY,
        CDDES_GTM.CODE_DESC AS SAP_GO_TO_MDL_DESC,
        ECSD.BNR_KEY AS SAP_BNR_KEY,
        CDDES_BNRKEY.CODE_DESC AS SAP_BNR_DESC,
        ECSD.BNR_FRMT_KEY AS SAP_BNR_FRMT_KEY,
        CDDES_BNRFMT.CODE_DESC AS SAP_BNR_FRMT_DESC,
        SUBCHNL_RETAIL_ENV.RETAIL_ENV,
        null AS REGION,
        null AS ZONE_OR_AREA,
        EGCH.GCGH_REGION AS GCH_REGION,
        EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
        EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
        EGCH.GCGH_MARKET AS GCH_MARKET,
        EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
        ROW_NUMBER() OVER (
            PARTITION BY SAP_CUST_ID
            ORDER BY SAP_PRNT_CUST_KEY DESC
        ) AS RANK
    FROM EDW_GCH_CUSTOMERHIERARCHY EGCH,
        EDW_CUSTOMER_SALES_DIM ECSD,
        EDW_CUSTOMER_BASE_DIM ECBD,
        EDW_COMPANY_DIM ECD,
        EDW_DSTRBTN_CHNL EDC,
        EDW_SALES_ORG_DIM ESOD,
        EDW_CODE_DESCRIPTIONS CDDES_PCK,
        EDW_CODE_DESCRIPTIONS CDDES_BNRKEY,
        EDW_CODE_DESCRIPTIONS CDDES_BNRFMT,
        EDW_CODE_DESCRIPTIONS CDDES_CHNL,
        EDW_CODE_DESCRIPTIONS CDDES_GTM,
        EDW_CODE_DESCRIPTIONS CDDES_SUBCHNL,
        EDW_SUBCHNL_RETAIL_ENV_MAPPING SUBCHNL_RETAIL_ENV,
        (
            SELECT CUST_NUM,
                MIN(
                    DECODE(CUST_DEL_FLAG, NULL, 'O', '', 'O', CUST_DEL_FLAG)
                ) AS CUST_DEL_FLAG
            FROM EDW_CUSTOMER_SALES_DIM
            WHERE SLS_ORG IN ('3300', '330B', '330H')
            GROUP BY CUST_NUM
        ) A
    WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
        AND ECSD.CUST_NUM = ECBD.CUST_NUM
        AND DECODE(
            ECSD.CUST_DEL_FLAG,
            NULL,
            'O',
            '',
            'O',
            ECSD.CUST_DEL_FLAG
        ) = A.CUST_DEL_FLAG
        AND A.CUST_NUM = ECSD.CUST_NUM
        AND ECSD.DSTR_CHNL = EDC.DISTR_CHAN
        AND ECSD.SLS_ORG = ESOD.SLS_ORG
        AND ESOD.SLS_ORG_CO_CD = ECD.CO_CD
        AND ECSD.SLS_ORG IN ('3300', '330B', '330H')
        AND CDDES_PCK.CODE_TYPE(+) = 'Parent Customer Key'
        AND CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
        AND cddes_bnrkey.code_type(+) = 'Banner Key'
        AND CDDES_BNRKEY.CODE(+) = ECSD.BNR_KEY
        AND cddes_bnrfmt.code_type(+) = 'Banner Format Key'
        AND CDDES_BNRFMT.CODE(+) = ECSD.BNR_FRMT_KEY
        AND cddes_chnl.code_type(+) = 'Channel Key'
        AND CDDES_CHNL.CODE(+) = ECSD.CHNL_KEY
        AND cddes_gtm.code_type(+) = 'Go To Model Key'
        AND CDDES_GTM.CODE(+) = ECSD.GO_TO_MDL_KEY
        AND cddes_subchnl.code_type(+) = 'Sub Channel Key'
        AND CDDES_SUBCHNL.CODE(+) = ECSD.SUB_CHNL_KEY
        AND UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL(+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
),
transformed as
(
    SELECT min(bill_dt) as min_date,
        TRIM(NVL(NULLIF(T3.GPH_PROD_BRND, ''), 'NA')) AS GLOBAL_PROD_BRAND,
        TRIM(NVL(NULLIF(T3.GPH_PROD_VRNT, ''), 'NA')) AS GLOBAL_PROD_VARIANT,
        TRIM(NVL(NULLIF(T3.GPH_PROD_SGMNT, ''), 'NA')) AS GLOBAL_PROD_SEGMENT,
        TRIM(NVL(NULLIF(T3.GPH_PROD_CTGRY, ''), 'NA')) AS GLOBAL_PROD_CATEGORY,
        TRIM(NVL(NULLIF(T3.pka_product_key, ''), 'NA')) AS pka_product_key,
        TRIM(NVL(NULLIF(T3.pka_size_desc, ''), 'NA')) AS pka_size_desc,
        TRIM(NVL(NULLIF(T4.SAP_PRNT_CUST_KEY, ''), 'NA')) AS SAP_PRNT_CUST_KEY
    FROM (
            SELECT sap_prnt_cust_key as sap_parent_customer_key,
                bill_dt,
                coalesce(nullif(matl_id, ''), 'NA') as matl_num,
                so_value
            FROM (
                    (
                        SELECT cust.sap_prnt_cust_key,
                            UPPER(trim(cust.sap_prnt_cust_desc::TEXT))::CHARACTER VARYING AS sap_prnt_cust_desc,
                            a.wk_end_dt as bill_dt,
                            a.matl_id::CHARACTER VARYING AS matl_id,
                            (a.so_qty * lp.amount)::NUMERIC(16, 4) AS so_value
                        FROM (
                                SELECT DISTINCT ecsd.prnt_cust_key AS sap_prnt_cust_key,
                                    cddes_pck.code_desc AS sap_prnt_cust_desc
                                FROM edw_customer_base_dim ecbd,
                                    edw_customer_sales_dim ecsd
                                    LEFT JOIN edw_code_descriptions cddes_pck ON cddes_pck.code::TEXT = ecsd.prnt_cust_key::TEXT
                                    AND cddes_pck.code_type::TEXT = 'Parent Customer Key'::CHARACTER VARYING::TEXT
                                WHERE ecsd.cust_num::TEXT = ecbd.cust_num::TEXT
                                    AND (
                                        ecsd.sls_org::TEXT = '3300'::CHARACTER VARYING::TEXT
                                        OR ecsd.sls_org::TEXT = '330B'::CHARACTER VARYING::TEXT
                                        OR ecsd.sls_org::TEXT = '330H'::CHARACTER VARYING::TEXT
                                        OR ecsd.sls_org::TEXT = '3410'::CHARACTER VARYING::TEXT
                                        OR ecsd.sls_org::TEXT = '341B'::CHARACTER VARYING::TEXT
                                    )
                            ) cust,
                            (
                                SELECT sales_derived.wk_end_dt,
                                    sales_derived.sales_grp_nm,
                                    LTRIM(vmd.matl_id::TEXT, '0'::CHARACTER VARYING::TEXT) AS matl_id,
                                    sales_derived.so_qty
                                FROM (
                                        SELECT to_date(iss.wk_end_dt) as wk_end_dt,
                                            iss.sales_grp_desc as sales_grp_nm,
                                            ean.matl_id,
                                            iss.scan_units::NUMERIC(10, 4) AS so_qty
                                        FROM vw_iri_scan_sales iss
                                            LEFT JOIN (
                                                SELECT edw_material_dim_pcf.matl_id,
                                                    edw_material_dim_pcf.bar_cd
                                                FROM edw_material_dim_pcf
                                                WHERE (
                                                        edw_material_dim_pcf.bar_cd IN (
                                                            SELECT DISTINCT derived_table1.bar_cd
                                                            FROM (
                                                                    SELECT COUNT(*) AS COUNT,
                                                                        edw_material_dim_pcf.bar_cd
                                                                    FROM edw_material_dim_pcf
                                                                    GROUP BY edw_material_dim_pcf.bar_cd
                                                                    HAVING COUNT(*) = 1
                                                                ) derived_table1
                                                        )
                                                    )
                                            ) ean ON LTRIM (ean.bar_cd::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM (
                                                COALESCE (iss.iri_ean, '0'::CHARACTER VARYING)::TEXT,
                                                '0'::CHARACTER VARYING::TEXT
                                            )
                                        WHERE (
                                                UPPER(trim(iss.sales_grp_desc::TEXT)) = 'COLES'::CHARACTER VARYING::TEXT
                                                OR UPPER(trim(iss.sales_grp_desc::TEXT)) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT --OR (UPPER(trim(iss.ac_nielsencode)) = 'MCG')
                                            )
                                    ) sales_derived
                                    LEFT JOIN edw_material_dim_pcf vmd ON LTRIM (vmd.matl_id::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM (
                                        COALESCE (sales_derived.matl_id, '0'::CHARACTER VARYING)::TEXT,
                                        '0'::CHARACTER VARYING::TEXT
                                    )
                                UNION ALL
                                SELECT to_date(iss.wk_end_dt) as bill_dt,
                                    iss.sales_grp_desc as sales_grp_nm,
                                    LTRIM(vmd.matl_id::TEXT, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS matl_id,
                                    iss.scan_units::NUMERIC(10, 4) AS so_qty
                                FROM vw_iri_scan_sales iss
                                    LEFT JOIN (
                                        (
                                            SELECT derived_table6.jj_month_id,
                                                derived_table6.bar_cd,
                                                derived_table6.cust_no,
                                                derived_table6.material_id
                                            FROM (
                                                    SELECT DISTINCT derived_table5.jj_month_id,
                                                        derived_table5.bar_cd,
                                                        derived_table5.cust_no,
                                                        derived_table5.material_id
                                                    FROM (
                                                            SELECT DISTINCT derived_table4.jj_month_id,
                                                                derived_table4.master_code,
                                                                derived_table4.bar_cd,
                                                                derived_table4.cust_no,
                                                                derived_table4.material_count,
                                                                derived_table4.gts_val,
                                                                COUNT(DISTINCT derived_table4.master_code) AS COUNT,
                                                                CASE
                                                                    WHEN COUNT(DISTINCT derived_table4.master_code) > 1 THEN CASE
                                                                        WHEN derived_table4.master_code IS NOT NULL
                                                                        AND derived_table4.gts_val >= 0::NUMERIC::NUMERIC(18, 0)
                                                                        AND UPPER(derived_table4.channel_desc::TEXT) <> 'AU - EXPORTS'::CHARACTER VARYING::TEXT THEN derived_table4.max_material_id::CHARACTER VARYING
                                                                        WHEN derived_table4.master_code IS NULL
                                                                        AND derived_table4.gts_val < 0::NUMERIC::NUMERIC(18, 0)
                                                                        AND UPPER(derived_table4.channel_desc::TEXT) = 'AU - EXPORTS'::CHARACTER VARYING::TEXT THEN 'NULL'::CHARACTER VARYING
                                                                        ELSE NULL::CHARACTER VARYING
                                                                    END
                                                                    WHEN COUNT(DISTINCT derived_table4.master_code) = 1 THEN CASE
                                                                        WHEN derived_table4.material_count > 1
                                                                        AND derived_table4.gts_val >= 0::NUMERIC::NUMERIC(18, 0) THEN derived_table4.max_material_id::CHARACTER VARYING
                                                                        WHEN derived_table4.material_count = 1 THEN derived_table4.max_material_id::CHARACTER VARYING
                                                                        ELSE NULL::CHARACTER VARYING
                                                                    END
                                                                    ELSE derived_table4.material_id
                                                                END AS material_id
                                                            FROM (
                                                                    SELECT DISTINCT a.jj_month_id,
                                                                        a.master_code,
                                                                        a.bar_cd,
                                                                        a.matl_id AS material_id,
                                                                        a.cust_no,
                                                                        COUNT(a.matl_id) OVER (
                                                                            PARTITION BY a.jj_month_id,
                                                                            a.bar_cd,
                                                                            a.cust_no
                                                                            order by null rows bETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                        ) AS material_count,
                                                                        "max"(a.matl_id::TEXT) OVER (
                                                                            PARTITION BY a.jj_month_id,
                                                                            a.master_code,
                                                                            a.bar_cd,
                                                                            a.cust_no,
                                                                            SUM(a.gts_val)
                                                                            order by null rows bETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                        ) AS max_material_id,
                                                                        row_number() OVER (
                                                                            PARTITION BY a.jj_month_id,
                                                                            a.bar_cd,
                                                                            a.cust_no
                                                                            ORDER BY SUM(a.gts_val) DESC
                                                                        ) AS sales_rank,
                                                                        COUNT(COALESCE(a.master_code, 'NA'::CHARACTER VARYING)) OVER (
                                                                            PARTITION BY a.jj_month_id,
                                                                            a.bar_cd,
                                                                            a.cust_no,
                                                                            a.matl_id
                                                                            order by null rows bETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                        ) AS master_code_count,
                                                                        SUM(a.gts_val) AS gts_val,
                                                                        a.channel_desc,
                                                                        b.matl_bar_count
                                                                    FROM (
                                                                            SELECT vsf.jj_month_id,
                                                                                vsf.gts_val,
                                                                                vmd.matl_id,
                                                                                vmd.bar_cd,
                                                                                mstrcd.master_code,
                                                                                vcd.cust_no,
                                                                                vcd.channel_desc
                                                                            FROM vw_sapbw_ciw_fact vsf
                                                                                LEFT JOIN vw_customer_dim vcd ON vsf.cust_no::TEXT = vcd.cust_no::TEXT
                                                                                LEFT JOIN edw_material_dim_pcf vmd ON vsf.matl_id::TEXT = vmd.matl_id::TEXT
                                                                                LEFT JOIN (
                                                                                    vw_apo_parent_child_dim vapcd
                                                                                    LEFT JOIN (
                                                                                        SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                                                                            vw_apo_parent_child_dim.parent_matl_desc
                                                                                        FROM vw_apo_parent_child_dim
                                                                                        WHERE vw_apo_parent_child_dim.cmp_id::TEXT = 7470::CHARACTER VARYING::TEXT
                                                                                        UNION ALL
                                                                                        SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                                                                            vw_apo_parent_child_dim.parent_matl_desc
                                                                                        FROM vw_apo_parent_child_dim
                                                                                        WHERE NOT (
                                                                                                vw_apo_parent_child_dim.master_code IN (
                                                                                                    SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                                                                                    FROM vw_apo_parent_child_dim
                                                                                                    WHERE vw_apo_parent_child_dim.cmp_id::TEXT = 7470::CHARACTER VARYING::TEXT
                                                                                                )
                                                                                            )
                                                                                    ) mstrcd ON vapcd.master_code::TEXT = mstrcd.master_code::TEXT
                                                                                ) ON vsf.cmp_id::TEXT = vapcd.cmp_id::TEXT
                                                                                AND vsf.matl_id::TEXT = vapcd.matl_id::TEXT
                                                                        ) a
                                                                        JOIN (
                                                                            SELECT DISTINCT edw_material_dim_pcf.bar_cd,
                                                                                COUNT(DISTINCT edw_material_dim_pcf.matl_id) AS matl_bar_count
                                                                            FROM edw_material_dim_pcf
                                                                            WHERE (
                                                                                    COALESCE(edw_material_dim_pcf.bar_cd, 'NA'::CHARACTER VARYING) IN (
                                                                                        SELECT DISTINCT COALESCE(derived_table3.bar_cd, 'NA'::CHARACTER VARYING) AS "coalesce"
                                                                                        FROM (
                                                                                                SELECT COUNT(*) AS COUNT,
                                                                                                    edw_material_dim_pcf.bar_cd
                                                                                                FROM edw_material_dim_pcf
                                                                                                GROUP BY edw_material_dim_pcf.bar_cd
                                                                                                HAVING COUNT(*) > 1
                                                                                            ) derived_table3
                                                                                    )
                                                                                )
                                                                            GROUP BY edw_material_dim_pcf.bar_cd
                                                                        ) b ON a.bar_cd::TEXT = b.bar_cd::TEXT
                                                                    GROUP BY a.jj_month_id,
                                                                        a.master_code,
                                                                        a.bar_cd,
                                                                        a.matl_id,
                                                                        a.cust_no,
                                                                        a.channel_desc,
                                                                        b.matl_bar_count
                                                                ) derived_table4
                                                            WHERE derived_table4.sales_rank = 1
                                                            GROUP BY derived_table4.jj_month_id,
                                                                derived_table4.master_code,
                                                                derived_table4.bar_cd,
                                                                derived_table4.cust_no,
                                                                derived_table4.material_id,
                                                                derived_table4.material_count,
                                                                derived_table4.max_material_id,
                                                                derived_table4.master_code_count,
                                                                derived_table4.channel_desc,
                                                                derived_table4.gts_val,
                                                                derived_table4.matl_bar_count
                                                        ) derived_table5
                                                ) derived_table6
                                        ) ean
                                        LEFT JOIN edw_material_dim_pcf vmd ON LTRIM (vmd.matl_id::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM (
                                            COALESCE (ean.material_id, '0'::CHARACTER VARYING)::TEXT,
                                            '0'::CHARACTER VARYING::TEXT
                                        )
                                    ) ON LTRIM (ean.cust_no::TEXT, '0'::CHARACTER VARYING::TEXT) = iss.ac_code::TEXT
                                    AND COALESCE (ean.bar_cd, '0'::CHARACTER VARYING)::TEXT = COALESCE (iss.iri_ean, '0'::CHARACTER VARYING)::TEXT
                                WHERE NOT (
                                        COALESCE(iss.iri_ean, 'NA'::CHARACTER VARYING) IN (
                                            SELECT DISTINCT COALESCE(bar_cd_map.iri_ean, '0'::CHARACTER VARYING) AS "coalesce"
                                            FROM (
                                                    SELECT sales_derived.wk_end_dt,
                                                        sales_derived.iri_ean,
                                                        sales_derived.sales_grp_nm,
                                                        LTRIM(vmd.matl_id::TEXT, '0'::CHARACTER VARYING::TEXT) AS matl_id,
                                                        sales_derived.so_qty
                                                    FROM (
                                                            SELECT to_date(iss.wk_end_dt) as wk_end_dt,
                                                                iss.iri_ean,
                                                                iss.sales_grp_desc as sales_grp_nm,
                                                                ean.matl_id,
                                                                iss.scan_units::NUMERIC(10, 4) AS so_qty
                                                            FROM vw_iri_scan_sales iss
                                                                LEFT JOIN (
                                                                    SELECT edw_material_dim_pcf.matl_id,
                                                                        edw_material_dim_pcf.bar_cd
                                                                    FROM edw_material_dim_pcf
                                                                    WHERE (
                                                                            edw_material_dim_pcf.bar_cd IN (
                                                                                SELECT DISTINCT derived_table1.bar_cd
                                                                                FROM (
                                                                                        SELECT COUNT(*) AS COUNT,
                                                                                            edw_material_dim_pcf.bar_cd
                                                                                        FROM edw_material_dim_pcf
                                                                                        GROUP BY edw_material_dim_pcf.bar_cd
                                                                                        HAVING COUNT(*) = 1
                                                                                    ) derived_table1
                                                                            )
                                                                        )
                                                                ) ean ON LTRIM (ean.bar_cd::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM (
                                                                    COALESCE (iss.iri_ean, '0'::CHARACTER VARYING)::TEXT,
                                                                    '0'::CHARACTER VARYING::TEXT
                                                                )
                                                            WHERE (
                                                                    UPPER(trim(iss.sales_grp_desc::TEXT)) = 'COLES'::CHARACTER VARYING::TEXT
                                                                    OR UPPER(trim(iss.sales_grp_desc::TEXT)) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
                                                                )
                                                        ) sales_derived
                                                        LEFT JOIN edw_material_dim_pcf vmd ON LTRIM (vmd.matl_id::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM (
                                                            COALESCE (sales_derived.matl_id, '0'::CHARACTER VARYING)::TEXT,
                                                            '0'::CHARACTER VARYING::TEXT
                                                        )
                                                ) bar_cd_map
                                        )
                                    )
                            ) a
                            LEFT JOIN (
                                SELECT lp.material,
                                    lp.list_price,
                                    b.parameter_value,
                                    (
                                        lp.list_price * b.parameter_value::NUMERIC(10, 4)
                                    )::NUMERIC(10, 4) AS amount
                                FROM (
                                        SELECT LTRIM(
                                                edw_list_price.material::TEXT,
                                                0::CHARACTER VARYING::TEXT
                                            ) AS material,
                                            edw_list_price.amount AS list_price,
                                            row_number() OVER (
                                                PARTITION BY LTRIM(
                                                    edw_list_price.material::TEXT,
                                                    0::CHARACTER VARYING::TEXT
                                                )
                                                ORDER BY TO_DATE(
                                                        edw_list_price.valid_to::TEXT,
                                                        'YYYYMMDD'::CHARACTER VARYING::TEXT
                                                    ) DESC,
                                                    TO_DATE(
                                                        edw_list_price.dt_from::TEXT,
                                                        'YYYYMMDD'::CHARACTER VARYING::TEXT
                                                    ) DESC
                                            ) AS rn
                                        FROM edw_list_price
                                        WHERE edw_list_price.sls_org::TEXT = '3300'::CHARACTER VARYING::TEXT
                                    ) lp,
                                    itg_parameter_reg_inventory b
                                WHERE lp.rn = 1
                                    AND b.country_name::TEXT = 'AUSTRALIA'::CHARACTER VARYING::TEXT
                                    AND b.parameter_name::TEXT = 'listprice'::CHARACTER VARYING::TEXT
                            ) lp ON LTRIM (a.matl_id, 0::CHARACTER VARYING::TEXT) = LTRIM (lp.material, 0::CHARACTER VARYING::TEXT)
                        WHERE UPPER(trim(cust.sap_prnt_cust_desc::TEXT)) = UPPER(trim(a.sales_grp_nm::TEXT))
                        UNION ALL
                        SELECT derived_table1.sap_prnt_cust_key,
                            UPPER(trim(derived_table1.sap_prnt_cust_desc::TEXT))::CHARACTER VARYING AS sap_prnt_cust_desc,
                            derived_table1.cal_date as bill_dt,
                            derived_table1.matl_num,
                            (
                                derived_table1.gross_units * derived_table1.amount
                            )::NUMERIC(16, 4) AS so_value
                        FROM (
                                SELECT a.matl_num,
                                    a.cal_date,
                                    a.gross_units,
                                    lp.amount,
                                    cust.sap_prnt_cust_key,
                                    cust.sap_prnt_cust_desc
                                FROM (
                                        SELECT DISTINCT ecsd.prnt_cust_key AS sap_prnt_cust_key,
                                            cddes_pck.code_desc AS sap_prnt_cust_desc
                                        FROM edw_customer_base_dim ecbd,
                                            edw_customer_sales_dim ecsd
                                            LEFT JOIN edw_code_descriptions cddes_pck ON cddes_pck.code::TEXT = ecsd.prnt_cust_key::TEXT
                                            AND cddes_pck.code_type::TEXT = 'Parent Customer Key'::CHARACTER VARYING::TEXT
                                        WHERE ecsd.cust_num::TEXT = ecbd.cust_num::TEXT
                                            AND (
                                                ecsd.sls_org::TEXT = '3300'::CHARACTER VARYING::TEXT
                                                OR ecsd.sls_org::TEXT = '330B'::CHARACTER VARYING::TEXT
                                                OR ecsd.sls_org::TEXT = '330H'::CHARACTER VARYING::TEXT
                                                OR ecsd.sls_org::TEXT = '3410'::CHARACTER VARYING::TEXT
                                                OR ecsd.sls_org::TEXT = '341B'::CHARACTER VARYING::TEXT
                                            )
                                    ) cust,
                                    (
                                        SELECT b.prod_id AS matl_num,
                                            b.prod_ean AS ean,
                                            to_date(a.cal_date) as cal_date,
                                            a.gross_units::NUMERIC(10, 4) AS gross_units
                                        FROM edw_metcash_ind_grocery_fact a
                                            LEFT JOIN (
                                                SELECT edw_perenso_prod_dim.prod_id,
                                                    edw_perenso_prod_dim.prod_ean,
                                                    edw_perenso_prod_dim.prod_metcash_code
                                                FROM edw_perenso_prod_dim
                                                WHERE edw_perenso_prod_dim.prod_metcash_code::TEXT <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
                                            ) b ON LTRIM (a.product_id::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM (
                                                b.prod_metcash_code::TEXT,
                                                0::CHARACTER VARYING::TEXT
                                            )
                                    ) a
                                    LEFT JOIN (
                                        SELECT lp.material,
                                            lp.list_price,
                                            b.parameter_value,
                                            (
                                                lp.list_price * b.parameter_value::NUMERIC(10, 4)
                                            )::NUMERIC(10, 4) AS amount
                                        FROM (
                                                SELECT LTRIM(
                                                        edw_list_price.material::TEXT,
                                                        0::CHARACTER VARYING::TEXT
                                                    ) AS material,
                                                    edw_list_price.amount AS list_price,
                                                    row_number() OVER (
                                                        PARTITION BY LTRIM(
                                                            edw_list_price.material::TEXT,
                                                            0::CHARACTER VARYING::TEXT
                                                        )
                                                        ORDER BY TO_DATE(
                                                                edw_list_price.valid_to::TEXT,
                                                                'YYYYMMDD'::CHARACTER VARYING::TEXT
                                                            ) DESC,
                                                            TO_DATE(
                                                                edw_list_price.dt_from::TEXT,
                                                                'YYYYMMDD'::CHARACTER VARYING::TEXT
                                                            ) DESC
                                                    ) AS rn
                                                FROM edw_list_price
                                                WHERE edw_list_price.sls_org::TEXT = '3300'::CHARACTER VARYING::TEXT
                                            ) lp,
                                            itg_parameter_reg_inventory b
                                        WHERE lp.rn = 1
                                            AND b.country_name::TEXT = 'AUSTRALIA'::CHARACTER VARYING::TEXT
                                            AND b.parameter_name::TEXT = 'listprice'::CHARACTER VARYING::TEXT
                                    ) lp ON LTRIM (a.matl_num::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM (lp.material, 0::CHARACTER VARYING::TEXT)
                                WHERE UPPER(trim(cust.sap_prnt_cust_desc::TEXT)) = (
                                        (
                                            (
                                                SELECT itg_parameter_reg_inventory.parameter_value
                                                FROM itg_parameter_reg_inventory
                                                WHERE itg_parameter_reg_inventory.parameter_name::TEXT = 'parent_desc_IG'::CHARACTER VARYING::TEXT
                                            )
                                        )::TEXT
                                    )
                            ) derived_table1
                    )
                    UNION ALL
                    SELECT itg_customer_sellout.sap_parent_customer_key,
                        UPPER(
                            trim(
                                itg_customer_sellout.sap_parent_customer_desc::TEXT
                            )
                        )::CHARACTER VARYING AS sap_parent_customer_desc,
                        to_date(itg_customer_sellout.so_date) as so_date,
                        itg_customer_sellout.matl_num,
                        (
                            itg_customer_sellout.so_qty * itg_customer_sellout.std_cost
                        )::NUMERIC(16, 4) AS so_val
                    FROM itg_customer_sellout
                )
            where sap_prnt_cust_desc in (
                    'COLES',
                    'WOOLWORTHS',
                    'METCASH',
                    'SYMBION',
                    'CENTRAL HEALTHCARE SERVICES PTY LTD',
                    'API',
                    'SIGMA'
                )
                and so_value > 0
            UNION ALL
            select cust.sap_prnt_cust_key as sap_prnt_cust_key,
                a.wk_end_dt as bill_dt,
                nvl(a.matl_id, a.lst_sku) as matl_id,
                (scan_units * b.amount)::numeric(10, 4) as so_value
            from vw_iri_scan_sales_analysis a
                left join (
                    SELECT lp.material,
                        lp.list_price,
                        --b.parameter_value,
                        (
                            lp.list_price * b.parameter_value::NUMERIC(10, 4)
                        )::NUMERIC(10, 4) AS amount
                    FROM (
                            SELECT LTRIM(edw_list_price.material, 0) AS material,
                                edw_list_price.amount AS list_price,
                                row_number() OVER (
                                    PARTITION BY LTRIM(edw_list_price.material, '0')
                                    ORDER BY TO_DATE(edw_list_price.valid_to, 'YYYYMMDD') DESC,
                                        TO_DATE(edw_list_price.dt_from, 'YYYYMMDD') DESC
                                ) AS rn
                            FROM edw_list_price
                            WHERE edw_list_price.sls_org = '3300'
                        ) lp,
                        itg_parameter_reg_inventory b
                    WHERE lp.rn = 1
                        AND b.country_name::TEXT = 'AUSTRALIA'
                        AND b.parameter_name::TEXT = 'listprice'
                ) b on nvl(a.matl_id, a.lst_sku)::varchar = b.material::varchar
                join itg_mds_pacific_prod_mapping_cwh cwhprod on nvl(a.matl_id, a.lst_sku)::varchar = cwhprod.matl_num::varchar
                join (
                    SELECT DISTINCT ltrim(ecbd.cust_num, '0') cust_num,
                        ecsd.prnt_cust_key AS sap_prnt_cust_key,
                        cddes_pck.code_desc AS sap_prnt_cust_desc
                    FROM edw_customer_base_dim ecbd,
                        edw_customer_sales_dim ecsd
                        LEFT JOIN edw_code_descriptions cddes_pck ON cddes_pck.code::text = ecsd.prnt_cust_key::text
                        AND cddes_pck.code_type::text = 'Parent Customer Key'::CHARACTER VARYING::text
                    WHERE ecsd.cust_num::text = ecbd.cust_num::text
                        AND (
                            ecsd.sls_org::text = '3300'::CHARACTER VARYING::text
                            OR ecsd.sls_org::text = '330B'::CHARACTER VARYING::text
                            OR ecsd.sls_org::text = '330H'::CHARACTER VARYING::text
                            OR ecsd.sls_org::text = '3410'::CHARACTER VARYING::text
                            OR ecsd.sls_org::text = '341B'::CHARACTER VARYING::text
                        )
                        AND ltrim(ecbd.cust_num, '0') = '133647'
                ) cust on UPPER(trim(cust.sap_prnt_cust_desc::TEXT)) = case
                    when UPPER(trim(a.sales_grp_nm::TEXT)) = 'SIGMA'
                    OR UPPER(trim(a.sales_grp_nm::TEXT)) = 'API' then 'CHEMIST WAREHOUSE'
                    else UPPER(trim(a.sales_grp_nm::TEXT))
                end
            where upper(a.ac_nielsencode) = 'MCG'
                and so_value > 0
        ) T1,
        PRODUCT T3,
        (
            SELECT DISTINCT SAP_PRNT_CUST_KEY
            FROM CUSTOMER
            WHERE RANK = 1
        ) T4
    WHERE LTRIM(T3.SAP_MATL_NUM(+), '0') = ltrim(T1.matl_num, 0)
        AND T4.SAP_PRNT_CUST_KEY(+) = T1.sap_parent_customer_key
        AND left(bill_dt, 4) > (DATE_PART(YEAR, current_date) -6)
    GROUP BY
        TRIM(NVL(NULLIF(T3.GPH_PROD_BRND, ''), 'NA')),
        TRIM(NVL(NULLIF(T3.GPH_PROD_VRNT, ''), 'NA')),
        TRIM(NVL(NULLIF(T3.GPH_PROD_SGMNT, ''), 'NA')),
        TRIM(NVL(NULLIF(T3.GPH_PROD_CTGRY, ''), 'NA')),
        TRIM(NVL(NULLIF(T3.pka_product_key, ''), 'NA')),
        TRIM(NVL(NULLIF(T3.pka_size_desc, ''), 'NA')),
        TRIM(NVL(NULLIF(T4.SAP_PRNT_CUST_KEY, ''), 'NA'))
),
final as
(
    select
        min_date::date as min_date,
        global_prod_brand::varchar(30) as global_prod_brand,
        global_prod_variant::varchar(100) as global_prod_variant,
        global_prod_segment::varchar(50) as global_prod_segment,
        global_prod_category::varchar(50) as global_prod_category,
        pka_product_key::varchar(68) as pka_product_key,
        pka_size_desc::varchar(30) as pka_size_desc,
        sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key
    from transformed
)
select * from final