with edw_vw_greenlight_skus as 
(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
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
edw_sales_org_dim as 
(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_code_descriptions as 
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as 
(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
vw_edw_kr_ecom_dstr_sellout as 
(
    select * from {{ ref('ntaedw_integration__vw_edw_kr_ecom_dstr_sellout') }}
),
edw_otc_inv_si_so as 
(
    select * from {{ ref('ntaedw_integration__edw_otc_inv_si_so') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
product AS (
    Select * from 
    (
        Select distinct EMD.matl_num AS SAP_MATL_NUM,
            EMD.MATL_DESC AS SAP_MAT_DESC,
            EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
            EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
            -- EMD.SAP_BASE_UOM_CD AS SAP_BASE_UOM_CD,
            -- EMD.SAP_PRCHSE_UOM_CD AS SAP_PRCHSE_UOM_CD,
            EMD.PRODH1 AS SAP_PROD_SGMT_CD,
            EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
            --  EMD.SAP_BASE_PROD_CD AS SAP_BASE_PROD_CD,
            EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
            -- EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
            EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
            --  EMD.SAP_BRND_CD AS SAP_BRND_CD,
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
                order by sap_matl_num
            ) rnk
        FROM --(Select * from  edw_vw_greenlight_skus where  sls_org in ('3200','320A','320S','321A') ) EMD
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
                WHERE sls_org in ('3200', '320A', '320S', '321A')
            )
    )
    where rnk = 1
),
customer AS (
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
        REGZONE.REGION_NAME AS REGION,
        REGZONE.ZONE_NAME AS ZONE_OR_AREA,
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
            WHERE SLS_ORG IN ('3200', '320A', '320S', '321A')
            GROUP BY CUST_NUM
        ) A,
        (
            SELECT DISTINCT CUSTOMER_CODE,
                REGION_NAME,
                ZONE_NAME
            FROM EDW_CUSTOMER_DIM
        ) REGZONE
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
        AND ECSD.SLS_ORG IN ('3200', '320A', '320S', '321A')
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
        AND LTRIM(ECSD.CUST_NUM, '0') = REGZONE.CUSTOMER_CODE(+)
),
final as (
    SELECT min(bill_dt) as min_date,
        TRIM(NVL(NULLIF(T3.GPH_PROD_BRND, ''), 'NA')) AS GLOBAL_PROD_BRAND,
        TRIM(NVL(NULLIF(T3.GPH_PROD_VRNT, ''), 'NA')) AS GLOBAL_PROD_VARIANT,
        TRIM(NVL(NULLIF(T3.GPH_PROD_SGMNT, ''), 'NA')) AS GLOBAL_PROD_SEGMENT,
        TRIM(NVL(NULLIF(T3.GPH_PROD_CTGRY, ''), 'NA')) AS GLOBAL_PROD_CATEGORY,
        TRIM(NVL(NULLIF(T3.pka_product_key, ''), 'NA')) AS pka_product_key,
        TRIM(NVL(NULLIF(T3.pka_size_desc, ''), 'NA')) AS pka_size_desc,
        TRIM(
            NVL(NULLIF(T4.SAP_PRNT_CUST_KEY, ''), 'Not Assigned')
        ) AS SAP_PRNT_CUST_KEY
    FROM 
        (
            Select dstr_cd,
                matl_num,
                bill_dt,
                so_trd_sls
            from 
                (
                    with SISO as 
                    (
                        SELECT 'Ecomm' as subj_area,
                            'KR' ctry_cd,
                            'SELLOUT' Data_Type,
                            dstr_cd as dstr_cd,
                            matl_num,
                            so_date as bill_dt,
                            --ean,
                            so_qty as so_qty,
                            0 AS so_value
                        FROM vw_edw_kr_ecom_dstr_sellout so
                        WHERE left(so_date, 4) >=(
                                date_part(
                                    year,
                                    convert_timezone('UTC', current_timestamp())
                                ) -6
                            )
                        UNION ALL
                        ---------------------- OTC product -------------------
                        SELECT 'OTC' as subj_area,
                            'KR' ctry_cd,
                            null as Data_Type,
                            inv.dstr_cd as dstr_cd,
                            inv.prod_cd as matl_num,
                            case
                                when inv.month is null then null
                                else cast(
                                    (
                                        left(inv.month, 4) || '-' || substring(inv.month, 5, 6) || '-01'
                                    ) as date
                                )
                            end as bill_dt,
                            --inv.ean,
                            so_qty as so_qty,
                            so_value as so_value
                        FROM edw_otc_inv_si_so inv
                        WHERE left(inv.month, 4) >=(
                                date_part(
                                    year,
                                    convert_timezone('UTC', current_timestamp())
                                ) -6
                            )
                    ),
                    BILL_FACT as (
                        SELECT material,
                            cust_sls,
                            bill_dt,
                            bill_qty,
                            netval_inv,
                            cast(netval_inv / bill_qty as numeric(15, 4)) InvoicePrice,
                            ROW_NUMBER() OVER (
                                PARTITION BY material,
                                cust_sls
                                ORDER BY bill_dt DESC
                            ) rn
                        FROM edw_billing_fact bill
                        WHERE sls_org in ('3200', '320A', '320S', '321A')
                            AND bill_type = 'ZF2K'
                    )
                    Select subj_area,
                        dstr_cd,
                        t1.matl_num,
                        bill_dt,
                        CASE
                            WHEN upper(subj_area) = 'OTC' THEN T1.so_value
                            ELSE T1.SO_QTY * nvl(t5.amount, t6.amount)
                        END AS SO_TRD_SLS
                    from SISO t1,
                        (
                            SELECT distinct LTRIM(material, 0) material,
                                cust_sls,
                                (cast(InvoicePrice as numeric(10, 4))) AS amount
                            FROM BILL_FACT
                            WHERE rn = 1
                        ) T5,
                        (
                            Select distinct t_msd.matl_num,
                                t_ean.*
                            from (
                                    Select cust_sls,
                                        ean_num,
                                        (cast(price as numeric(10, 4))) as amount
                                    from (
                                            Select ivp.*,
                                                (
                                                    row_number() over (
                                                        partition by ivp.cust_sls
                                                        order by ivp.price,
                                                            ivp.ean_num desc
                                                    )
                                                ) rn
                                            from (
                                                    Select distinct msd.matl_num,
                                                        msd.ean_num,
                                                        bill.*
                                                    from edw_material_sales_dim msd,
                                                        (
                                                            SELECT distinct LTRIM(material, 0) material,
                                                                cust_sls,
                                                                InvoicePrice AS price
                                                            FROM BILL_FACT
                                                            WHERE rn = 1
                                                        ) bill
                                                    WHERE msd.sls_org in ('3200', '320A', '320S', '321A')
                                                        AND LTRIM(msd.matl_num, '0') = LTRIM(bill.material, '0')
                                                        and ean_num != ''
                                                ) ivp
                                        ) t_ivp
                                    where rn = 1 --group by ean_num
                                ) t_ean,
                                edw_material_sales_dim t_msd
                            where t_msd.ean_num = t_ean.ean_num
                                and t_msd.sls_org in ('3200', '320A', '320S', '321A')
                        ) t6
                    where ltrim(T1.dstr_cd, 0) = ltrim(t5.cust_sls(+), 0)
                        AND Ltrim(T1.matl_num, 0) = ltrim(T5.material(+), 0)
                        AND Ltrim(T1.matl_num, 0) = ltrim(T6.matl_num(+), 0)
                        AND ltrim(T1.dstr_cd, 0) = ltrim(T6.cust_sls(+), 0)
                )
            where so_trd_sls > 0
        ) T1,
        PRODUCT T3,
        (
            SELECT *
            FROM CUSTOMER
            where RANK = 1
        ) T4
    WHERE LTRIM(T3.SAP_MATL_NUM(+), '0') = T1.matl_num
        AND LTRIM(T4.SAP_CUST_ID(+), '0') = T1.dstr_cd
    GROUP BY T3.GPH_PROD_BRND,
        T3.GPH_PROD_VRNT,
        T3.GPH_PROD_SGMNT,
        T3.GPH_PROD_CTGRY,
        T3.pka_size_desc,
        T3.pka_product_key,
        T4.SAP_PRNT_CUST_KEY
)
select * from final