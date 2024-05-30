with 
wks_indonesia_siso_propagate_final as 
(
    select * from idnwks_integration.wks_indonesia_siso_propagate_final
),
edw_indonesia_lppb_analysis as 
(
    select * from idnedw_integration.edw_indonesia_lppb_analysis
),
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_product_dim as 
(
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
edw_vw_os_material_dim as 
(
    select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
),
edw_vw_greenlight_skus as 
(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_vw_os_customer_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_customer_dim') }}
),
edw_material_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
vw_edw_reg_exch_rate as 
(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
edw_distributor_dim as 
(
    select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
wks_indonesia_inventory_health_analysis_propagation_prestep as 
(
    select * from idnwks_integration__wks_indonesia_inventory_health_analysis_propagation_prestep
),
v_edw_customer_sales_dim as 
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
edw_company_dim as 
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_copa_trans_fact as 
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
trans as
(
Select *
from (
        with Regional as (
            WITH ONSESEA AS (
                SELECT TMD.YEAR,
                    CAST(TMD.QRTR_NO AS VARCHAR) as QRTR_NO,
                    CAST(TMD.MNTH_ID AS VARCHAR) AS month_year,
                    TMD.MNTH_NO,
                    CAST('Indonesia' AS VARCHAR) AS country_name,
                    CASE
                        WHEN A.sap_parent_customer_key IS NULL THEN 'NA'
                        WHEN A.sap_parent_customer_key = '' THEN 'NA'
                        ELSE TRIM(A.sap_parent_customer_key)
                    END AS dstrbtr_grp_cd,
                    A.jj_sap_dstrbtr_nm || ' - ' || A.sap_parent_customer_key as dstrbtr_grp_name,
                    CASE
                        WHEN EGPH.GPH_PROD_FRNCHSE IS NULL THEN 'NA'
                        WHEN EGPH.GPH_PROD_FRNCHSE = '' THEN 'NA'
                        ELSE TRIM(EGPH.GPH_PROD_FRNCHSE)
                    END AS GPH_PROD_FRNCHSE,
                    CASE
                        WHEN EGPH.GPH_PROD_BRND IS NULL THEN 'NA'
                        WHEN EGPH.GPH_PROD_BRND = '' THEN 'NA'
                        ELSE TRIM(EGPH.GPH_PROD_BRND)
                    END AS GPH_PROD_BRND,
                    CASE
                        WHEN EGPH.GPH_PROD_SUB_BRND IS NULL THEN 'NA'
                        WHEN EGPH.GPH_PROD_SUB_BRND = '' THEN 'NA'
                        ELSE TRIM(EGPH.GPH_PROD_SUB_BRND)
                    END AS GPH_PROD_SUB_BRND,
                    CASE
                        WHEN EGPH.GPH_PROD_VRNT IS NULL THEN 'NA'
                        WHEN EGPH.GPH_PROD_VRNT = '' THEN 'NA'
                        ELSE TRIM(EGPH.GPH_PROD_VRNT)
                    END AS GPH_PROD_VRNT,
                    CASE
                        WHEN EGPH.GPH_PROD_SGMNT IS NULL THEN 'NA'
                        WHEN EGPH.GPH_PROD_SGMNT = '' THEN 'NA'
                        ELSE TRIM(EGPH.GPH_PROD_SGMNT)
                    END AS GPH_PROD_SGMNT,
                    CASE
                        WHEN EGPH.GPH_PROD_SUBSGMNT IS NULL THEN 'NA'
                        WHEN EGPH.GPH_PROD_SUBSGMNT = '' THEN 'NA'
                        ELSE TRIM(EGPH.GPH_PROD_SUBSGMNT)
                    END AS GPH_PROD_SUBSGMNT,
                    CASE
                        WHEN EGPH.GPH_PROD_CTGRY IS NULL THEN 'NA'
                        WHEN EGPH.GPH_PROD_CTGRY = '' THEN 'NA'
                        ELSE TRIM(EGPH.GPH_PROD_CTGRY)
                    END AS GPH_PROD_CTGRY,
                    CASE
                        WHEN EGPH.GPH_PROD_SUBCTGRY IS NULL THEN 'NA'
                        WHEN EGPH.GPH_PROD_SUBCTGRY = '' THEN 'NA'
                        ELSE TRIM(EGPH.GPH_PROD_SUBCTGRY)
                    END AS GPH_PROD_SUBCTGRY,
                    TRIM(NVL(NULLIF(EGPH.pka_size_desc, ''), 'NA')) AS pka_size_desc,
                    CASE
                        WHEN A.matl_num IS NULL THEN 'NA'
                        WHEN A.matl_num = '' THEN 'NA'
                        ELSE TRIM(A.matl_num)
                    END AS SKU_CD,
                    CASE
                        WHEN details.JJ_SAP_CD_MP_PROD_DESC IS NULL THEN 'NA'
                        WHEN details.JJ_SAP_CD_MP_PROD_DESC = '' THEN 'NA'
                        ELSE TRIM(details.JJ_SAP_CD_MP_PROD_DESC)
                    END AS SKU_DESCRIPTION,
                    TRIM(NVL(NULLIF(EGPH.pka_product_key, ''), 'NA')) AS pka_product_key,
                    TRIM(
                        NVL(NULLIF(EGPH.pka_product_key_description, ''), 'NA')
                    ) AS pka_product_key_description,
                    TRIM(NVL(NULLIF(EGPH.product_key, ''), 'NA')) AS product_key,
                    TRIM(
                        NVL(NULLIF(EGPH.product_key_description, ''), 'NA')
                    ) AS product_key_description,
                    CAST('IDR' AS VARCHAR) AS FROM_CCY,
                    'USD' AS TO_CCY,
                    EXCH_RATE,
                    CASE
                        WHEN CUST_HIER.SAP_PRNT_CUST_KEY IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_PRNT_CUST_KEY = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_PRNT_CUST_KEY)
                    END AS SAP_PRNT_CUST_KEY,
                    CASE
                        WHEN A.sap_parent_customer_desc IS NULL THEN 'NA'
                        WHEN A.sap_parent_customer_desc = '' THEN 'NA'
                        ELSE TRIM(A.sap_parent_customer_desc)
                    END AS SAP_PRNT_CUST_DESC,
                    CASE
                        WHEN CUST_HIER.SAP_CUST_CHNL_KEY IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_CUST_CHNL_KEY = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_CUST_CHNL_KEY)
                    END AS SAP_CUST_CHNL_KEY,
                    CASE
                        WHEN CUST_HIER.SAP_CUST_CHNL_DESC IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_CUST_CHNL_DESC = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_CUST_CHNL_DESC)
                    END SAP_CUST_CHNL_DESC,
                    CASE
                        WHEN CUST_HIER.SAP_CUST_SUB_CHNL_KEY IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_CUST_SUB_CHNL_KEY = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_CUST_SUB_CHNL_KEY)
                    END AS SAP_CUST_SUB_CHNL_KEY,
                    CASE
                        WHEN CUST_HIER.SAP_SUB_CHNL_DESC IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_SUB_CHNL_DESC = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_SUB_CHNL_DESC)
                    END AS SAP_SUB_CHNL_DESC,
                    CASE
                        WHEN CUST_HIER.SAP_GO_TO_MDL_KEY IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_GO_TO_MDL_KEY = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_GO_TO_MDL_KEY)
                    END AS SAP_GO_TO_MDL_KEY,
                    CASE
                        WHEN CUST_HIER.SAP_GO_TO_MDL_DESC IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_GO_TO_MDL_DESC = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_GO_TO_MDL_DESC)
                    END AS SAP_GO_TO_MDL_DESC,
                    CASE
                        WHEN CUST_HIER.SAP_BNR_KEY IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_BNR_KEY = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_BNR_KEY)
                    END AS SAP_BNR_KEY,
                    CASE
                        WHEN CUST_HIER.SAP_BNR_DESC IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_BNR_DESC = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_BNR_DESC)
                    END AS SAP_BNR_DESC,
                    CASE
                        WHEN CUST_HIER.SAP_BNR_FRMT_KEY IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_BNR_FRMT_KEY = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_BNR_FRMT_KEY)
                    END AS SAP_BNR_FRMT_KEY,
                    CASE
                        WHEN CUST_HIER.SAP_BNR_FRMT_DESC IS NULL THEN 'NA'
                        WHEN CUST_HIER.SAP_BNR_FRMT_DESC = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.SAP_BNR_FRMT_DESC)
                    END AS SAP_BNR_FRMT_DESC,
                    CASE
                        WHEN CUST_HIER.RETAIL_ENV IS NULL THEN 'NA'
                        WHEN CUST_HIER.RETAIL_ENV = '' THEN 'NA'
                        ELSE TRIM(CUST_HIER.RETAIL_ENV)
                    END AS RETAIL_ENV,
                    'INDONESIA' AS REGION,
                    'INDONESIA' AS ZONE_OR_AREA,
                    SUM(last_3months_so) AS last_3months_so_qty,
                    SUM(last_6months_so) AS last_6months_so_qty,
                    SUM(last_12months_so) AS last_12months_so_qty,
                    SUM(last_3months_so_value) AS last_3months_so_val,
                    SUM(last_6months_so_value) AS last_6months_so_val,
                    SUM(last_12months_so_value) AS last_12months_so_val,
                    SUM(last_36months_so_value) AS last_36months_so_val,
                    CAST(
                        (SUM(last_3months_so_value * Exch_rate) / 1000) AS NUMERIC(38, 5)
                    ) AS last_3months_so_val_usd,
                    CAST(
                        (SUM(last_6months_so_value * Exch_rate) / 1000) AS NUMERIC(38, 5)
                    ) AS last_6months_so_val_usd,
                    CAST(
                        (SUM(last_12months_so_value * Exch_rate) / 1000) AS NUMERIC(38, 5)
                    ) AS last_12months_so_val_usd,
                    propagate_flag,
                    propagate_from,
                    CASE
                        WHEN propagate_flag = 'N' THEN 'Not propagate'
                        ELSE reason
                    END AS reason,
                    replicated_flag,
                    SUM(sell_in_qty) AS SI_SLS_QTY,
                    SUM(sell_in_value) AS SI_GTS_VAL,
                    SUM(sell_in_value * EXCH_RATE) / 1000 AS SI_GTS_VAL_USD,
                    SUM(inv_qty) AS INVENTORY_QUANTITY,
                    SUM(inv_value) AS INVENTORY_VAL,
                    SUM(inv_value * EXCH_RATE) / 1000 AS INVENTORY_VAL_USD,
                    SUM(SO_QTY) AS SO_SLS_QTY,
                    SUM(SO_VALue) AS SO_grs_TRD_SLS,
                    ROUND(SUM(SO_VALUE * EXCH_RATE) / 1000) AS SO_grs_TRD_SLS_USD
                FROM (
                        SELECT *
                        FROM wks_Indonesia_siso_propagate_final
                    ) AS A
                    LEFT JOIN (
                        SELECT DISTINCT JJ_MNTH_ID,
                            DSTRBTR_GRP_CD,
                            JJ_SAP_CD_MP_PROD_ID,
                            JJ_SAP_CD_MP_PROD_DESC,
                            DSTRBTR_GRP_NM
                        FROM EDW_INDONESIA_LPPB_ANALYSIS
                        WHERE JJ_SAP_CD_MP_PROD_ID != '33514660'
                    ) details ON A.month = details.jj_mnth_id 
                    AND A.sap_parent_customer_key = details.DSTRBTR_GRP_CD 
                    AND A.matl_num = details.JJ_SAP_CD_MP_PROD_ID 
                    LEFT JOIN (
                        SELECT DISTINCT "year" as year,
                            QRTR_NO,
                            MNTH_ID,
                            MNTH_NO
                        FROM EDW_VW_OS_TIME_DIM
                    ) AS TMD ON A.month = TMD.MNTH_ID
                    LEFT JOIN (
                        Select *
                        from (
                                Select sap_matl_num,
                                    gph_prod_frnchse,
                                    gph_prod_brnd,
                                    gph_prod_sub_brnd,
                                    gph_prod_vrnt,
                                    gph_prod_sgmnt,
                                    gph_prod_subsgmnt,
                                    gph_prod_ctgry,
                                    gph_prod_subctgry,
                                    EMD.pka_product_key as pka_product_key,
                                    EMD.pka_product_key_description as pka_product_key_description,
                                    EMD.pka_product_key as product_key,
                                    EMD.pka_product_key_description as product_key_description,
                                    EMD.pka_size_desc as pka_size_desc,
                                    row_number () over(
                                        partition by sap_matl_num
                                        order by sap_matl_num,
                                            effective_to desc
                                    ) as rnk
                                from (
                                        SELECT DISTINCT sap_matl_num,
                                            GPH_PROD_FRNCHSE,
                                            GPH_PROD_BRND,
                                            GPH_PROD_SUB_BRND,
                                            GPH_PROD_VRNT,
                                            GPH_PROD_SGMNT,
                                            GPH_PROD_SUBSGMNT,
                                            GPH_PROD_CTGRY,
                                            GPH_PROD_SUBCTGRY,
                                            '999912' as effective_to
                                        FROM EDW_VW_OS_MATERIAL_DIM
                                        WHERE CNTRY_KEY = 'ID'
                                        UNION ALL
                                        SELECT DISTINCT jj_sap_prod_id,
                                            franchise,
                                            brand AS brand,
                                            NULL AS sub_brand,
                                            variant3 AS variant,
                                            variant2 AS Segment,
                                            NULL AS sub_segment,
                                            NULL AS category,
                                            NULL AS sub_Category,
                                            effective_to
                                        FROM edw_product_dim
                                        WHERE LTRIM(jj_sap_prod_id, 0) NOT IN (
                                                SELECT LTRIM(sap_matl_num, 0)
                                                FROM EDW_VW_OS_MATERIAL_DIM
                                                WHERE CNTRY_KEY = 'ID'
                                            )
                                    ) product
                                    left join 
                                    edw_material_dim EMD on ltrim(product.sap_matl_num, 0) = LTRIM(EMD.MATL_NUM, '0')
                            )
                        where rnk = 1
                    ) EGPH ON UPPER (LTRIM (A.matl_num, 0)) = UPPER (LTRIM (SAP_MATL_NUM, 0))
                    LEFT JOIN 
                    (
                        SELECT *
                        FROM vw_edw_reg_exch_rate
                        WHERE cntry_key = 'ID'
                            AND TO_CCY = 'USD'
                            AND JJ_MNTH_ID = (
                                SELECT MAX(JJ_MNTH_ID)
                                FROM vw_edw_reg_exch_rate
                            )
                    ) ID_CUR ON ID_CUR.FROM_CCY = 'IDR'
                    LEFT JOIN (
                        SELECT DSTRBTR_GRP_CD,
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
                            RETAIL_ENV
                        FROM (
                                SELECT *,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY DSTRBTR_GRP_CD
                                        ORDER BY SAP_PRNT_CUST_KEY DESC
                                    ) AS RNUM
                                FROM (
                                        SELECT DISTINCT T2.DSTRBTR_GRP_CD,
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
                                            RETAIL_ENV
                                        FROM (
                                                SELECT *
                                                FROM EDW_VW_OS_CUSTOMER_DIM
                                                WHERE SAP_CNTRY_CD = 'ID'
                                            ) AS T1,
                                            EDW_DISTRIBUTOR_DIM AS T2
                                        WHERE LTRIM(T2.JJ_SAP_DSTRBTR_ID, '0') = LTRIM(T1.SAP_CUST_ID, '0')
                                    )
                            )
                        WHERE RNUM = 1
                    ) AS CUST_HIER ON UPPER (LTRIM (A.sap_parent_customer_key, '0')) = UPPER (LTRIM (CUST_HIER.DSTRBTR_GRP_CD, '0'))
                GROUP BY TMD.YEAR,
                    TMD.QRTR_NO,
                    TMD.MNTH_ID,
                    TMD.MNTH_NO,
                    A.sap_parent_customer_key,
                    A.jj_sap_dstrbtr_nm,
                    A.sap_parent_customer_desc,
                    EGPH.GPH_PROD_FRNCHSE,
                    EGPH.GPH_PROD_BRND,
                    EGPH.GPH_PROD_SUB_BRND,
                    EGPH.GPH_PROD_VRNT,
                    EGPH.GPH_PROD_SGMNT,
                    EGPH.GPH_PROD_SUBSGMNT,
                    EGPH.GPH_PROD_CTGRY,
                    EGPH.GPH_PROD_SUBCTGRY,
                    --EGPH.GPH_PROD_PUT_UP_DESC,
                    pka_size_desc,
                    A.matl_num,
                    details.JJ_SAP_CD_MP_PROD_DESC,
                    --greenlight_sku_flag,
                    pka_product_key,
                    pka_product_key_description,
                    product_key,
                    product_key_description,
                    EXCH_RATE,
                    CUST_HIER.SAP_PRNT_CUST_KEY,
                    A.sap_parent_customer_desc,
                    CUST_HIER.SAP_CUST_CHNL_KEY,
                    CUST_HIER.SAP_CUST_CHNL_DESC,
                    CUST_HIER.SAP_CUST_SUB_CHNL_KEY,
                    CUST_HIER.SAP_SUB_CHNL_DESC,
                    CUST_HIER.SAP_GO_TO_MDL_KEY,
                    CUST_HIER.SAP_GO_TO_MDL_DESC,
                    CUST_HIER.SAP_BNR_KEY,
                    CUST_HIER.SAP_BNR_DESC,
                    CUST_HIER.SAP_BNR_FRMT_KEY,
                    CUST_HIER.SAP_BNR_FRMT_DESC,
                    CUST_HIER.RETAIL_ENV,
                    propagate_flag,
                    propagate_from,
                    reason,
                    replicated_flag
            )
            SELECT *,
                SUM(SI_GTS_VAL) OVER (PARTITION BY COUNTRY_NAME, YEAR, MONTH_YEAR) AS SI_INV_DB_VAL,
                SUM(SI_GTS_VAL_USD) OVER (PARTITION BY COUNTRY_NAME, YEAR, MONTH_YEAR) AS SI_INV_DB_VAL_USD
            FROM ONSESEA
            WHERE COUNTRY_NAME || SAP_PRNT_CUST_DESC IN (
                    SELECT COUNTRY_NAME || SAP_PRNT_CUST_DESC AS INCLUSION
                    FROM (
                            SELECT COUNTRY_NAME,
                                SAP_PRNT_CUST_DESC,
                                NVL(SUM(INVENTORY_VAL), 0) AS INV_VAL,
                                NVL(SUM(SO_GRS_TRD_SLS), 0) as Sellout_val
                            FROM ONSESEA
                            WHERE SAP_PRNT_CUST_DESC IS NOT NULL
                            GROUP BY COUNTRY_NAME,
                                SAP_PRNT_CUST_DESC
                            HAVING INV_VAL <> 0
                        )
                )
        ),
        COPA AS (
            WITH RegionalCurrency AS (
                Select cntry_key,
                    cntry_nm,
                    rate_type,
                    from_ccy,
                    to_ccy,
                    valid_date,
                    jj_year,
                    jj_mnth_id as MNTH_ID,
(cast(EXCH_RATE as numeric(15, 5))) as EXCH_RATE
                FROM vw_edw_reg_exch_rate
                where cntry_key = 'ID'
                    and jj_mnth_id >= (DATE_PART(YEAR, current_timestamp()) -2)
                    and to_ccy = 'USD'
            ),
            GTS as (
                Select ctry_key,
                    obj_crncy_co_obj,
                    caln_yr_mo,
                    fisc_yr,
                    sum(SI_ALL_DB_VAL) as gts_value,
                    sum(
                        case
                            when avail_customer is null then 0
                            else si_all_db_val
                        end
                    ) as si_inv_db_val
                from(
                        with sellin_all as (
                            Select ctry_key,
                                obj_crncy_co_obj,
                                prnt_cust_key,
                                caln_yr_mo,
                                fisc_yr,
(cast(gts as numeric(38, 15))) as gts
                            from (
                                    select copa.ctry_key as ctry_key,
                                        obj_crncy_co_obj,
                                        cus_sales_extn.prnt_cust_key,
                                        substring(fisc_yr_per, 1, 4) || substring(fisc_yr_per, 6, 2) as caln_yr_mo,
                                        fisc_yr,
                                        SUM(amt_obj_crncy) AS gts
                                    from edw_copa_trans_fact copa
                                        LEFT JOIN edw_company_dim cmp ON copa.co_cd = cmp.co_cd
                                        LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON copa.sls_org = cus_sales_extn.sls_org
                                        AND copa.dstr_chnl = cus_sales_extn.dstr_chnl::TEXT
                                        AND copa.div = cus_sales_extn.div
                                        AND copa.cust_num = cus_sales_extn.cust_num
                                    WHERE cmp.ctry_group = 'Indonesia'
                                        and left(fisc_yr_per, 4) >= (DATE_PART(YEAR, current_timestamp()) -2)
                                        and copa.cust_num is not null
                                        and copa.acct_hier_shrt_desc = 'GTS'
                                        and amt_obj_crncy > 0
                                    group by 1,
                                        2,
                                        3,
                                        4,
                                        5
                                )
                        ),
                        available_customers as (
                            select month_year,
                                country_name,
                                sap_prnt_cust_key,
                                sap_prnt_cust_desc,
                                sum(si_gts_val) as si_gts_val,
                                sum(si_sls_qty) as si_sls_qty
                            from wks_indonesia_inventory_health_analysis_propagation_prestep inv
                            where country_name in ('Indonesia')
                            group by 1,
                                2,
                                3,
                                4
                            having (
                                    sum(inventory_quantity) <> 0
                                    or sum(inventory_val) <> 0
                                )
                            order by 1 desc,
                                2,
                                3,
                                4
                        )
                        select a.ctry_key,
                            a.obj_crncy_co_obj,
                            a.caln_yr_mo,
                            a.fisc_yr,
                            a.prnt_cust_key as total_customer,
                            b.sap_prnt_cust_key as avail_customer,
                            sum(gts) as SI_ALL_DB_VAL
                        from sellin_all a
                            left join available_customers b on b.month_year = a.caln_yr_mo
                            and a.prnt_cust_key = b.sap_prnt_cust_key
                        group by 1,
                            2,
                            3,
                            4,
                            5,
                            6
                        order by 1 desc,
                            2,
                            3,
                            4
                    )
                group by 1,
                    2,
                    3,
                    4
            )
            Select ctry_key,
                obj_crncy_co_obj,
                caln_yr_mo,
                fisc_yr,
(cast (gts_value as numeric(38, 5))) as gts,
                si_inv_db_val,
                Case
                    when ctry_key = 'ID' then cast((gts_value * exch_rate) / 1000 as numeric(38, 5))
                end as GTS_USD,
                case
                    when ctry_key = 'ID' then cast((si_inv_db_val * exch_rate) / 1000 as numeric(38, 5))
                end as si_inv_db_val_usd
            FROM gts,
                RegionalCurrency
            WHERE GTS.obj_crncy_co_obj = RegionalCurrency.from_ccy
                AND RegionalCurrency.MNTH_ID =(
                    Select max(MNTH_ID)
                    from RegionalCurrency
                )
        )
        Select year,
            qrtr_no as year_quarter,
            month_year,
            mnth_no as month_number,
            country_name,
            dstrbtr_grp_cd,
            dstrbtr_grp_name,
            GPH_PROD_FRNCHSE as franchise,
            GPH_PROD_BRND as brand,
            GPH_PROD_SUB_BRND as prod_sub_brand,
            GPH_PROD_VRNT as variant,
            GPH_PROD_SGMNT as segment,
            GPH_PROD_SUBSGMNT as prod_subsegment,
            GPH_PROD_CTGRY as prod_category,
            GPH_PROD_SUBCTGRY as prod_subcategory,
            --GPH_PROD_PUT_UP_DESC as put_up_description,
            pka_size_desc as put_up_description,
            sku_cd,
            sku_description,
            --greenlight_sku_flag,
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
            round(cast(si_sls_qty as numeric(38, 5)), 5) as si_sls_qty,
            round(cast(si_gts_val as numeric (38, 5)), 5) as si_gts_val,
            round(cast(si_gts_val_usd as numeric(38, 5)), 5) as si_gts_val_usd,
            round(cast (inventory_quantity as numeric(38, 5)), 5) as inventory_quantity,
            round(cast(inventory_val as numeric(38, 5)), 5) as inventory_val,
            round(cast (inventory_val_usd as numeric(38, 5)), 5) as inventory_val_usd,
            round(cast (so_sls_qty as numeric(38, 5)), 5) as so_sls_qty,
            round(cast (so_grs_trd_sls as numeric(38, 5)), 5) as so_grs_trd_sls,
            so_grs_trd_sls_usd as so_grs_trd_sls_usd,
            round(cast (COPA.gts as numeric(38, 5)), 5) as SI_ALL_DB_VAL,
            round(cast (COPA.gts_usd as numeric (38, 5)), 5) as SI_ALL_DB_VAL_USD,
            round(cast (COPA.si_inv_db_val as numeric(38, 5)), 5) as si_inv_db_val,
            round(cast (COPA.si_inv_db_val_usd as numeric(38, 5)), 5) as si_inv_db_val_usd,
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
        from Regional,
            COPA
        where Regional.year = COPA.fisc_yr
            and Regional.month_year = COPA.caln_yr_mo
            and Regional.from_ccy = COPA.obj_crncy_co_obj
    )
),
final as 
(
    select 
     year::integer as year,
    year_quarter::varchar(11) as year_quarter,
    month_year::varchar(23) as month_year,
    month_number::integer as month_number,
    country_name::varchar(9) as country_name,
    dstrbtr_grp_cd::varchar(25) as dstrbtr_grp_cd,
    dstrbtr_grp_name::varchar(103) as dstrbtr_grp_name,
    franchise::varchar(50) as franchise,
    brand::varchar(50) as brand,
    prod_sub_brand::varchar(100) as prod_sub_brand,
    variant::varchar(100) as variant,
    segment::varchar(50) as segment,
    prod_subsegment::varchar(100) as prod_subsegment,
    prod_category::varchar(50) as prod_category,
    prod_subcategory::varchar(50) as prod_subcategory,
    put_up_description::varchar(30) as put_up_description,
    sku_cd::varchar(50) as sku_cd,
    sku_description::varchar(100) as sku_description,
    pka_product_key::varchar(68) as pka_product_key,
    pka_product_key_description::varchar(255) as pka_product_key_description,
    product_key::varchar(68) as product_key,
    product_key_description::varchar(255) as product_key_description,
    from_ccy::varchar(3) as from_ccy,
    to_ccy::varchar(3) as to_ccy,
    exch_rate::numeric(15,5) as exch_rate,
    sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
    sap_prnt_cust_desc::varchar(155) as sap_prnt_cust_desc,
    sap_cust_chnl_key::varchar(12) as sap_cust_chnl_key,
    sap_cust_chnl_desc::varchar(50) as sap_cust_chnl_desc,
    sap_cust_sub_chnl_key::varchar(12) as sap_cust_sub_chnl_key,
    sap_sub_chnl_desc::varchar(50) as sap_sub_chnl_desc,
    sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
    sap_go_to_mdl_desc::varchar(50) as sap_go_to_mdl_desc,
    sap_bnr_key::varchar(12) as sap_bnr_key,
    sap_bnr_desc::varchar(50) as sap_bnr_desc,
    sap_bnr_frmt_key::varchar(12) as sap_bnr_frmt_key,
    sap_bnr_frmt_desc::varchar(50) as sap_bnr_frmt_desc,
    retail_env::varchar(50) as retail_env,
    region::varchar(9) as region,
    zone_or_area::varchar(9) as zone_or_area,
    si_sls_qty::numeric(38,5) as si_sls_qty,
    si_gts_val::numeric(38,5) as si_gts_val,
    si_gts_val_usd::numeric(38,5) as si_gts_val_usd,
    inventory_quantity::numeric(38,5) as inventory_quantity,
    inventory_val::numeric(38,5) as inventory_val,
    inventory_val_usd::numeric(38,5) as inventory_val_usd,
    so_sls_qty::numeric(38,5) as so_sls_qty,
    so_grs_trd_sls::numeric(38,5) as so_grs_trd_sls,
    so_grs_trd_sls_usd::numeric(30,0) as so_grs_trd_sls_usd,
    si_all_db_val::numeric(38,5) as si_all_db_val,
    si_all_db_val_usd::numeric(38,5) as si_all_db_val_usd,
    si_inv_db_val::numeric(38,5) as si_inv_db_val,
    si_inv_db_val_usd::numeric(38,5) as si_inv_db_val_usd,
    last_3months_so_qty::numeric(38,4) as last_3months_so_qty,
    last_6months_so_qty::numeric(38,4) as last_6months_so_qty,
    last_12months_so_qty::numeric(38,4) as last_12months_so_qty,
    last_3months_so_val::numeric(38,4) as last_3months_so_val,
    last_3months_so_val_usd::numeric(38,5) as last_3months_so_val_usd,
    last_6months_so_val::numeric(38,4) as last_6months_so_val,
    last_6months_so_val_usd::numeric(38,5) as last_6months_so_val_usd,
    last_12months_so_val::numeric(38,4) as last_12months_so_val,
    last_12months_so_val_usd::numeric(38,5) as last_12months_so_val_usd,
    propagate_flag::varchar(1) as propagate_flag,
    propagate_from::integer as propagate_from,
    reason::varchar(100) as reason,
    last_36months_so_val::numeric(38,4) as last_36months_so_val
     from trans
)
select * from final
