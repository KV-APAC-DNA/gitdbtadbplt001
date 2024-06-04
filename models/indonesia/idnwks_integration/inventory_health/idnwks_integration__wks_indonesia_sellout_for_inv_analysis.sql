with 
itg_all_distributor_sellout_sales_fact as 
(
    select * from {{ ref('idnitg_integration__itg_all_distributor_sellout_sales_fact') }}
),
itg_all_non_ivy_distributor_sellout_sales_fact as 
(
    select * from {{ source('idnitg_integration', 'itg_all_non_ivy_distributor_sellout_sales_fact') }}
),
itg_all_ivy_distributor_sellout_sales_fact as 
(
    select * from {{ ref('idnitg_integration__itg_all_ivy_distributor_sellout_sales_fact') }}
),
edw_distributor_dim as 
(
    select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
itg_all_distributor_sellin_sales_fact as 
(
    select * from {{ ref('idnitg_integration__itg_all_distributor_sellin_sales_fact') }}
),
edw_time_dim as 
(
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
edw_material_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_product_dim as 
(
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
edw_vw_os_material_dim as 
(
    select * from {{ ref('idnedw_integration__edw_vw_id_material_dim') }}
),
edw_vw_greenlight_skus as 
(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_vw_os_customer_dim
as (
    select * from {{ ref('idnedw_integration__edw_vw_id_customer_dim') }}
),
trans as 
(
    SELECT A.JJ_SAP_DSTRBTR_NM || ' - ' || A.DSTRBTR_GRP_CD as dstrbtr_grp_name,
    A.DSTRBTR_GRP_CD,
    min(bill_dt) as min_date,
    CASE
        WHEN CUST_HIER.SAP_PRNT_CUST_KEY IS NULL THEN 'NA'
        WHEN CUST_HIER.SAP_PRNT_CUST_KEY = '' THEN 'NA'
        ELSE TRIM(CUST_HIER.SAP_PRNT_CUST_KEY)
    END AS SAP_PRNT_CUST_KEY,
    TRIM(NVL(NULLIF(EGPH.pka_size_desc, ''), 'NA')) AS pka_size_desc,
    TRIM(NVL(NULLIF(EGPH.pka_product_key, ''), 'NA')) AS pka_product_key,
    CASE
        WHEN EGPH.GPH_PROD_BRND IS NULL THEN 'NA'
        WHEN EGPH.GPH_PROD_BRND = '' THEN 'NA'
        ELSE TRIM(EGPH.GPH_PROD_BRND)
    END AS GPH_PROD_BRND,
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
        WHEN EGPH.GPH_PROD_CTGRY IS NULL THEN 'NA'
        WHEN EGPH.GPH_PROD_CTGRY = '' THEN 'NA'
        ELSE TRIM(EGPH.GPH_PROD_CTGRY)
    END AS GPH_PROD_CTGRY
FROM (
        SELECT nvl(nullif(EPD.JJ_SAP_CD_MP_PROD_ID, ''), 'NA') as matl_num,
            bill_dt,
            T1.DSTRBTR_GRP_CD,
            nvl(nullif(EDD.JJ_SAP_DSTRBTR_NM, ''), 'NA') as JJ_SAP_DSTRBTR_NM,
            jj_mnth_id
        FROM (
                --2#SELLOUT DATA EXCLUDING NKA & E-COMMERCE
                SELECT 'SELLOUT' AS IDENTIFIER,
                    (T1.bill_dt::timestamp without time zone) AS bill_dt,
                    T1.JJ_SAP_DSTRBTR_ID,
                    TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) AS DSTRBTR_GRP_CD,
                    T1.JJ_SAP_PROD_ID,
                    T1.GROSS_SELLOUT_VAL AS GROSS_SELLOUT_VAL,
                    jj_mnth_id
                FROM (
                        SELECT T1.BILL_DT,
                            T1.DSTRBTR_GRP_CD,
                            T1.JJ_SAP_DSTRBTR_ID,
                            T1.JJ_SAP_PROD_ID,
                            T1.GRS_VAL AS GROSS_SELLOUT_VAL,
                            jj_mnth_id
                        FROM ITG_ALL_DISTRIBUTOR_SELLOUT_SALES_FACT T1
                        UNION ALL
                        SELECT T1.BILL_DT,
                            T1.DSTRBTR_GRP_CD,
                            T1.JJ_SAP_DSTRBTR_ID,
                            T1.JJ_SAP_PROD_ID,
                            T1.GRS_VAL AS GROSS_SELLOUT_VAL,
                            jj_mnth_id
                        FROM ITG_ALL_NON_IVY_DISTRIBUTOR_SELLOUT_SALES_FACT T1
                        UNION ALL
                        SELECT T1.BILL_DT,
                            T1.DSTRBTR_GRP_CD,
                            T1.JJ_SAP_DSTRBTR_ID,
                            T1.JJ_SAP_PROD_ID,
                            T1.GRS_VAL AS GROSS_SELLOUT_VAL,
                            jj_mnth_id
                        FROM ITG_ALL_IVY_DISTRIBUTOR_SELLOUT_SALES_FACT T1
                    ) T1,
                    EDW_DISTRIBUTOR_DIM EDD
                WHERE UPPER(TRIM(T1.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID))
                    and T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
                    AND TRIM(UPPER(T1.JJ_SAP_PROD_ID)) IS NOT NULL
                    AND TRIM(UPPER(T1.JJ_SAP_PROD_ID)) != 'DAOG20'
                    AND UPPER(TRIM(EDD.REGION)) NOT IN ('NATIONAL ACCOUNT', 'E-COMMERCE')
                    AND T1.GROSS_SELLOUT_VAL > 0 --4#SELLOUT_DISTRIBUTOR_NATIONAL_ACCOUNT AND E-COMMERCE REGION
                UNION ALL
                SELECT 'SELLOUT_NKA_ECOM' AS IDENTIFIER,
                    (T1.bill_dt::timestamp without time zone) AS bill_dt,
                    EDD.AREA AS JJ_SAP_DSTRBTR_ID,
                    TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) AS DSTRBTR_GRP_CD,
                    T1.JJ_SAP_PROD_ID,
                    T1.GROSS_SELLOUT_VAL AS GROSS_SELLOUT_VAL,
                    jj_mnth_id
                FROM (
                        SELECT T1.BILL_DT,
                            EDD.DSTRBTR_GRP_CD,
                            T1.JJ_SAP_DSTRBTR_ID,
                            T1.JJ_SAP_PROD_ID,
                            T1.NET_VAL AS GROSS_SELLOUT_VAL,
                            T2.jj_mnth_id
                        FROM ITG_ALL_DISTRIBUTOR_SELLIN_SALES_FACT T1,
                            EDW_DISTRIBUTOR_DIM EDD,
                            EDW_TIME_DIM T2
                        WHERE UPPER(TRIM(T1.BILL_TYPE_ID)) IN ('F2', 'RE')
                            and (T1.BILL_DT) = (T2.CAL_DATE)
                            AND UPPER(TRIM(T1.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID))
                            and T2.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
                        UNION ALL
                        SELECT T1.BILL_DT,
                            EDD.DSTRBTR_GRP_CD,
                            DECODE(
                                LTRIM(T1.SOLD_TO, 0),
                                '120166',
                                '115330',
                                '120167',
                                '115327',
                                '120168',
                                '115329',
                                '120169',
                                '115328',
                                '120170',
                                '116193',
                                '120165',
                                '116193',
                                '120171',
                                '116194',
                                '120173',
                                '116206',
                                '120174',
                                '116207',
                                '120172',
                                '116195',
                                '123537',
                                '119756',
                                '123685',
                                '123877',
                                '123686',
                                '123878',
                                '123687',
                                '123879',
                                '123688',
                                '123880',
                                '126000',
                                '125881',
                                '126001',
                                '125821',
                                '126003',
                                '125822',
                                '126002',
                                '125823',
                                '131389',
                                '119756',
                                '121413',
                                '122155',
                                '130200',
                                '130122',
                                '130196',
                                '130118',
                                '130197',
                                '125687',
                                '130199',
                                '129735',
                                '130198',
                                '125686',
                                '130961',
                                '130962',
                                '130774',
                                '123877',
                                '131653',
                                '109676',
                                '133685',
                                '133699',
                                '131587',
                                '131588',
                                '134432',
                                '134433',
                                '134679',
                                '134433',
                                '131595',
                                '131594',
                                '135355',
                                '135362',
                                LTRIM(T1.SOLD_TO, 0)
                            ) AS JJ_SAP_DSTRBTR_ID,
                            LTRIM(T1.MATERIAL, 0) AS JJ_SAP_PROD_ID,
                            T1.SUBTOTAL_1 AS GROSS_SELLOUT_VAL,
                            T2.jj_mnth_id
                        FROM EDW_BILLING_FACT T1,
                            EDW_DISTRIBUTOR_DIM EDD,
                            EDW_TIME_DIM T2
                        WHERE UPPER(
                                DECODE(
                                    LTRIM(T1.SOLD_TO, 0),
                                    '120166',
                                    '115330',
                                    '120167',
                                    '115327',
                                    '120168',
                                    '115329',
                                    '120169',
                                    '115328',
                                    '120170',
                                    '116193',
                                    '120165',
                                    '116193',
                                    '120171',
                                    '116194',
                                    '120173',
                                    '116206',
                                    '120174',
                                    '116207',
                                    '120172',
                                    '116195',
                                    '123537',
                                    '119756',
                                    '123685',
                                    '123877',
                                    '123686',
                                    '123878',
                                    '123687',
                                    '123879',
                                    '123688',
                                    '123880',
                                    '126000',
                                    '125881',
                                    '126001',
                                    '125821',
                                    '126003',
                                    '125822',
                                    '126002',
                                    '125823',
                                    '131389',
                                    '119756',
                                    '121413',
                                    '122155',
                                    '130200',
                                    '130122',
                                    '130196',
                                    '130118',
                                    '130197',
                                    '125687',
                                    '130199',
                                    '129735',
                                    '130198',
                                    '125686',
                                    '130961',
                                    '130962',
                                    '130774',
                                    '123877',
                                    '131653',
                                    '109676',
                                    '133685',
                                    '133699',
                                    '131587',
                                    '131588',
                                    '134432',
                                    '134433',
                                    '134679',
                                    '134433',
                                    '131595',
                                    '131594',
                                    '135355',
                                    '135362',
                                    LTRIM(T1.SOLD_TO, 0)
                                )
                            ) = UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID))
                            and (T1.BILL_DT) = (T2.CAL_DATE)
                            and T2.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
                            AND UPPER(T1.LOC_CURRCY) = 'IDR'
                            AND UPPER(T1.BILL_TYPE) IN ('ZF2I', 'ZL2I', 'ZC2I', 'ZG2I', 'S1', 'S2')
                            AND UPPER(T1.MATERIAL) != 'REBATE'
                            AND LTRIM(T1.SOLD_TO, 0) != '590092'
                    ) T1,
                    EDW_DISTRIBUTOR_DIM EDD
                WHERE UPPER(TRIM(T1.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID))
                    and T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
                    AND UPPER(TRIM(T1.JJ_SAP_PROD_ID)) IS NOT NULL
                    AND UPPER(TRIM(T1.JJ_SAP_PROD_ID)) != 'DAOG20'
                    AND UPPER(TRIM(EDD.REGION)) IN ('NATIONAL ACCOUNT', 'E-COMMERCE')
                    AND GROSS_SELLOUT_VAL > 0
            ) T1,
            EDW_PRODUCT_DIM AS EPD,
            EDW_DISTRIBUTOR_DIM AS EDD
        WHERE TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(T1.JJ_SAP_PROD_ID))
            and T1.jj_mnth_id between EPD.effective_from(+) and EPD.effective_to(+)
            AND TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID))
            and T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
            AND TRIM(UPPER(T1.jj_sap_prod_id)) != 'REBATE'
            AND TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID)) != '119683'
            AND TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) != 'SPLD'
            AND TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) != 'JYM'
            AND left(T1.jj_mnth_id, 4) > (DATE_PART(YEAR, current_timestamp()) -6)
    ) A
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
                    --gph_prod_put_up_desc,
                    --EMD.greenlight_sku_flag as greenlight_sku_flag,
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
                            --GPH_PROD_PUT_UP_DESC,
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
                            --CAST(put_up AS VARCHAR) AS put_up,
                            effective_to
                        FROM edw_product_dim
                        WHERE LTRIM(jj_sap_prod_id, 0) NOT IN (
                                SELECT LTRIM(sap_matl_num, 0)
                                FROM EDW_VW_OS_MATERIAL_DIM
                                WHERE CNTRY_KEY = 'ID'
                            )
                    ) product
                    left join --edw_vw_greenlight_skus EMD
                    edw_material_dim EMD on ltrim(product.sap_matl_num, 0) = LTRIM(EMD.MATL_NUM, '0')
            )
        where rnk = 1
    ) EGPH ON UPPER (LTRIM (A.matl_num, 0)) = UPPER (LTRIM (SAP_MATL_NUM, 0))
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
    ) AS CUST_HIER ON UPPER (LTRIM (A.DSTRBTR_GRP_CD, '0')) = UPPER (LTRIM (CUST_HIER.DSTRBTR_GRP_CD, '0'))
GROUP BY A.JJ_SAP_DSTRBTR_NM,
    A.DSTRBTR_GRP_CD,
    EGPH.GPH_PROD_BRND,
    EGPH.GPH_PROD_VRNT,
    EGPH.GPH_PROD_SGMNT,
    EGPH.GPH_PROD_CTGRY,
    EGPH.pka_size_desc,
    EGPH.pka_product_key,
    CUST_HIER.SAP_PRNT_CUST_KEY
),
final as 
(
    select 
        dstrbtr_grp_name::varchar(83) as dstrbtr_grp_name,
        dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
        to_date(min_date) as min_date,
        sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
        pka_size_desc::varchar(30) as pka_size_desc,
        pka_product_key::varchar(68) as pka_product_key,
        gph_prod_brnd::varchar(50) as gph_prod_brnd,
        gph_prod_vrnt::varchar(100) as gph_prod_vrnt,
        gph_prod_sgmnt::varchar(50) as gph_prod_sgmnt,
        gph_prod_ctgry::varchar(50) as gph_prod_ctgry
    from trans 

)
select * from final