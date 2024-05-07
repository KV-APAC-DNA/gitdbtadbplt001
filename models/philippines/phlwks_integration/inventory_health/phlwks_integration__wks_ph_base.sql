with edw_ph_siso_analysis as (
    select * from {{ ref('phledw_integration__edw_ph_siso_analysis') }}
),
itg_mds_ph_ref_distributors as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_distributors') }}
),
edw_vw_ph_dstrbtr_customer_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_dstrbtr_customer_dim') }}
),
edw_vw_ph_customer_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_customer_dim') }}
),
itg_mds_ph_pos_pricelist as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
edw_vw_ph_si_pos_inv_analysis as (
    select * from {{ ref('phledw_integration__edw_vw_ph_si_pos_inv_analysis') }}
),
edw_mv_ph_customer_dim as (
    select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
ph_lst_list_price as
(
    SELECT DISTINCT ITEM_CD,
        JJ_MNTH_ID,
        LST_PRICE_UNIT
    FROM 
        (
            SELECT ITEM_CD,
                LP.JJ_MNTH_ID AS LST_JJ_MNTH_ID,
                SALES.JJ_MNTH_ID AS JJ_MNTH_ID,
                LST_PRICE_UNIT,
                --LAG(LST_PRICE_UNIT) OVER (PARTITION BY ITEM_CD ORDER BY ITEM_CD,LP.JJ_MNTH_ID),
                RANK() OVER (
                    PARTITION BY ITEM_CD,
                    SALES.JJ_MNTH_ID
                    ORDER BY LP.JJ_MNTH_ID DESC
                ) AS RNK
            FROM (
                    SELECT DISTINCT ITEM_CD,
                        JJ_MNTH_ID,
                        LST_PRICE_UNIT
                    FROM ITG_MDS_PH_POS_PRICELIST
                    WHERE ACTIVE = 'Y'
                ) AS LP,
                (
                    SELECT DISTINCT JJ_MNTH_ID,
                        SKU_CD
                    FROM EDW_VW_PH_SI_POS_INV_ANALYSIS
                    WHERE JJ_YEAR >= (DATE_PART(YEAR, current_timestamp) -6)
                ) SALES
            WHERE LTRIM(LP.ITEM_CD, '0') = LTRIM(SALES.SKU_CD, '0')
                AND LP.JJ_MNTH_ID < SALES.JJ_MNTH_ID
        )
    WHERE RNK = 1
)

,
set_1 as
(    
    SELECT A.JJ_YEAR,
        A.JJ_MNTH_ID,
        NVL(CUST_HIER.SAP_PRNT_CUST_KEY, A.PARENT_CUSTOMER_CD) AS SAP_PRNT_CUST_KEY,
        nvl(
            nullif(a.dstrbtr_grp_cd || ' - ' || A.dstrbtr_grp_nm, ''),
            'NA'
        ) as dstr_cd_nm,
        nvl(nullif(A.dstrbtr_grp_cd, ''), 'NA') as dstrbtr_grp_cd,
        --,
        A.dstrbtr_grp_nm,
        nvl(nullif(a.sls_grp_desc, ''), 'NA') as sls_grp_desc,
        nvl(nullif(A.PARENT_CUSTOMER_CD, ''), 'NA') as PARENT_CUSTOMER_CD,
        nvl(nullif(a.sku, ''), 'NA') as matl_num,
        SUM(A.SI_SLS_QTY) AS SI_SLS_QTY,
        SUM(A.SI_GTS_VAL) AS SI_GTS_VAL,
        SUM(
            CASE
                WHEN INV.FLG = 'Y' THEN A.END_STOCK_QTY
                ELSE 0
            END
        ) AS INVENTORY_QUANTITY,
        SUM(
            CASE
                WHEN INV.FLG = 'Y' THEN (A.END_STOCK_VAL)
                ELSE 0
            END
        ) AS INVENTORY_VAL,
        SUM(A.SO_SLS_QTY_PC) AS SO_SLS_QTY,
        SUM(
            A.SO_jj_GRS_TRD_SLS + A.SO_JJ_RET_VAL - A.so_trd_discnt
        ) AS SO_GRS_TRD_SLS
    from 
        (
            (
                Select a.*,
                    mds.dstrbtr_grp_nm
                from (
                        SELECT *
                        FROM EDW_PH_SISO_ANALYSIS
                        WHERE JJ_YEAR >=(DATE_PART(YEAR, current_timestamp) -6)
                    ) a
                    left join (
                        Select distinct primary_sold_to,
                            dstrbtr_grp_cd,
                            dstrbtr_grp_nm,
                            active
                        from itg_mds_ph_ref_distributors
                    ) mds on a.dstrbtr_grp_cd = mds.dstrbtr_grp_cd
                    and mds.active = 'Y'
            ) A
            LEFT JOIN -- JOINED TO FIND LATEST DATE TO CALCULATE INVENTORY
            (
                SELECT CNTRY_NM,
                    JJ_YEAR,
                    JJ_QRTR,
                    JJ_MNTH_ID,
                    JJ_MNTH_NO,
                    DSTRBTR_GRP_CD,
                    MAX(JJ_MNTH_WK_NO) AS MX_WK_NUM,
                    'Y' AS FLG
                FROM EDW_PH_SISO_ANALYSIS A
                WHERE JJ_YEAR >=(DATE_PART(YEAR, current_timestamp) -6)
                    AND (DSTRBTR_GRP_CD || JJ_MNTH_ID || JJ_MNTH_WK_NO) IN (
                        SELECT (DSTRBTR_GRP_CD || JJ_MNTH_ID || JJ_MNTH_WK_NO) AS INCLUSION
                        FROM (
                                SELECT DSTRBTR_GRP_CD,
                                    JJ_MNTH_ID,
                                    JJ_MNTH_WK_NO,
                                    SUM(END_STOCK_VAL) AS END_STOCK_VAL
                                FROM EDW_PH_SISO_ANALYSIS
                                GROUP BY DSTRBTR_GRP_CD,
                                    JJ_MNTH_ID,
                                    JJ_MNTH_WK_NO
                                HAVING SUM(END_STOCK_VAL) <> 0
                            )
                    )
                GROUP BY CNTRY_NM,
                    JJ_YEAR,
                    JJ_QRTR,
                    JJ_MNTH_ID,
                    JJ_MNTH_NO,
                    DSTRBTR_GRP_CD
            ) INV ON A.CNTRY_NM = INV.CNTRY_NM
            AND A.JJ_YEAR = INV.JJ_YEAR
            AND A.JJ_MNTH_ID = INV.JJ_MNTH_ID
            AND A.DSTRBTR_GRP_CD = INV.DSTRBTR_GRP_CD
            AND INV.MX_WK_NUM = A.JJ_MNTH_WK_NO
            LEFT JOIN (
                SELECT DISTINCT DSTRBTR_GRP_CD,
                    SAP_SOLDTO_CODE
                FROM EDW_VW_PH_DSTRBTR_CUSTOMER_DIM
                WHERE CNTRY_CD = 'PH'
            ) AS CUST_MAP ON A.DSTRBTR_GRP_CD = CUST_MAP.DSTRBTR_GRP_CD
            LEFT JOIN (
                SELECT DISTINCT T1.SAP_PRNT_CUST_KEY,
                    T1.SAP_PRNT_CUST_DESC,
                    T1.SAP_CUST_ID
                FROM EDW_VW_PH_CUSTOMER_DIM T1
                WHERE T1.SAP_CNTRY_CD = 'PH'
            ) CUST_HIER ON LTRIM(CUST_MAP.SAP_SOLDTO_CODE, '0') = LTRIM(CUST_HIER.SAP_CUST_ID, '0')
            LEFT JOIN (
                SELECT DISTINCT JJ_MNTH_ID,
                    ITEM_CD,
                    LST_PRICE_UNIT
                from itg_mds_ph_pos_pricelist
                where active = 'Y'
            ) LP ON CAST(LP.JJ_MNTH_ID AS INT) = CAST(A.JJ_MNTH_ID AS INT)
            AND LTRIM(LP.ITEM_CD, '0') = LTRIM(A.SKU, '0')
        )
    group by 
        A.JJ_YEAR,
        A.JJ_MNTH_ID,
        NVL(CUST_HIER.SAP_PRNT_CUST_KEY, A.PARENT_CUSTOMER_CD),
        a.sls_grp_desc,
        A.dstrbtr_grp_cd,
        A.dstrbtr_grp_nm,
        A.PARENT_CUSTOMER_CD,
        A.SKU
)
,
set_2 as
(
        
    SELECT A.JJ_YEAR,
        A.JJ_MNTH_ID,
        NVL(CUST_HIER.SAP_PRNT_CUST_KEY, A.PARENT_CUSTOMER_CD) AS SAP_PRNT_CUST_KEY,
        nvl(nullif(A.parent_cust_nm, ''), 'NA') as sls_grp_dstr,
        nvl(nullif(A.DSTRBTR_GRP_CD, ''), 'NA') as dstrbtr_grp_cd,
        null as DSTRBTR_GRP_nm,
        null as sls_grp_desc,
        nvl(nullif(A.parent_cust_cd, ''), 'NA') as PARENT_CUSTOMER_CD,
        nvl(nullif(a.sku, ''), 'NA') as matl_num,
        SUM(A.SI_SLS_QTY) AS SI_SLS_QTY,
        SUM(A.SI_GTS_VAL) AS SI_GTS_VAL,
        SUM(A.END_STOCK_QTY) AS INVENTORY_QUANTITY,
        SUM(
            A.END_STOCK_QTY * NVL (LP.LST_PRICE_UNIT, LP_MAX.LST_PRICE_UNIT)
        ) AS INVENTORY_VAL,
        SUM(A.SO_SLS_QTY_PC) AS SO_SLS_QTY,
        SUM(A.SO_GRS_TRD_SLS) AS SO_GRS_TRD_SLS
    FROM (
            SELECT JJ_YEAR,
                JJ_QRTR,
                JJ_MNTH_ID,
                JJ_MNTH_NO,
                CNTRY_NM,
                INV.DSTRBTR_GRP_CD,
                inv.parent_cust_cd,
                inv.parent_cust_nm,
                inv.rpt_grp_2_desc as sls_grp,
                SOLD_TO AS SAP_SOLDTO_CODE,
                EOCD.SAP_PRNT_CUST_KEY AS PARENT_CUSTOMER_CD,
                EOCD.SAP_PRNT_CUST_DESC AS PARENT_CUSTOMER,
                GLOBAL_PROD_FRANCHISE,
                GLOBAL_PROD_BRAND,
                GLOBAL_PROD_SUB_BRAND,
                GLOBAL_PROD_VARIANT,
                GLOBAL_PROD_SEGMENT,
                GLOBAL_PROD_SUBSEGMENT,
                GLOBAL_PROD_CATEGORY,
                GLOBAL_PROD_SUBCATEGORY,
                GLOBAL_PUT_UP_DESC,
                SKU_CD AS SKU,
                SKU_DESCRIPTION AS SKU_DESC,
                SI_SLS_QTY,
                SI_GTS_VAL,
                END_STOCK_QTY,
                JJ_QTY_PC AS SO_SLS_QTY_PC,
                JJ_GTS AS SO_GRS_TRD_SLS
            FROM (
                    Select inv.*,
                        cust.parent_cust_cd,
                        parent_cust_nm,
                        rpt_grp_2_desc
                    from (
                            SELECT *
                            FROM EDW_VW_PH_SI_POS_INV_ANALYSIS
                            WHERE JJ_YEAR >= (DATE_PART(YEAR, current_timestamp) -6)
                        ) inv
                        left join (
                            Select distinct cust_id,
                                cust_nm,
                                parent_cust_cd,
                                parent_cust_nm,
                                dstrbtr_grp_cd,
                                dstrbtr_grp_nm,
                                rpt_grp_2_desc
                            from EDW_MV_PH_CUSTOMER_DIM
                        ) cust on inv.sold_to = cust.cust_id
                ) INV,
                (
                    SELECT *
                    FROM EDW_VW_PH_CUSTOMER_DIM
                    WHERE SAP_CNTRY_CD = 'PH'
                ) AS EOCD
            WHERE LTRIM(EOCD.SAP_CUST_ID(+), '0') = LTRIM(INV.SOLD_TO, '0')
        ) A
        LEFT JOIN (
            SELECT T1.*,
                T2.REGION_NM AS REGION,
                T2.PROVINCE_NM AS ZONE_OR_AREA
            FROM EDW_VW_PH_CUSTOMER_DIM T1,
                EDW_MV_PH_CUSTOMER_DIM T2
            WHERE T1.SAP_CNTRY_CD = 'PH'
                AND LTRIM(T1.SAP_CUST_ID, '0') = LTRIM(T2.CUST_ID(+), '0')
        ) CUST_HIER ON LTRIM (A.SAP_SOLDTO_CODE, '0') = LTRIM (CUST_HIER.SAP_CUST_ID, '0')
        LEFT JOIN (
            SELECT DISTINCT JJ_MNTH_ID,
                ITEM_CD,
                LST_PRICE_UNIT
            FROM itg_mds_ph_pos_pricelist
            WHERE active = 'Y'
        ) LP ON CAST (LP.JJ_MNTH_ID AS INT) = CAST (A.JJ_MNTH_ID AS INT)
        AND LTRIM (LP.ITEM_CD, '0') = LTRIM (A.SKU, '0')
        LEFT JOIN (
            SELECT *
            FROM PH_LST_LIST_PRICE
        ) LP_MAX ON LTRIM (LP_MAX.ITEM_CD, '0') = LTRIM (NVL (A.SKU, 'NA'), '0')
        AND LP_MAX.JJ_MNTH_ID = A.JJ_MNTH_ID
    GROUP BY A.JJ_YEAR,
        A.JJ_MNTH_ID,
        NVL(CUST_HIER.SAP_PRNT_CUST_KEY, A.PARENT_CUSTOMER_CD),
        A.sls_grp,
        A.parent_cust_nm,
        A.DSTRBTR_GRP_CD,
        a.parent_cust_cd,
        a.sku
)
,
final as
(
    select
        jj_year::number(18,0) as jj_year,
        jj_mnth_id::varchar(30) as jj_mnth_id,
        sap_prnt_cust_key::varchar(50) as sap_prnt_cust_key,
        dstr_cd_nm::varchar(308) as dstr_cd_nm,
        dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
        dstrbtr_grp_nm::varchar(655) as dstrbtr_grp_nm,
        sls_grp_desc::varchar(655) as sls_grp_desc,
        parent_customer_cd::varchar(50) as parent_customer_cd,
        matl_num::varchar(655) as matl_num,
        si_sls_qty::number(38,5) as si_sls_qty,
        si_gts_val::number(38,5) as si_gts_val,
        inventory_quantity::number(38,4) as inventory_quantity,
        inventory_val::number(38,8) as inventory_val,
        so_sls_qty::number(38,6) as so_sls_qty,
        so_grs_trd_sls::number(38,16) as so_grs_trd_sls
    from
    (
        select * from set_1
        union all
        select * from set_2
    )
)
select * from final