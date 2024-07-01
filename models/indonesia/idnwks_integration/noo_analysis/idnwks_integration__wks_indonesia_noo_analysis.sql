{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
        pre_hook="{% if is_incremental() %}
                delete from {{this}} wks where (jj_sap_dstrbtr_id, replace(wks.jj_mnth,'.','')) in (select distinct jj_sap_dstrbtr_id,jj_mnth_id from {{ source('idnwks_integration', 'wks_itg_all_distributor_sellin_sellout_fact') }} where upper(identifier) ='SELLOUT' or upper(identifier)='SELLOUT_NKA_ECOM');
                {% endif %}"     
    )
}}

with EDW_TIME_DIM as(
select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
wks_itg_all_distributor_sellin_sellout_fact as(
select * from {{ source('idnwks_integration', 'wks_itg_all_distributor_sellin_sellout_fact') }}
-- loaded from macro 
),
EDW_DISTRIBUTOR_DIM as(
select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
EDW_PRODUCT_DIM_rnk as(
select * from {{ ref('idnedw_integration__edw_product_dim') }} qualify row_number() over (partition by JJ_SAP_PROD_ID order by null) = 1
),
EDW_DISTRIBUTOR_CUSTOMER_DIM_rnk_outlet as (
select * from {{ ref('idnedw_integration__edw_distributor_customer_dim') }} qualify row_number() over (partition by key_outlet order by null) = 1
),
EDW_DISTRIBUTOR_CUSTOMER_DIM_rnk as(
select * from {{ ref('idnedw_integration__edw_distributor_customer_dim') }} qualify row_number() over (partition by JJ_SAP_DSTRBTR_ID,cust_id_map order by null) = 1
),
EDW_PRODUCT_DIM as(
select * from {{ ref('idnedw_integration__edw_product_dim') }} 
),
EDW_DISTRIBUTOR_CUSTOMER_DIM as(
select * from {{ ref('idnedw_integration__edw_distributor_customer_dim') }} 
),
EDW_DISTRIBUTOR_SALESMAN_DIM as(
select * from {{ ref('idnedw_integration__edw_distributor_salesman_dim') }}
),
union1 as 
(
    SELECT ETD.JJ_YEAR,
        ETD.JJ_QRTR,
        ETD.JJ_MNTH,
        ETD.JJ_WK,
        ETD.JJ_MNTH_WK_NO,
        ETD.JJ_MNTH_NO,
        EADSSF.BILL_DOC,
        EADSSF.BILL_DT,
        TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) AS DSTRBTR_GRP_CD,
        EDD.DSTRBTR_GRP_NM,
        TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
        EDD.JJ_SAP_DSTRBTR_NM,
        EDD.JJ_SAP_DSTRBTR_NM || ' ^' || EDD.JJ_SAP_DSTRBTR_ID AS DSTRBTR_CD_NM,
        EDD.AREA,
        EDD.REGION,
        EDD.BDM_NM,
        EDD.RBM_NM,
        EDD.STATUS AS DSTRBTR_STATUS,
        TRIM(UPPER(EDCD.CUST_ID_MAP)) AS CUST_ID_MAP,
        NVL(EDCD.CUST_NM_MAP, '') AS CUST_NM_MAP,
        (
            NVL(EDCD.CUST_NM_MAP, '') || ' ^' || EDCD.CUST_ID_MAP || ' ^' || EDCD.JJID
        ) AS DSTRBTR_CUST_CD_NM,
        EDCD.CUST_GRP,
        TRIM(UPPER(EDCD.CHNL)) AS CHNL,
        TRIM(UPPER(EDCD.OUTLET_TYPE)) AS OUTLET_TYPE,
        EDCD.CHNL_GRP,
        EDCD.JJID,
        EDCD.CHNL_GRP2,
        UPPER(EDD.PROV_NM || ' - ' || EDCD.CITY) AS CITY,
        (
            CASE
                WHEN EDCD.CUST_CRTD_DT < '12/29/2015' THEN 'EXISTING'
                WHEN EDCD.CUST_CRTD_DT BETWEEN '1/4/2016' AND '12/31/2016' THEN 'NOO 2016'
                ELSE 'NOO 2017'
            END
        ) AS CUST_STATUS,
        TRIM(UPPER(EPD.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
        EPD.JJ_SAP_PROD_DESC,
        EPD.JJ_SAP_UPGRD_PROD_ID,
        EPD.JJ_SAP_UPGRD_PROD_DESC,
        EPD.JJ_SAP_CD_MP_PROD_ID,
        EPD.JJ_SAP_CD_MP_PROD_DESC,
        EPD.JJ_SAP_UPGRD_PROD_DESC || ' ^' || EPD.JJ_SAP_UPGRD_PROD_ID AS SAP_PROD_CODE_NAME,
        EPD.FRANCHISE,
        EPD.BRAND,
        EPD.VARIANT1,
        EPD.VARIANT2,
        EPD.VARIANT3 AS VARIANT,
        EPD.VARIANT3 || ' ' || NVL(CAST(EPD.PUT_UP AS VARCHAR), '') AS PUT_UP,
        EPD.STATUS AS PROD_STATUS,
        NVL(TRIM(UPPER(EDSD.SLSMN_ID)), 'Noname') AS SLSMN_ID,
        NVL(EDSD.SLSMN_NM, 'Noname') AS SLSMN_NM,
        EADSSF.SLS_QTY,
        EADSSF.GRS_VAL AS HNA,
        EADSSF.JJ_NET_VAL AS NIV,
        EADSSF.TRD_DSCNT,
        EADSSF.DSTRBTR_NET_VAL AS DSTRBTR_NIV,
        EADSSF.RTRN_QTY,
        EADSSF.RTRN_VAL,
        0 AS HSKU_TARGET_GROWTH,
        0 AS HSKU_TARGET_COVERAGE,
        ETD.JJ_MNTH_LONG AS jj_mnth_long,
        0 AS TRGT_HNA,
        0 AS TRGT_NIV,
        NULL AS NPI_FLAG,
        NULL AS BENCHMARK_SKU_CODE,
        NULL AS SKU_BENCHMARK,
        NULL AS HERO_SKU_FLAG,
        NULL AS TRGT_DIST_BRND_CHNL_FLAG,
        EDCD.cust_grp2 AS Tiering,
        NULL as COUNT_SKU_CODE,
        NULL as MCS_STATUS,
        NULL as LOCAL_VARIANT,
        NULL as COUNT_LOCAL_VARIANT,
        EDSD.REC_KEY AS SALESMAN_KEY,
        EDSD.SFA_ID AS SFA_ID,
        NULL as LATEST_CHNL,
        NULL as LATEST_OUTLET_TYPE,
        NULL as LATEST_CHNL_GRP,
        NULL as LATEST_CUST_GRP2,
        NULL as LATEST_CUST_GRP,
        NULL as LATEST_CUST_NM_MAP,
        NULL as LATEST_REGION,
        NULL as LATEST_AREA,
        NULL as LATEST_RBM,
        NULL as LATEST_AREA_PIC,
        NULL as LATEST_JJID,
        NULL as LATEST_PUT_UP,
        NULL as LATEST_FRANCHISE,
        NULL as LATEST_BRAND,
        NULL as LATEST_MSL,
        NULL as LATEST_COUNT_LOCAL_VARIANT,
        NULL as LATEST_CHNL_GRP2,
        NULL as Latest_Distributor_Group,
        NULL as Latest_Dstrbtr_grp_cd
    FROM (
            SELECT TRANS_KEY,
                BILL_DOC,
                BILL_DT,
                JJ_MNTH_ID,
                jj_wk,
                DSTRBTR_GRP_CD,
                DSTRBTR_ID,
                JJ_SAP_DSTRBTR_ID,
                DSTRBTR_CUST_ID,
                DSTRBTR_PROD_ID,
                JJ_SAP_PROD_ID,
                DSTRBTN_CHNL,
                GRP_OUTLET,
                DSTRBTR_SLSMN_ID,
                SUM(SLS_QTY) AS SLS_QTY,
                SUM(GRS_VAL) AS GRS_VAL,
                SUM(JJ_NET_VAL) AS JJ_NET_VAL,
                SUM(TRD_DSCNT) AS TRD_DSCNT,
                SUM(DSTRBTR_NET_VAL) AS DSTRBTR_NET_VAL,
                SUM(RTRN_QTY) AS RTRN_QTY,
                SUM(RTRN_VAL) AS RTRN_VAL
            FROM (
                    SELECT T1.TRANS_KEY,
                        T1.BILL_DOC,
                        T1.BILL_DT,
                        T1.JJ_MNTH_ID,
                        T1.JJ_WK,
                        T1.DSTRBTR_GRP_CD,
                        T1.DSTRBTR_ID,
                        T1.JJ_SAP_DSTRBTR_ID,
                        T1.DSTRBTR_CUST_ID,
                        T1.DSTRBTR_PROD_ID,
                        T1.JJ_SAP_PROD_ID,
                        T1.DSTRBTN_CHNL,
                        T1.GRP_OUTLET,
                        T1.DSTRBTR_SLSMN_ID,
                        T1.SELLOUT_QTY AS SLS_QTY,
                        T1.GROSS_SELLOUT_VAL AS GRS_VAL,
                        T1.SELLOUT_VAL AS JJ_NET_VAL,
                        T1.TRD_DSCNT,
                        T1.DSTRBTR_NET_VAL,
                        T1.RTRN_QTY,
                        T1.RTRN_VAL
                    FROM WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_FACT T1
                    WHERE UPPER(T1.IDENTIFIER) = 'SELLOUT'
                )
            GROUP BY TRANS_KEY,
                BILL_DOC,
                BILL_DT,
                JJ_MNTH_ID,
                JJ_WK,
                DSTRBTR_GRP_CD,
                DSTRBTR_ID,
                JJ_SAP_DSTRBTR_ID,
                DSTRBTR_CUST_ID,
                DSTRBTR_PROD_ID,
                JJ_SAP_PROD_ID,
                DSTRBTN_CHNL,
                GRP_OUTLET,
                DSTRBTR_SLSMN_ID
            HAVING SUM(JJ_NET_VAL) <> 0
        ) AS EADSSF,
        EDW_DISTRIBUTOR_DIM AS EDD,
        EDW_PRODUCT_DIM AS EPD,
        EDW_DISTRIBUTOR_CUSTOMER_DIM AS EDCD,
        EDW_DISTRIBUTOR_SALESMAN_DIM AS EDSD,
        (
            SELECT DISTINCT JJ_YEAR,
                JJ_QRTR_NO,
                JJ_QRTR,
                JJ_MNTH_ID,
                JJ_MNTH,
                JJ_MNTH_NO,
                JJ_MNTH_SHRT,
                JJ_MNTH_LONG,
                JJ_WK,
                JJ_MNTH_WK_NO,
                JJ_DATE,
                CAL_DATE
            FROM EDW_TIME_DIM
        ) AS ETD
    WHERE TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(EADSSF.JJ_SAP_DSTRBTR_ID))
        and EADSSF.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
        AND TRIM(UPPER(EDCD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(EADSSF.JJ_SAP_DSTRBTR_ID))
        AND TRIM(UPPER(EDCD.CUST_ID(+))) = TRIM(UPPER(EADSSF.DSTRBTR_CUST_ID))
        and EADSSF.jj_mnth_id between EDCD.effective_from(+) and EDCD.effective_to(+)
        AND to_date(ETD.CAL_DATE) = to_date(EADSSF.BILL_DT)
        AND TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(EADSSF.JJ_SAP_PROD_ID))
        and EADSSF.jj_mnth_id between EPD.effective_from(+) and EPD.effective_to(+)
        AND TRIM(UPPER(EDSD.SLSMN_ID(+))) = TRIM(UPPER(EADSSF.DSTRBTR_SLSMN_ID))
        AND TRIM(UPPER(EDSD.jj_sap_dstrbtr_id(+))) = TRIM(UPPER(EADSSF.JJ_SAP_DSTRBTR_ID))
        and EADSSF.jj_mnth_id between EDSD.effective_from(+) and EDSD.effective_to(+)
        AND (NOT (TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) IN ('PZI')))
        AND TRIM(UPPER(EADSSF.DSTRBTR_GRP_CD)) != 'SPLD'
        AND TRIM(UPPER(EADSSF.DSTRBTR_GRP_CD)) != 'JYM'
        AND TRIM(UPPER(EADSSF.JJ_SAP_PROD_ID)) IS NOT NULL
        AND TRIM(UPPER(EADSSF.JJ_SAP_PROD_ID)) != 'DAOG20'
        AND UPPER(TRIM(EDD.REGION)) NOT IN ('NATIONAL ACCOUNT', 'E-COMMERCE')
),
union2 as
(
    SELECT ETD.JJ_YEAR,
        ETD.JJ_QRTR,
        ETD.JJ_MNTH,
        ETD.JJ_WK,
        ETD.JJ_MNTH_WK_NO,
        ETD.JJ_MNTH_NO,
        BILLING.BILL_DOC,
        CAST(BILLING.BILL_DT AS TIMESTAMP) AS BILL_DT,
        TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) AS DSTRBTR_GRP_CD,
        EDD.DSTRBTR_GRP_NM,
        TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
        EDD.JJ_SAP_DSTRBTR_NM,
        EDD.JJ_SAP_DSTRBTR_NM || ' ^' || EDD.JJ_SAP_DSTRBTR_ID AS DSTRBTR_CD_NM,
        EDD.AREA,
        EDD.REGION,
        EDD.BDM_NM,
        EDD.RBM_NM,
        EDD.STATUS AS DSTRBTR_STATUS,
        TRIM(UPPER(EDCD.CUST_ID_MAP)) AS CUST_ID_MAP,
        NVL(EDCD.CUST_NM_MAP, '') AS CUST_NM_MAP,
        (
            NVL(EDCD.CUST_NM_MAP, '') || ' ^' || EDCD.CUST_ID_MAP || ' ^' || EDCD.JJID
        ) AS DSTRBTR_CUST_CD_NM,
        EDCD.CUST_GRP,
        TRIM(UPPER(EDCD.CHNL)) AS CHNL,
        TRIM(UPPER(EDCD.OUTLET_TYPE)) AS OUTLET_TYPE,
        EDCD.CHNL_GRP,
        EDCD.JJID,
        EDCD.CHNL_GRP2,
        UPPER(EDD.PROV_NM || ' - ' || EDCD.CITY) AS CITY,
        (
            CASE
                WHEN EDCD.CUST_CRTD_DT < '12/29/2015' THEN 'EXISTING'
                WHEN EDCD.CUST_CRTD_DT BETWEEN '1/4/2016' AND '12/31/2016' THEN 'NOO 2016'
                ELSE 'NOO 2017'
            END
        ) AS CUST_STATUS,
        TRIM(UPPER(EPD.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
        EPD.JJ_SAP_PROD_DESC,
        EPD.JJ_SAP_UPGRD_PROD_ID,
        EPD.JJ_SAP_UPGRD_PROD_DESC,
        EPD.JJ_SAP_CD_MP_PROD_ID,
        EPD.JJ_SAP_CD_MP_PROD_DESC,
        EPD.JJ_SAP_UPGRD_PROD_DESC || ' ^' || EPD.JJ_SAP_UPGRD_PROD_ID AS SAP_PROD_CODE_NAME,
        EPD.FRANCHISE,
        EPD.BRAND,
        EPD.VARIANT1,
        EPD.VARIANT2,
        EPD.VARIANT3 AS VARIANT,
        EPD.VARIANT3 || ' ' || NVL(CAST(EPD.PUT_UP AS VARCHAR), '') AS PUT_UP,
        EPD.STATUS AS PROD_STATUS,
        'Noname' AS SLSMN_ID,
        'Noname' AS SLSMN_NM,
        BILLING.SLS_QTY,
        BILLING.GRS_VAL AS HNA,
        BILLING.JJ_NET_VAL AS NIV,
        0 AS TRD_DSCNT,
        0 AS DSTRBTR_NIV,
        0 AS RTRN_QTY,
        0 AS RTRN_VAL,
        0 AS HSKU_TARGET_GROWTH,
        0 AS HSKU_TARGET_COVERAGE,
        ETD.JJ_MNTH_LONG AS jj_mnth_long,
        0 AS TRGT_HNA,
        0 AS TRGT_NIV,
        NULL AS NPI_FLAG,
        NULL AS BENCHMARK_SKU_CODE,
        NULL AS SKU_BENCHMARK,
        NULL AS HERO_SKU_FLAG,
        NULL AS TRGT_DIST_BRND_CHNL_FLAG,
        EDCD.cust_grp2 AS Tiering,
        NULL as COUNT_SKU_CODE,
        NULL as MCS_STATUS,
        NULL as LOCAL_VARIANT,
        NULL as COUNT_LOCAL_VARIANT,
        NULL AS SALESMAN_KEY,
        NULL AS SFA_ID,
        NULL as LATEST_CHNL,
        NULL as LATEST_OUTLET_TYPE,
        NULL as LATEST_CHNL_GRP,
        NULL as LATEST_CUST_GRP2,
        NULL as LATEST_CUST_GRP,
        NULL as LATEST_CUST_NM_MAP,
        NULL as LATEST_REGION,
        NULL as LATEST_AREA,
        NULL as LATEST_RBM,
        NULL as LATEST_AREA_PIC,
        NULL as LATEST_JJID,
        NULL as LATEST_PUT_UP,
        NULL as LATEST_FRANCHISE,
        NULL as LATEST_BRAND,
        NULL as LATEST_MSL,
        NULL as LATEST_COUNT_LOCAL_VARIANT,
        NULL as LATEST_CHNL_GRP2,
        NULL as Latest_Distributor_Group,
        NULL as Latest_Dstrbtr_grp_cd
    FROM (
            SELECT T1.TRANS_KEY,
                T1.BILL_DOC,
                T1.BILL_DT,
                T1.JJ_MNTH_ID,
                T1.JJ_WK,
                T1.DSTRBTR_GRP_CD,
                T1.DSTRBTR_ID,
                T1.JJ_SAP_DSTRBTR_ID,
                T1.DSTRBTR_CUST_ID,
                T1.DSTRBTR_PROD_ID,
                T1.JJ_SAP_PROD_ID,
                T1.DSTRBTN_CHNL,
                T1.GRP_OUTLET,
                T1.DSTRBTR_SLSMN_ID,
                T1.SELLOUT_QTY AS SLS_QTY,
                T1.GROSS_SELLOUT_VAL AS GRS_VAL,
                T1.SELLOUT_VAL AS JJ_NET_VAL,
                T1.TRD_DSCNT,
                T1.DSTRBTR_NET_VAL,
                T1.RTRN_QTY,
                T1.RTRN_VAL
            FROM WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_FACT T1
            WHERE UPPER(T1.IDENTIFIER) = 'SELLOUT_NKA_ECOM'
        ) BILLING,
        EDW_DISTRIBUTOR_DIM AS EDD,
        EDW_PRODUCT_DIM AS EPD,
        EDW_DISTRIBUTOR_CUSTOMER_DIM AS EDCD,
        (
            SELECT DISTINCT JJ_YEAR,
                JJ_QRTR_NO,
                JJ_QRTR,
                JJ_MNTH_ID,
                JJ_MNTH,
                JJ_MNTH_NO,
                JJ_MNTH_SHRT,
                JJ_MNTH_LONG,
                JJ_WK,
                JJ_MNTH_WK_NO,
                JJ_DATE,
                CAL_DATE
            FROM EDW_TIME_DIM
        ) AS ETD
    WHERE TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(BILLING.JJ_SAP_DSTRBTR_ID))
        and BILLING.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
        AND TRIM(UPPER(EDCD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(BILLING.JJ_SAP_DSTRBTR_ID))
        and BILLING.jj_mnth_id between EDCD.effective_from(+) and EDCD.effective_to(+) -- AND   TRIM(UPPER(EDCD.CUST_ID(+))) = TRIM(UPPER(BILLING.DSTRBTR_CUST_ID))
        AND to_date(ETD.CAL_DATE) = to_date(BILLING.BILL_DT)
        AND TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(BILLING.JJ_SAP_PROD_ID))
        and BILLING.jj_mnth_id between EPD.effective_from(+) and EPD.effective_to(+)
        AND (NOT (TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) IN ('PZI')))
        AND TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) != 'SPLD'
        AND TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) != 'JYM'
        AND TRIM(UPPER(BILLING.JJ_SAP_PROD_ID)) IS NOT NULL
        AND TRIM(UPPER(BILLING.JJ_SAP_PROD_ID)) != 'DAOG20'
),
transformed as
(
    select * from union1
    union all
    select * from union2
),
updt as(
    select 
        transformed.jj_year as jj_year,
        transformed.jj_qrtr as jj_qrtr,
        transformed.jj_mnth as jj_mnth,
        transformed.jj_wk as jj_wk,
        transformed.jj_mnth_wk_no as jj_mnth_wk_no,
        transformed.jj_mnth_no as jj_mnth_no,
        transformed.bill_doc as bill_doc,
        transformed.bill_dt as bill_dt,
        transformed.dstrbtr_grp_cd as dstrbtr_grp_cd,
        transformed.dstrbtr_grp_nm as dstrbtr_grp_nm,
        transformed.jj_sap_dstrbtr_id as jj_sap_dstrbtr_id,
        transformed.jj_sap_dstrbtr_nm as jj_sap_dstrbtr_nm,
        transformed.dstrbtr_cd_nm as dstrbtr_cd_nm,
        transformed.area as area,
        transformed.region as region,
        transformed.bdm_nm as bdm_nm,
        transformed.rbm_nm as rbm_nm,
        transformed.dstrbtr_status as dstrbtr_status,
        transformed.cust_id_map as cust_id_map,
        transformed.cust_nm_map as cust_nm_map,
        transformed.dstrbtr_cust_cd_nm as dstrbtr_cust_cd_nm,
        transformed.cust_grp as cust_grp,
        transformed.chnl as chnl,
        transformed.outlet_type as outlet_type,
        transformed.chnl_grp as chnl_grp,
        transformed.jjid as jjid,
        transformed.chnl_grp2 as chnl_grp2,
        transformed.city as city,
        transformed.cust_status as cust_status,
        transformed.jj_sap_prod_id as jj_sap_prod_id,
        transformed.jj_sap_prod_desc as jj_sap_prod_desc,
        transformed.jj_sap_upgrd_prod_id as jj_sap_upgrd_prod_id,
        transformed.jj_sap_upgrd_prod_desc as jj_sap_upgrd_prod_desc,
        transformed.jj_sap_cd_mp_prod_id as jj_sap_cd_mp_prod_id,
        transformed.jj_sap_cd_mp_prod_desc as jj_sap_cd_mp_prod_desc,
        transformed.sap_prod_code_name as sap_prod_code_name,
        transformed.franchise as franchise,
        transformed.brand as brand,
        transformed.variant1 as variant1,
        transformed.variant2 as variant2,
        transformed.variant as variant,
        transformed.put_up as put_up,
        transformed.prod_status as prod_status,
        transformed.slsmn_id as slsmn_id,
        transformed.slsmn_nm as slsmn_nm,
        transformed.sls_qty as sls_qty,
        transformed.hna as hna,
        transformed.niv as niv,
        transformed.trd_dscnt as trd_dscnt,
        transformed.dstrbtr_niv as dstrbtr_niv,
        transformed.rtrn_qty as rtrn_qty,
        transformed.rtrn_val as rtrn_val,
        transformed.hsku_target_growth as hsku_target_growth,
        transformed.hsku_target_coverage as hsku_target_coverage,
        transformed.jj_mnth_long as jj_mnth_long,
        transformed.trgt_hna as trgt_hna,
        transformed.trgt_niv as trgt_niv,
        transformed.npi_flag as npi_flag,
        transformed.benchmark_sku_code as benchmark_sku_code,
        transformed.sku_benchmark as sku_benchmark,
        transformed.hero_sku_flag as hero_sku_flag,
        transformed.trgt_dist_brnd_chnl_flag as trgt_dist_brnd_chnl_flag,
        transformed.tiering as tiering,
        transformed.count_sku_code as count_sku_code,
        transformed.mcs_status as mcs_status,
        transformed.local_variant as local_variant,
        transformed.count_local_variant as count_local_variant,
        transformed.salesman_key as salesman_key,
        transformed.sfa_id as sfa_id,
        nvl(upper(edcd.chnl), transformed.latest_chnl) as latest_chnl,
        nvl(
            upper(edcd.outlet_type),
            transformed.latest_outlet_type
        ) as latest_outlet_type,
        nvl(edcd.chnl_grp, transformed.latest_chnl_grp) as latest_chnl_grp,
        nvl(edcd.cust_grp2, transformed.latest_cust_grp2) as latest_cust_grp2,
        nvl(edcd.cust_grp, transformed.latest_cust_grp) as latest_cust_grp,
        nvl(edcd.cust_nm_map, transformed.latest_cust_nm_map) as latest_cust_nm_map,
        nvl(edd.region, transformed.latest_region) as latest_region,
        nvl(edd.area, transformed.latest_area) as latest_area,
        nvl(edd.rbm_nm, transformed.latest_rbm) as latest_rbm,
        nvl(edd.bdm_nm, transformed.latest_area_pic) as latest_area_pic,
        nvl(edcd.jjid, transformed.latest_jjid) as latest_jjid,
        nvl(
            edp.VARIANT3 || ' ' || NVL(CAST(edp.PUT_UP AS VARCHAR), ''),
            transformed.latest_put_up
        ) as latest_put_up,
        nvl(edp.franchise, transformed.latest_franchise) as latest_franchise,
        nvl(edp.brand, transformed.latest_brand) as latest_brand,
        transformed.latest_msl as latest_msl,
        transformed.latest_count_local_variant as latest_count_local_variant,
        nvl(edcd.chnl_grp2, transformed.latest_chnl_grp2) as latest_chnl_grp2,
        nvl(
            edd.dstrbtr_grp_nm,
            transformed.latest_distributor_group
        ) as latest_distributor_group,
        --transformed.latest_distributor_group as latest_distributor_group,
        nvl(
            edd.dstrbtr_grp_cd,
            transformed.latest_dstrbtr_grp_cd
        ) as latest_dstrbtr_grp_cd,
        -- transformed.latest_dstrbtr_grp_cd as latest_dstrbtr_grp_cd
    from transformed
        left join EDW_DISTRIBUTOR_CUSTOMER_DIM_rnk_outlet EDCD ON concat(
            TRIM(UPPER(transformed.JJ_SAP_DSTRBTR_ID)),
            TRIM(UPPER(transformed.cust_id_map))
        ) = trim(upper(edcd.key_outlet))
        left join EDW_DISTRIBUTOR_DIM EDD ON TRIM(UPPER(transformed.JJ_SAP_DSTRBTR_ID)) = trim(upper(edd.jj_sap_dstrbtr_id))
        left join EDW_PRODUCT_DIM_rnk AS EDP on TRIM(UPPER(transformed.JJ_SAP_PROD_ID)) = trim(upper(edp.jj_sap_prod_id))
),
updt2 as(
    select updt.jj_year as jj_year,
        updt.jj_qrtr as jj_qrtr,
        updt.jj_mnth as jj_mnth,
        updt.jj_wk as jj_wk,
        updt.jj_mnth_wk_no as jj_mnth_wk_no,
        updt.jj_mnth_no as jj_mnth_no,
        updt.bill_doc as bill_doc,
        updt.bill_dt as bill_dt,
        updt.dstrbtr_grp_cd as dstrbtr_grp_cd,
        updt.dstrbtr_grp_nm as dstrbtr_grp_nm,
        updt.jj_sap_dstrbtr_id as jj_sap_dstrbtr_id,
        updt.jj_sap_dstrbtr_nm as jj_sap_dstrbtr_nm,
        updt.dstrbtr_cd_nm as dstrbtr_cd_nm,
        updt.area as area,
        updt.region as region,
        updt.bdm_nm as bdm_nm,
        updt.rbm_nm as rbm_nm,
        updt.dstrbtr_status as dstrbtr_status,
        updt.cust_id_map as cust_id_map,
        updt.cust_nm_map as cust_nm_map,
        updt.dstrbtr_cust_cd_nm as dstrbtr_cust_cd_nm,
        updt.cust_grp as cust_grp,
        updt.chnl as chnl,
        updt.outlet_type as outlet_type,
        updt.chnl_grp as chnl_grp,
        updt.jjid as jjid,
        updt.chnl_grp2 as chnl_grp2,
        updt.city as city,
        updt.cust_status as cust_status,
        updt.jj_sap_prod_id as jj_sap_prod_id,
        updt.jj_sap_prod_desc as jj_sap_prod_desc,
        updt.jj_sap_upgrd_prod_id as jj_sap_upgrd_prod_id,
        updt.jj_sap_upgrd_prod_desc as jj_sap_upgrd_prod_desc,
        updt.jj_sap_cd_mp_prod_id as jj_sap_cd_mp_prod_id,
        updt.jj_sap_cd_mp_prod_desc as jj_sap_cd_mp_prod_desc,
        updt.sap_prod_code_name as sap_prod_code_name,
        updt.franchise as franchise,
        updt.brand as brand,
        updt.variant1 as variant1,
        updt.variant2 as variant2,
        updt.variant as variant,
        updt.put_up as put_up,
        updt.prod_status as prod_status,
        updt.slsmn_id as slsmn_id,
        updt.slsmn_nm as slsmn_nm,
        updt.sls_qty as sls_qty,
        updt.hna as hna,
        updt.niv as niv,
        updt.trd_dscnt as trd_dscnt,
        updt.dstrbtr_niv as dstrbtr_niv,
        updt.rtrn_qty as rtrn_qty,
        updt.rtrn_val as rtrn_val,
        updt.hsku_target_growth as hsku_target_growth,
        updt.hsku_target_coverage as hsku_target_coverage,
        updt.jj_mnth_long as jj_mnth_long,
        updt.trgt_hna as trgt_hna,
        updt.trgt_niv as trgt_niv,
        updt.npi_flag as npi_flag,
        updt.benchmark_sku_code as benchmark_sku_code,
        updt.sku_benchmark as sku_benchmark,
        updt.hero_sku_flag as hero_sku_flag,
        updt.trgt_dist_brnd_chnl_flag as trgt_dist_brnd_chnl_flag,
        updt.tiering as tiering,
        updt.count_sku_code as count_sku_code,
        updt.mcs_status as mcs_status,
        updt.local_variant as local_variant,
        updt.count_local_variant as count_local_variant,
        updt.salesman_key as salesman_key,
        updt.sfa_id as sfa_id,
        --updt.latest_chnl as latest_chnl,
        case
            when updt.chnl is not null
            and updt.latest_chnl is null then upper(edcd.chnl)
            else updt.latest_chnl
        end as latest_chnl,
        case
            when updt.outlet_type is not null
            and updt.latest_outlet_type is null then upper(edcd.outlet_type)
            else updt.latest_outlet_type
        end as latest_outlet_type,
        case
            when updt.chnl_grp is not null
            and updt.latest_chnl_grp is null then edcd.chnl_grp
            else updt.latest_chnl_grp
        end as latest_chnl_grp,
        --updt.latest_chnl_grp as latest_chnl_grp,
        case
            when updt.tiering is not null
            and updt.latest_cust_grp2 is null then edcd.cust_grp2
            else updt.latest_cust_grp2
        end as latest_cust_grp2,
        --updt.latest_cust_grp2 as latest_cust_grp2,
        case
            when updt.cust_grp is not null
            and updt.latest_cust_grp is null then edcd.cust_grp
            else updt.latest_cust_grp
        end as latest_cust_grp,
        --updt.latest_cust_grp as latest_cust_grp,
        case
            when updt.cust_nm_map is not null
            and updt.latest_cust_nm_map is null then edcd.cust_nm_map
            else updt.latest_cust_nm_map
        end as latest_cust_nm_map,
        --updt.latest_cust_nm_map as latest_cust_nm_map,
        case
            when updt.region is not null
            and updt.latest_region is null then edd.region
            else updt.latest_region
        end as latest_region,
        -- updt.latest_region as latest_region,
        case
            when updt.area is not null
            and updt.latest_area is null then edd.area
            else updt.latest_area
        end as latest_area,
        --updt.latest_area as latest_area,
        case
            when updt.rbm_nm is not null
            and updt.latest_rbm is null then edd.rbm_nm
            else updt.latest_rbm
        end as latest_rbm,
        --updt.latest_rbm as latest_rbm,
        case
            when updt.bdm_nm is not null
            and updt.latest_area_pic is null then edd.bdm_nm
            else updt.latest_area_pic
        end as latest_area_pic,
        --updt.latest_area_pic as latest_area_pic,
        case
            when updt.jjid is not null
            and updt.latest_jjid is null then upper(edcd.jjid)
            else updt.latest_jjid
        end as latest_jjid,
        --  updt.latest_jjid as latest_jjid,
        case
            when updt.put_up is not null
            and updt.latest_put_up is null then (
                edp.VARIANT3 || ' ' || NVL(CAST(edp.PUT_UP AS VARCHAR), '')
            )
            else updt.latest_put_up
        end as latest_put_up,
        -- updt.latest_put_up as latest_put_up,
        case
            when updt.franchise is not null
            and updt.latest_franchise is null then edp.franchise
            else updt.latest_franchise
        end as latest_franchise,
        -- updt.latest_franchise as latest_franchise,
        case
            when updt.brand is not null
            and updt.latest_brand is null then edp.brand
            else updt.latest_brand
        end as latest_brand,filtered_noo
        --updt.latest_brand as latest_brand,
        updt.latest_msl as latest_msl,
        updt.latest_count_local_variant as latest_count_local_variant,
        case
            when updt.tiering is not null
            and updt.latest_chnl_grp2 is null then edcd.chnl_grp2
            else updt.latest_chnl_grp2
        end as latest_chnl_grp2,
        --updt.latest_chnl_grp2 as latest_chnl_grp2,
        updt.latest_distributor_group as latest_distributor_group,
        updt.latest_dstrbtr_grp_cd as latest_dstrbtr_grp_cd
    from updt
        left join EDW_DISTRIBUTOR_CUSTOMER_DIM_rnk EDCD on concat(
            TRIM(UPPER(updt.JJ_SAP_DSTRBTR_ID)),
            TRIM(UPPER(updt.cust_id_map))
        ) = concat(
            TRIM(UPPER(edcd.JJ_SAP_DSTRBTR_ID)),
            TRIM(UPPER(edcd.cust_id_map))
        )
        left join EDW_DISTRIBUTOR_DIM EDD ON TRIM(UPPER(updt.JJ_SAP_DSTRBTR_ID)) = trim(upper(edd.jj_sap_dstrbtr_id))
        left join EDW_PRODUCT_DIM_rnk AS EDP on TRIM(UPPER(updt.JJ_SAP_PROD_ID)) = trim(upper(edp.jj_sap_prod_id))
)


{% if is_incremental() %}
,
filtered_noo as (
    select * from {{ this }} as noo where jj_sap_dstrbtr_id, jj_mnth_id not in (select distinct jj_sap_dstrbtr_id, jj_mnth_id from updt2)
),
updated_noo as (
    select 
        filtered_noo.jj_year as jj_year,
        filtered_noo.jj_qrtr as jj_qrtr,
        filtered_noo.jj_mnth as jj_mnth,
        filtered_noo.jj_wk as jj_wk,
        filtered_noo.jj_mnth_wk_no as jj_mnth_wk_no,
        filtered_noo.jj_mnth_no as jj_mnth_no,
        filtered_noo.bill_doc as bill_doc,
        filtered_noo.bill_dt as bill_dt,
        filtered_noo.dstrbtr_grp_cd as dstrbtr_grp_cd,
        filtered_noo.dstrbtr_grp_nm as dstrbtr_grp_nm,
        filtered_noo.jj_sap_dstrbtr_id as jj_sap_dstrbtr_id,
        filtered_noo.jj_sap_dstrbtr_nm as jj_sap_dstrbtr_nm,
        filtered_noo.dstrbtr_cd_nm as dstrbtr_cd_nm,
        filtered_noo.area as area,
        filtered_noo.region as region,
        filtered_noo.bdm_nm as bdm_nm,
        filtered_noo.rbm_nm as rbm_nm,
        filtered_noo.dstrbtr_status as dstrbtr_status,
        filtered_noo.cust_id_map as cust_id_map,
        filtered_noo.cust_nm_map as cust_nm_map,
        filtered_noo.dstrbtr_cust_cd_nm as dstrbtr_cust_cd_nm,
        filtered_noo.cust_grp as cust_grp,
        filtered_noo.chnl as chnl,
        filtered_noo.outlet_type as outlet_type,
        filtered_noo.chnl_grp as chnl_grp,
        filtered_noo.jjid as jjid,
        filtered_noo.chnl_grp2 as chnl_grp2,
        filtered_noo.city as city,
        filtered_noo.cust_status as cust_status,
        filtered_noo.jj_sap_prod_id as jj_sap_prod_id,
        filtered_noo.jj_sap_prod_desc as jj_sap_prod_desc,
        filtered_noo.jj_sap_upgrd_prod_id as jj_sap_upgrd_prod_id,
        filtered_noo.jj_sap_upgrd_prod_desc as jj_sap_upgrd_prod_desc,
        filtered_noo.jj_sap_cd_mp_prod_id as jj_sap_cd_mp_prod_id,
        filtered_noo.jj_sap_cd_mp_prod_desc as jj_sap_cd_mp_prod_desc,
        filtered_noo.sap_prod_code_name as sap_prod_code_name,
        filtered_noo.franchise as franchise,
        filtered_noo.brand as brand,
        filtered_noo.variant1 as variant1,
        filtered_noo.variant2 as variant2,
        filtered_noo.variant as variant,
        filtered_noo.put_up as put_up,
        filtered_noo.prod_status as prod_status,
        filtered_noo.slsmn_id as slsmn_id,
        filtered_noo.slsmn_nm as slsmn_nm,
        filtered_noo.sls_qty as sls_qty,
        filtered_noo.hna as hna,
        filtered_noo.niv as niv,
        filtered_noo.trd_dscnt as trd_dscnt,
        filtered_noo.dstrbtr_niv as dstrbtr_niv,
        filtered_noo.rtrn_qty as rtrn_qty,
        filtered_noo.rtrn_val as rtrn_val,
        filtered_noo.hsku_target_growth as hsku_target_growth,
        filtered_noo.hsku_target_coverage as hsku_target_coverage,
        filtered_noo.jj_mnth_long as jj_mnth_long,
        filtered_noo.trgt_hna as trgt_hna,
        filtered_noo.trgt_niv as trgt_niv,
        filtered_noo.npi_flag as npi_flag,
        filtered_noo.benchmark_sku_code as benchmark_sku_code,
        filtered_noo.sku_benchmark as sku_benchmark,
        filtered_noo.hero_sku_flag as hero_sku_flag,
        filtered_noo.trgt_dist_brnd_chnl_flag as trgt_dist_brnd_chnl_flag,
        filtered_noo.tiering as tiering,
        filtered_noo.count_sku_code as count_sku_code,
        filtered_noo.mcs_status as mcs_status,
        filtered_noo.local_variant as local_variant,
        filtered_noo.count_local_variant as count_local_variant,
        filtered_noo.salesman_key as salesman_key,
        filtered_noo.sfa_id as sfa_id,
        nvl(upper(edcd.chnl), filtered_noo.latest_chnl) as latest_chnl,
        nvl(
            upper(edcd.outlet_type),
            filtered_noo.latest_outlet_type
        ) as latest_outlet_type,
        nvl(edcd.chnl_grp, filtered_noo.latest_chnl_grp) as latest_chnl_grp,
        nvl(edcd.cust_grp2, filtered_noo.latest_cust_grp2) as latest_cust_grp2,
        nvl(edcd.cust_grp, filtered_noo.latest_cust_grp) as latest_cust_grp,
        nvl(edcd.cust_nm_map, filtered_noo.latest_cust_nm_map) as latest_cust_nm_map,
        nvl(edd.region, filtered_noo.latest_region) as latest_region,
        nvl(edd.area, filtered_noo.latest_area) as latest_area,
        nvl(edd.rbm_nm, filtered_noo.latest_rbm) as latest_rbm,
        nvl(edd.bdm_nm, filtered_noo.latest_area_pic) as latest_area_pic,
        nvl(edcd.jjid, filtered_noo.latest_jjid) as latest_jjid,
        nvl(
            edp.VARIANT3 || ' ' || NVL(CAST(edp.PUT_UP AS VARCHAR), ''),
            filtered_noo.latest_put_up
        ) as latest_put_up,
        nvl(edp.franchise, filtered_noo.latest_franchise) as latest_franchise,
        nvl(edp.brand, filtered_noo.latest_brand) as latest_brand,
        filtered_noo.latest_msl as latest_msl,
        filtered_noo.latest_count_local_variant as latest_count_local_variant,
        nvl(edcd.chnl_grp2, filtered_noo.latest_chnl_grp2) as latest_chnl_grp2,
        nvl(
            edd.dstrbtr_grp_nm,
            filtered_noo.latest_distributor_group
        ) as latest_distributor_group,
        --filtered_noo.latest_distributor_group as latest_distributor_group,
        nvl(
            edd.dstrbtr_grp_cd,
            filtered_noo.latest_dstrbtr_grp_cd
        ) as latest_dstrbtr_grp_cd,
        -- filtered_noo.latest_dstrbtr_grp_cd as latest_dstrbtr_grp_cd
    from filtered_noo
        left join EDW_DISTRIBUTOR_CUSTOMER_DIM_rnk_outlet EDCD ON concat(
            TRIM(UPPER(filtered_noo.JJ_SAP_DSTRBTR_ID)),
            TRIM(UPPER(filtered_noo.cust_id_map))
        ) = trim(upper(edcd.key_outlet))
        left join EDW_DISTRIBUTOR_DIM EDD ON TRIM(UPPER(filtered_noo.JJ_SAP_DSTRBTR_ID)) = trim(upper(edd.jj_sap_dstrbtr_id))
        left join EDW_PRODUCT_DIM_rnk AS EDP on TRIM(UPPER(filtered_noo.JJ_SAP_PROD_ID)) = trim(upper(edp.jj_sap_prod_id))
),
transformed_noo as (
    select updated_noo.jj_year as jj_year,
        updated_noo.jj_qrtr as jj_qrtr,
        updated_noo.jj_mnth as jj_mnth,
        updated_noo.jj_wk as jj_wk,
        updated_noo.jj_mnth_wk_no as jj_mnth_wk_no,
        updated_noo.jj_mnth_no as jj_mnth_no,
        updated_noo.bill_doc as bill_doc,
        updated_noo.bill_dt as bill_dt,
        updated_noo.dstrbtr_grp_cd as dstrbtr_grp_cd,
        updated_noo.dstrbtr_grp_nm as dstrbtr_grp_nm,
        updated_noo.jj_sap_dstrbtr_id as jj_sap_dstrbtr_id,
        updated_noo.jj_sap_dstrbtr_nm as jj_sap_dstrbtr_nm,
        updated_noo.dstrbtr_cd_nm as dstrbtr_cd_nm,
        updated_noo.area as area,
        updated_noo.region as region,
        updated_noo.bdm_nm as bdm_nm,
        updated_noo.rbm_nm as rbm_nm,
        updated_noo.dstrbtr_status as dstrbtr_status,
        updated_noo.cust_id_map as cust_id_map,
        updated_noo.cust_nm_map as cust_nm_map,
        updated_noo.dstrbtr_cust_cd_nm as dstrbtr_cust_cd_nm,
        updated_noo.cust_grp as cust_grp,
        updated_noo.chnl as chnl,
        updated_noo.outlet_type as outlet_type,
        updated_noo.chnl_grp as chnl_grp,
        updated_noo.jjid as jjid,
        updated_noo.chnl_grp2 as chnl_grp2,
        updated_noo.city as city,
        updated_noo.cust_status as cust_status,
        updated_noo.jj_sap_prod_id as jj_sap_prod_id,
        updated_noo.jj_sap_prod_desc as jj_sap_prod_desc,
        updated_noo.jj_sap_upgrd_prod_id as jj_sap_upgrd_prod_id,
        updated_noo.jj_sap_upgrd_prod_desc as jj_sap_upgrd_prod_desc,
        updated_noo.jj_sap_cd_mp_prod_id as jj_sap_cd_mp_prod_id,
        updated_noo.jj_sap_cd_mp_prod_desc as jj_sap_cd_mp_prod_desc,
        updated_noo.sap_prod_code_name as sap_prod_code_name,
        updated_noo.franchise as franchise,
        updated_noo.brand as brand,
        updated_noo.variant1 as variant1,
        updated_noo.variant2 as variant2,
        updated_noo.variant as variant,
        updated_noo.put_up as put_up,
        updated_noo.prod_status as prod_status,
        updated_noo.slsmn_id as slsmn_id,
        updated_noo.slsmn_nm as slsmn_nm,
        updated_noo.sls_qty as sls_qty,
        updated_noo.hna as hna,
        updated_noo.niv as niv,
        updated_noo.trd_dscnt as trd_dscnt,
        updated_noo.dstrbtr_niv as dstrbtr_niv,
        updated_noo.rtrn_qty as rtrn_qty,
        updated_noo.rtrn_val as rtrn_val,
        updated_noo.hsku_target_growth as hsku_target_growth,
        updated_noo.hsku_target_coverage as hsku_target_coverage,
        updated_noo.jj_mnth_long as jj_mnth_long,
        updated_noo.trgt_hna as trgt_hna,
        updated_noo.trgt_niv as trgt_niv,
        updated_noo.npi_flag as npi_flag,
        updated_noo.benchmark_sku_code as benchmark_sku_code,
        updated_noo.sku_benchmark as sku_benchmark,
        updated_noo.hero_sku_flag as hero_sku_flag,
        updated_noo.trgt_dist_brnd_chnl_flag as trgt_dist_brnd_chnl_flag,
        updated_noo.tiering as tiering,
        updated_noo.count_sku_code as count_sku_code,
        updated_noo.mcs_status as mcs_status,
        updated_noo.local_variant as local_variant,
        updated_noo.count_local_variant as count_local_variant,
        updated_noo.salesman_key as salesman_key,
        updated_noo.sfa_id as sfa_id,
        --updated_noo.latest_chnl as latest_chnl,
        case
            when updated_noo.chnl is not null
            and updated_noo.latest_chnl is null then upper(edcd.chnl)
            else updated_noo.latest_chnl
        end as latest_chnl,
        case
            when updated_noo.outlet_type is not null
            and updated_noo.latest_outlet_type is null then upper(edcd.outlet_type)
            else updated_noo.latest_outlet_type
        end as latest_outlet_type,
        case
            when updated_noo.chnl_grp is not null
            and updated_noo.latest_chnl_grp is null then edcd.chnl_grp
            else updated_noo.latest_chnl_grp
        end as latest_chnl_grp,
        --updated_noo.latest_chnl_grp as latest_chnl_grp,
        case
            when updated_noo.tiering is not null
            and updated_noo.latest_cust_grp2 is null then edcd.cust_grp2
            else updated_noo.latest_cust_grp2
        end as latest_cust_grp2,
        --updated_noo.latest_cust_grp2 as latest_cust_grp2,
        case
            when updated_noo.cust_grp is not null
            and updated_noo.latest_cust_grp is null then edcd.cust_grp
            else updated_noo.latest_cust_grp
        end as latest_cust_grp,
        --updated_noo.latest_cust_grp as latest_cust_grp,
        case
            when updated_noo.cust_nm_map is not null
            and updated_noo.latest_cust_nm_map is null then edcd.cust_nm_map
            else updated_noo.latest_cust_nm_map
        end as latest_cust_nm_map,
        --updated_noo.latest_cust_nm_map as latest_cust_nm_map,
        case
            when updated_noo.region is not null
            and updated_noo.latest_region is null then edd.region
            else updated_noo.latest_region
        end as latest_region,
        -- updated_noo.latest_region as latest_region,
        case
            when updated_noo.area is not null
            and updated_noo.latest_area is null then edd.area
            else updated_noo.latest_area
        end as latest_area,
        --updated_noo.latest_area as latest_area,
        case
            when updated_noo.rbm_nm is not null
            and updated_noo.latest_rbm is null then edd.rbm_nm
            else updated_noo.latest_rbm
        end as latest_rbm,
        --updated_noo.latest_rbm as latest_rbm,
        case
            when updated_noo.bdm_nm is not null
            and updated_noo.latest_area_pic is null then edd.bdm_nm
            else updated_noo.latest_area_pic
        end as latest_area_pic,
        --updated_noo.latest_area_pic as latest_area_pic,
        case
            when updated_noo.jjid is not null
            and updated_noo.latest_jjid is null then upper(edcd.jjid)
            else updated_noo.latest_jjid
        end as latest_jjid,
        --  updated_noo.latest_jjid as latest_jjid,
        case
            when updated_noo.put_up is not null
            and updated_noo.latest_put_up is null then (
                edp.VARIANT3 || ' ' || NVL(CAST(edp.PUT_UP AS VARCHAR), '')
            )
            else updated_noo.latest_put_up
        end as latest_put_up,
        -- updated_noo.latest_put_up as latest_put_up,
        case
            when updated_noo.franchise is not null
            and updated_noo.latest_franchise is null then edp.franchise
            else updated_noo.latest_franchise
        end as latest_franchise,
        -- updated_noo.latest_franchise as latest_franchise,
        case
            when updated_noo.brand is not null
            and updated_noo.latest_brand is null then edp.brand
            else updated_noo.latest_brand
        end as latest_brand,filtered_noo
        --updated_noo.latest_brand as latest_brand,
        updated_noo.latest_msl as latest_msl,
        updated_noo.latest_count_local_variant as latest_count_local_variant,
        case
            when updated_noo.tiering is not null
            and updated_noo.latest_chnl_grp2 is null then edcd.chnl_grp2
            else updated_noo.latest_chnl_grp2
        end as latest_chnl_grp2,
        --updated_noo.latest_chnl_grp2 as latest_chnl_grp2,
        updated_noo.latest_distributor_group as latest_distributor_group,
        updated_noo.latest_dstrbtr_grp_cd as latest_dstrbtr_grp_cd
    from updated_noo
        left join EDW_DISTRIBUTOR_CUSTOMER_DIM_rnk EDCD on concat(
            TRIM(UPPER(updated_noo.JJ_SAP_DSTRBTR_ID)),
            TRIM(UPPER(updated_noo.cust_id_map))
        ) = concat(
            TRIM(UPPER(edcd.JJ_SAP_DSTRBTR_ID)),
            TRIM(UPPER(edcd.cust_id_map))
        )
        left join EDW_DISTRIBUTOR_DIM EDD ON TRIM(UPPER(updated_noo.JJ_SAP_DSTRBTR_ID)) = trim(upper(edd.jj_sap_dstrbtr_id))
        left join EDW_PRODUCT_DIM_rnk AS EDP on TRIM(UPPER(updated_noo.JJ_SAP_PROD_ID)) = trim(upper(edp.jj_sap_prod_id))
)
{% endif %}

--Final select
select * from updt2
--Union logic added here to compensate update on records which exist in edw_material_dim and not in wks_material_dim
{% if is_incremental() %}
union 
select * from transformed_noo
{% endif %}