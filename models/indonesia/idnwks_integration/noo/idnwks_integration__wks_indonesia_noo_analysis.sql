{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        sql_header='use warehouse DEV_DNA_CORE_app2_wh;',
        pre_hook="delete from {{this}} wks where (jj_sap_dstrbtr_id, replace(wks.jj_mnth,'.','')) in (select distinct jj_sap_dstrbtr_id,jj_mnth_id from DEV_DNA_CORE.IDNWKS_INTEGRATION.WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_FACT where upper(identifier) ='SELLOUT' or upper(identifier)='SELLOUT_NKA_ECOM');",
        post_hook="{{ wks_indonesia_noo_analysis_update() }}"        
    )
}}
with EDW_TIME_DIM as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_TIME_DIM
),
WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_FACT as(
select * from DEV_DNA_CORE.IDNWKS_INTEGRATION.WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_FACT
),
EDW_DISTRIBUTOR_DIM as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_DISTRIBUTOR_DIM
),
EDW_PRODUCT_DIM_rnk as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_PRODUCT_DIM qualify row_number() over (partition by JJ_SAP_PROD_ID order by null) = 1
),
EDW_DISTRIBUTOR_CUSTOMER_DIM_rnk as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_DISTRIBUTOR_CUSTOMER_DIM qualify row_number() over (partition by JJ_SAP_DSTRBTR_ID order by null) = 1
),
EDW_PRODUCT_DIM as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_PRODUCT_DIM 
),
EDW_DISTRIBUTOR_CUSTOMER_DIM as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_DISTRIBUTOR_CUSTOMER_DIM 
),
EDW_DISTRIBUTOR_SALESMAN_DIM as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_DISTRIBUTOR_SALESMAN_DIM
),

union1 as (
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
            NVL(EDCD.CUST_NM_MAP,'') AS CUST_NM_MAP,
            (NVL(EDCD.CUST_NM_MAP,'') || ' ^' || EDCD.CUST_ID_MAP || ' ^' || EDCD.JJID) AS          DSTRBTR_CUST_CD_NM,
            EDCD.CUST_GRP,
            TRIM(UPPER(EDCD.CHNL)) AS CHNL,
            TRIM(UPPER(EDCD.OUTLET_TYPE)) AS OUTLET_TYPE,
            EDCD.CHNL_GRP,
            EDCD.JJID,
            EDCD.CHNL_GRP2,
            UPPER(EDD.PROV_NM || ' - ' || EDCD.CITY) AS CITY,
            (CASE WHEN EDCD.CUST_CRTD_DT < '12/29/2015' THEN 'EXISTING' WHEN EDCD.CUST_CRTD_DT BETWEEN '1/4/2016' AND '12/31/2016' THEN 'NOO 2016' ELSE 'NOO 2017' END) AS CUST_STATUS,
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
            EPD.VARIANT3 || ' ' || NVL(CAST(EPD.PUT_UP AS VARCHAR),'') AS PUT_UP,
            EPD.STATUS AS PROD_STATUS,
            NVL(TRIM(UPPER(EDSD.SLSMN_ID)),'Noname') AS SLSMN_ID,
            NVL(EDSD.SLSMN_NM,'Noname') AS SLSMN_NM,
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
	
      FROM (SELECT TRANS_KEY,
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
            FROM (SELECT T1.TRANS_KEY,
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
                        WHERE UPPER(T1.IDENTIFIER)='SELLOUT')
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
            HAVING SUM(JJ_NET_VAL) <> 0) AS EADSSF,
      EDW_DISTRIBUTOR_DIM AS EDD,
      EDW_PRODUCT_DIM AS EPD,
      EDW_DISTRIBUTOR_CUSTOMER_DIM AS EDCD,
      EDW_DISTRIBUTOR_SALESMAN_DIM AS EDSD,
      (SELECT DISTINCT JJ_YEAR,
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
            FROM EDW_TIME_DIM) AS ETD
      WHERE TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(EADSSF.JJ_SAP_DSTRBTR_ID))
      and EADSSF.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)   
      AND   TRIM(UPPER(EDCD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(EADSSF.JJ_SAP_DSTRBTR_ID))
      AND   TRIM(UPPER(EDCD.CUST_ID(+))) = TRIM(UPPER(EADSSF.DSTRBTR_CUST_ID))
      and EADSSF.jj_mnth_id between EDCD.effective_from(+) and EDCD.effective_to(+) 
      AND   to_date(ETD.CAL_DATE) = to_date(EADSSF.BILL_DT)
      AND   TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(EADSSF.JJ_SAP_PROD_ID))
      and EADSSF.jj_mnth_id between EPD.effective_from(+) and EPD.effective_to(+) 
      AND   TRIM(UPPER(EDSD.SLSMN_ID(+))) = TRIM(UPPER(EADSSF.DSTRBTR_SLSMN_ID))
      AND   TRIM(UPPER(EDSD.jj_sap_dstrbtr_id(+))) = TRIM(UPPER(EADSSF.JJ_SAP_DSTRBTR_ID)) 
      and EADSSF.jj_mnth_id between EDSD.effective_from(+) and EDSD.effective_to(+) 
      AND   (NOT (TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) IN ('PZI')))
      AND   TRIM(UPPER(EADSSF.DSTRBTR_GRP_CD)) != 'SPLD'
      AND   TRIM(UPPER(EADSSF.DSTRBTR_GRP_CD)) != 'JYM'
      AND   TRIM(UPPER(EADSSF.JJ_SAP_PROD_ID)) IS NOT NULL
      AND   TRIM(UPPER(EADSSF.JJ_SAP_PROD_ID)) != 'DAOG20'
      AND   UPPER(TRIM(EDD.REGION)) NOT IN ('NATIONAL ACCOUNT','E-COMMERCE')
),
union2 as(
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
            NVL(EDCD.CUST_NM_MAP,'') AS CUST_NM_MAP,
            (NVL(EDCD.CUST_NM_MAP,'') || ' ^' || EDCD.CUST_ID_MAP || ' ^' || EDCD.JJID) AS DSTRBTR_CUST_CD_NM,
            EDCD.CUST_GRP,
            TRIM(UPPER(EDCD.CHNL)) AS CHNL,
            TRIM(UPPER(EDCD.OUTLET_TYPE)) AS OUTLET_TYPE,
            EDCD.CHNL_GRP,
            EDCD.JJID,
            EDCD.CHNL_GRP2,
            UPPER(EDD.PROV_NM || ' - ' || EDCD.CITY) AS CITY,
            (CASE WHEN EDCD.CUST_CRTD_DT < '12/29/2015' THEN 'EXISTING' WHEN EDCD.CUST_CRTD_DT BETWEEN '1/4/2016' AND '12/31/2016' THEN 'NOO 2016' ELSE 'NOO 2017' END) AS CUST_STATUS,
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
            EPD.VARIANT3 || ' ' || NVL(CAST(EPD.PUT_UP AS VARCHAR),'') AS PUT_UP,
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
      FROM (SELECT T1.TRANS_KEY,
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
                        WHERE UPPER(T1.IDENTIFIER)='SELLOUT_NKA_ECOM') BILLING,
      EDW_DISTRIBUTOR_DIM AS EDD,
      EDW_PRODUCT_DIM AS EPD,
      EDW_DISTRIBUTOR_CUSTOMER_DIM AS EDCD,
      (SELECT DISTINCT JJ_YEAR,
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
            FROM EDW_TIME_DIM) AS ETD
            WHERE TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(BILLING.JJ_SAP_DSTRBTR_ID))
            and BILLING.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)  
            AND   TRIM(UPPER(EDCD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(BILLING.JJ_SAP_DSTRBTR_ID))
            and BILLING.jj_mnth_id between EDCD.effective_from(+) and EDCD.effective_to(+)
            -- AND   TRIM(UPPER(EDCD.CUST_ID(+))) = TRIM(UPPER(BILLING.DSTRBTR_CUST_ID))
            AND   to_date(ETD.CAL_DATE) = to_date(BILLING.BILL_DT)
            AND   TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(BILLING.JJ_SAP_PROD_ID))
            and BILLING.jj_mnth_id between EPD.effective_from(+) and EPD.effective_to(+) 
            AND   (NOT (TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) IN ('PZI')))
            AND   TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) != 'SPLD'
            AND   TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) != 'JYM'
            AND   TRIM(UPPER(BILLING.JJ_SAP_PROD_ID)) IS NOT NULL
            AND   TRIM(UPPER(BILLING.JJ_SAP_PROD_ID)) != 'DAOG20'
),
transformed as(
      select * from union1
      union all
      select * from union2
)
select * from transformed
