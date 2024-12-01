with edw_indonesia_noo_analysis as
(
    select * from {{ ref('idnedw_integration__edw_indonesia_noo_analysis') }}
),EDW_TIME_DIM as(
	select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
edw_distributor_dim as(
	select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
edw_product_dim as(
	select * from {{ ref('idnedw_integration__edw_product_dim') }}
),

trans as(
SELECT ETD.JJ_YEAR,
             ETD.JJ_QRTR,
             ETD.JJ_MNTH_ID,
             ETD.JJ_MNTH,
             ETD.JJ_MNTH_DESC,
             ETD.JJ_MNTH_NO,
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
             TRIM(UPPER(EPD.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
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
             T1.STRT_INV_QTY,
             T1.SELLIN_QTY,
             T1.SELLOUT_QTY,
             T1.END_INV_QTY,
             T1.STRT_INV_VAL,
             T1.GROSS_STRT_INV_VAL,
             T1.SELLIN_VAL,
             T1.GROSS_SELLIN_VAL,
             T1.SELLOUT_VAL,
             T1.GROSS_SELLOUT_VAL,
             T1.END_INV_VAL,
             T1.GROSS_END_INV_VAL,
             T1.SELLOUT_LAST_TWO_MNTHS_QTY,
             T1.SELLOUT_LAST_TWO_MNTHS_VAL,
             T1.GROSS_SELLOUT_LAST_TWO_MNTHS_VAL,
             T1.STOCK_ON_HAND_QTY,
             T1.STOCK_ON_HAND_VAL,
             NULL AS VARIANT,
             ETD.JJ_MNTH_LONG AS JJ_MNTH_LONG,
             0 AS BP_QTN,
             0 AS BP_VAL,
             0 AS S_OP_QTN,
             0 AS S_OP_VAL,
             0 AS TRGT_HNA,
             0 AS TRGT_NIV,
             NULL AS TRGT_BP_S_OP_FLAG,
             NULL AS TRGT_DIST_BRND_CHNL_FLAG,
             T1.SALEABLE_STOCK_QTY,
             T1.SALEABLE_STOCK_VALUE,
             T1.NON_SALEABLE_STOCK_QTY,
             T1.NON_SALEABLE_STOCK_VALUE,
             T1.INTRANSIT_QTY,
             T1.INTRANSIT_HNA,
             T1.INTRANSIT_NIV,
             0 as STOCK_ON_HAND_NIV,
             0 AS P3M_SELLOUT_VAL,
             0 AS P3M_GROSS_SELLOUT_VAL,
             0 AS P6M_SELLOUT_VAL,
             0 AS P6M_GROSS_SELLOUT_VAL,
			 T1.usd_conversion_rate
      FROM (SELECT 
                   
                   JJ_MNTH_ID,
                   JJ_SAP_DSTRBTR_ID,
                   JJ_SAP_PROD_ID,
                   0 as SELLIN_QTY,
                   SELLOUT_QTY,
                   0 as STRT_INV_QTY,
                   0 as END_INV_QTY,
                   0 as SELLIN_VAL,
                   0 as GROSS_SELLIN_VAL,
                   SELLOUT_VAL,
                   GROSS_SELLOUT_VAL,
                   0 as STRT_INV_VAL,
                   0 as GROSS_STRT_INV_VAL,
                   0 as END_INV_VAL,
                   0 as GROSS_END_INV_VAL,
                   0 as STOCK_ON_HAND_QTY,
                   0 as STOCK_ON_HAND_VAL,
				   CRTD_DTTM,
				   UPTD_DTTM,
                   0 as SALEABLE_STOCK_QTY,
                   0 as SALEABLE_STOCK_VALUE,
                   0 as NON_SALEABLE_STOCK_QTY,
                   0 as NON_SALEABLE_STOCK_VALUE,
                   0 as INTRANSIT_QTY,
                   0 as INTRANSIT_HNA,
                   0 as INTRANSIT_NIV,
                   usd_conversion_rate,
                   SUM(SELLOUT_QTY) OVER (PARTITION BY JJ_SAP_DSTRBTR_ID,JJ_SAP_PROD_ID ORDER BY JJ_MNTH_ID ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sellout_last_two_mnths_qty,
				   SUM(SELLOUT_VAL) OVER (PARTITION BY JJ_SAP_DSTRBTR_ID,JJ_SAP_PROD_ID ORDER BY JJ_MNTH_ID ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS SELLOUT_LAST_TWO_MNTHS_VAL,
				   SUM(GROSS_SELLOUT_VAL) OVER (PARTITION BY JJ_SAP_DSTRBTR_ID,JJ_SAP_PROD_ID ORDER BY JJ_MNTH_ID ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS GROSS_SELLOUT_LAST_TWO_MNTHS_VAL
            FROM 
			(select replace(JJ_MNTH,'.','') as JJ_MNTH_ID,JJ_SAP_DSTRBTR_ID,JJ_SAP_PROD_ID,usd_conversion_rate,sum(SLS_QTY) as SELLOUT_QTY,sum(niv) as SELLOUT_VAL,sum(hna) as GROSS_SELLOUT_VAL,MAX(current_timestamp()::timestamp_ntz) as CRTD_DTTM,
				   MAX(CAST(NULL AS TIMESTAMP)) as UPTD_DTTM, from 
			edw_indonesia_noo_analysis 
            group by JJ_MNTH,JJ_SAP_DSTRBTR_ID,JJ_SAP_PROD_ID,usd_conversion_rate) as noo
											) AS T1,
           EDW_DISTRIBUTOR_DIM AS EDD,
           EDW_PRODUCT_DIM AS EPD,
           --EDW_DISTRIBUTOR_GROUP_DIM AS EDGD,
      (SELECT DISTINCT JJ_YEAR,
              JJ_QRTR_NO,
              JJ_QRTR,
              JJ_MNTH_ID,
              JJ_MNTH,
              JJ_MNTH_DESC,
              JJ_MNTH_NO,
              JJ_MNTH_SHRT,
              JJ_MNTH_LONG
       FROM EDW_TIME_DIM) AS ETD
      WHERE
      --TRIM(UPPER(EDGD.DSTRBTR_GRP_CD (+)))=TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) AND
      TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID))
	  and   T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
      AND   ETD.JJ_MNTH_ID = T1.JJ_MNTH_ID
      AND   TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(T1.JJ_SAP_PROD_ID))
	  and   T1.jj_mnth_id between EPD.effective_from(+) and EPD.effective_to(+)
      AND   TRIM(UPPER(T1.jj_sap_prod_id)) != 'REBATE'
      AND   TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID)) != '119683'
      AND   TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) != 'SPLD'
      AND   TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) != 'JYM'
      AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID)) != 'DAOG20'
	  
	  )


select * from trans
