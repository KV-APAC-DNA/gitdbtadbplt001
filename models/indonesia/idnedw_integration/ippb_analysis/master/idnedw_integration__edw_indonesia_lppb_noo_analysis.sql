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
noo as (
	select replace(JJ_MNTH,'.','') as JJ_MNTH_ID,
		JJ_SAP_DSTRBTR_ID,
		JJ_SAP_PROD_ID,
		usd_conversion_rate,
		sum(SLS_QTY) as SELLOUT_QTY,
		sum(niv) as SELLOUT_VAL,
		sum(hna) as GROSS_SELLOUT_VAL,
		0 as P2M_SELLOUT_QTY,
		0 as P2M_SELLOUT_VAL,
		0 as P2M_GROSS_SELLOUT_VAL,
		0 as P3M_SELLOUT_VAL,
		0 as P3M_GROSS_SELLOUT_VAL,
		0 as P6M_SELLOUT_VAL,
		0 as P6M_GROSS_SELLOUT_VAL from 
edw_indonesia_noo_analysis 
            group by JJ_MNTH,JJ_SAP_DSTRBTR_ID,JJ_SAP_PROD_ID,usd_conversion_rate
),

p2m_sellout as(
		
	SELECT CAL.JJ_MNTH_ID,
        JJ_SAP_DSTRBTR_ID,
		JJ_SAP_PROD_ID,
		usd_conversion_rate,
		0 as SELLOUT_QTY,
		0 as SELLOUT_VAL,
		0 as GROSS_SELLOUT_VAL,
		SUM(T1.SELLOUT_QTY) AS P2M_SELLOUT_QTY,
        SUM(T1.SELLOUT_VAL) AS P2M_SELLOUT_VAL,
        SUM(T1.GROSS_SELLOUT_VAL) AS P2M_GROSS_SELLOUT_VAL,
		0 as P3M_SELLOUT_VAL,
		0 as P3M_GROSS_SELLOUT_VAL,
		0 as P6M_SELLOUT_VAL,
		0 as P6M_GROSS_SELLOUT_VAL
				   
				   
      FROM (SELECT 
                   
                   JJ_MNTH_ID,
                   JJ_SAP_DSTRBTR_ID,
                   JJ_SAP_PROD_ID,
                   SELLOUT_QTY,
                   SELLOUT_VAL,
                   GROSS_SELLOUT_VAL,
                   usd_conversion_rate,
				                      
            FROM noo ) AS T1,
           
      (SELECT DISTINCT 
              JJ_MNTH_ID,              
			  TO_CHAR(DATEADD(MONTH, -1, TO_DATE(TO_CHAR(EDW_TIME_DIM.JJ_MNTH_ID), 'YYYYMM')), 'YYYYMM') AS L1_MONTH,
              TO_CHAR(DATEADD(MONTH, -2, TO_DATE(TO_CHAR(EDW_TIME_DIM.JJ_MNTH_ID), 'YYYYMM')), 'YYYYMM') AS L2_MONTH
       FROM EDW_TIME_DIM) AS CAL
      WHERE
      T1.JJ_MNTH_ID >= CAL.L2_MONTH
      AND T1.JJ_MNTH_ID <= CAL.L1_MONTH
	  group by CAL.JJ_MNTH_ID,
                   JJ_SAP_DSTRBTR_ID,
                   JJ_SAP_PROD_ID,
                   usd_conversion_rate
	  
	  ),	  
p3m_sellout as (
	SELECT 
        CAL.JJ_MNTH_ID,
        JJ_SAP_DSTRBTR_ID,
		JJ_SAP_PROD_ID,
		usd_conversion_rate,
		0 as SELLOUT_QTY,
		0 as SELLOUT_VAL,
		0 as GROSS_SELLOUT_VAL,
		0 AS P2M_SELLOUT_QTY,
        0 AS P2M_SELLOUT_VAL,
        0 AS P2M_GROSS_SELLOUT_VAL,
		SUM(T1.SELLOUT_VAL) as P3M_SELLOUT_VAL,
        SUM(T1.GROSS_SELLOUT_VAL) as P3M_GROSS_SELLOUT_VAL,
		0 as P6M_SELLOUT_VAL,
		0 as P6M_GROSS_SELLOUT_VAL
				   
                   
      FROM (SELECT 
                   
                   JJ_MNTH_ID,
                   JJ_SAP_DSTRBTR_ID,
                   JJ_SAP_PROD_ID,
                   SELLOUT_QTY,
                   SELLOUT_VAL,
                   GROSS_SELLOUT_VAL,
                   usd_conversion_rate,
				                      
            FROM  noo ) AS T1,
           
      (SELECT DISTINCT 
              JJ_MNTH_ID,              
			  TO_CHAR(DATEADD(MONTH, -1, TO_DATE(TO_CHAR(EDW_TIME_DIM.JJ_MNTH_ID), 'YYYYMM')), 'YYYYMM') AS L1_MONTH,
              TO_CHAR(DATEADD(MONTH, -3, TO_DATE(TO_CHAR(EDW_TIME_DIM.JJ_MNTH_ID), 'YYYYMM')), 'YYYYMM') AS L3_MONTH
       FROM EDW_TIME_DIM) AS CAL
      WHERE
      T1.JJ_MNTH_ID >= CAL.L3_MONTH
      AND T1.JJ_MNTH_ID <= CAL.L1_MONTH
	  group by CAL.JJ_MNTH_ID,
                   JJ_SAP_DSTRBTR_ID,
                   JJ_SAP_PROD_ID,
                   usd_conversion_rate
	  
	  
	  
	  
	),
p6m_sellout as (
	SELECT CAL.JJ_MNTH_ID,
        JJ_SAP_DSTRBTR_ID,
		JJ_SAP_PROD_ID,
		usd_conversion_rate,
		0 as SELLOUT_QTY,
		0 as SELLOUT_VAL,
		0 as GROSS_SELLOUT_VAL,
		0 AS P2M_SELLOUT_QTY,
        0 AS P2M_SELLOUT_VAL,
        0 AS P2M_GROSS_SELLOUT_VAL,
		0 as P3M_SELLOUT_VAL,
        0 as P3M_GROSS_SELLOUT_VAL,
		SUM(T1.SELLOUT_VAL) as  P6M_SELLOUT_VAL,
        SUM(T1.GROSS_SELLOUT_VAL) as P6M_GROSS_SELLOUT_VAL
				   
                   
      FROM (SELECT 
                   
                   JJ_MNTH_ID,
                   JJ_SAP_DSTRBTR_ID,
                   JJ_SAP_PROD_ID,
                   SELLOUT_QTY,
                   SELLOUT_VAL,
                   GROSS_SELLOUT_VAL,
                   usd_conversion_rate,
				                      
            FROM  noo ) AS T1,
           
      (SELECT DISTINCT 
              JJ_MNTH_ID,              
			  TO_CHAR(DATEADD(MONTH, -1, TO_DATE(TO_CHAR(EDW_TIME_DIM.JJ_MNTH_ID), 'YYYYMM')), 'YYYYMM') AS L1_MONTH,
              TO_CHAR(DATEADD(MONTH, -6, TO_DATE(TO_CHAR(EDW_TIME_DIM.JJ_MNTH_ID), 'YYYYMM')), 'YYYYMM') AS L6_MONTH
       FROM EDW_TIME_DIM) AS CAL
      WHERE
      T1.JJ_MNTH_ID >= CAL.L6_MONTH
      AND T1.JJ_MNTH_ID <= CAL.L1_MONTH
	  group by CAL.JJ_MNTH_ID,
                   JJ_SAP_DSTRBTR_ID,
                   JJ_SAP_PROD_ID,
                   usd_conversion_rate
),

trans as (
	SELECT JJ_MNTH_ID,
        JJ_SAP_DSTRBTR_ID,
		JJ_SAP_PROD_ID,
		usd_conversion_rate,
		sum(SELLOUT_QTY) as SELLOUT_QTY,
		sum(SELLOUT_VAL) as SELLOUT_VAL,
		sum(GROSS_SELLOUT_VAL) as GROSS_SELLOUT_VAL,
		sum(P2M_SELLOUT_QTY) as SELLOUT_LAST_TWO_MNTHS_QTY,
        sum(P2M_SELLOUT_VAL) as SELLOUT_LAST_TWO_MNTHS_VAL,
        sum(P2M_GROSS_SELLOUT_VAL) as GROSS_SELLOUT_LAST_TWO_MNTHS_VAL,
		sum(P3M_SELLOUT_VAL) as P3M_SELLOUT_VAL,
        sum(P3M_GROSS_SELLOUT_VAL) as P3M_GROSS_SELLOUT_VAL,
		sum(P6M_SELLOUT_VAL) as P6M_SELLOUT_VAL,
        sum(P6M_GROSS_SELLOUT_VAL) as P6M_GROSS_SELLOUT_VAL,
		MAX(current_timestamp()::timestamp_ntz) as CRTD_DTTM,
		MAX(CAST(NULL AS TIMESTAMP)) as UPTD_DTTM
		from (

	select * from noo
	union all
	select * from p2m_sellout
	union all
	select * from p3m_sellout
	union all
	select * from p6m_sellout ) sellout
    where sellout.JJ_MNTH_ID <= (SELECT DISTINCT JJ_MNTH_ID
                                       FROM EDW_TIME_DIM
                                       WHERE to_date(CAL_DATE) = to_date(current_timestamp()::timestamp_ntz(9)))
	group by JJ_MNTH_ID,JJ_SAP_DSTRBTR_ID, JJ_SAP_PROD_ID,usd_conversion_rate
),
final as(
SELECT ETD.JJ_YEAR,
             ETD.JJ_QRTR,
             ETD.JJ_MNTH_ID,
             ETD.JJ_MNTH,
             ETD.JJ_MNTH_DESC,
             ETD.JJ_MNTH_NO,
             TRIM(UPPER(NOO.latest_dstrbtr_grp_cd)) AS DSTRBTR_GRP_CD,
             noo.latest_distributor_group as DSTRBTR_GRP_NM,
             TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
             noo.latest_distributor_name as JJ_SAP_DSTRBTR_NM,
             EDD.JJ_SAP_DSTRBTR_NM || ' ^' || EDD.JJ_SAP_DSTRBTR_ID AS DSTRBTR_CD_NM,
             NOO.latest_area as AREA,
             NOO.latest_region as REGION,
             NOO.latest_area_pic as BDM_NM,
             NOO.latest_rbm as RBM_NM,
             EDD.STATUS AS DSTRBTR_STATUS,
             TRIM(UPPER(EPD.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
             EPD.JJ_SAP_PROD_DESC,
             EPD.JJ_SAP_UPGRD_PROD_ID,
             EPD.JJ_SAP_UPGRD_PROD_DESC,
             EPD.JJ_SAP_CD_MP_PROD_ID,
             EPD.JJ_SAP_CD_MP_PROD_DESC,
             EPD.JJ_SAP_UPGRD_PROD_DESC || ' ^' || EPD.JJ_SAP_UPGRD_PROD_ID AS SAP_PROD_CODE_NAME,
             NOO.latest_franchise as FRANCHISE,
             NOO.latest_brand as BRAND,
             EPD.VARIANT1 AS SKU_GRP_OR_VARIANT,
             EPD.VARIANT2 AS SKU_GRP1_OR_VARIANT1,
             EPD.VARIANT3 AS SKU_GRP2_OR_VARIANT2,
             NOO.latest_put_up AS SKU_GRP3_OR_VARIANT3,
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
             T1.P3M_SELLOUT_VAL,
             T1.P3M_GROSS_SELLOUT_VAL,
             T1.P6M_SELLOUT_VAL,
             T1.P6M_GROSS_SELLOUT_VAL,
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
				   SELLOUT_LAST_TWO_MNTHS_QTY,
				   SELLOUT_LAST_TWO_MNTHS_VAL,
				   GROSS_SELLOUT_LAST_TWO_MNTHS_VAL,
				   P3M_SELLOUT_VAL,
				   P3M_GROSS_SELLOUT_VAL,
				   P6M_SELLOUT_VAL,
				   P6M_GROSS_SELLOUT_VAL,
				   
                   
            FROM trans ) AS T1,
           EDW_DISTRIBUTOR_DIM AS EDD,
           EDW_PRODUCT_DIM AS EPD,
           edw_indonesia_noo_analysis NOO,
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
      TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID))
	  and   T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)
      AND   ETD.JJ_MNTH_ID = T1.JJ_MNTH_ID
      AND   TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(T1.JJ_SAP_PROD_ID))
	  and   T1.jj_mnth_id between EPD.effective_from(+) and EPD.effective_to(+)
      and   TRIM(UPPER(NOO.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID)) 
      and replace(NOO.JJ_MNTH(+),'.','') = T1.JJ_MNTH_ID and TRIM(UPPER(NOO.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(T1.JJ_SAP_PROD_ID))
	  
	  )
select * from final
