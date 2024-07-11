with edw_time_dim as (
select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
vw_sapbw_ciw_fact as (
select * from {{ ref('pcfedw_integration__vw_sapbw_ciw_fact') }}
),
vw_customer_dim as (
select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
edw_material_dim as (
select * from  {{ ref('pcfedw_integration__edw_material_dim') }}
),
vw_apo_parent_child_dim as (
select * from {{ ref('pcfedw_integration__vw_apo_parent_child_dim') }}
),
edw_perenso_prod_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_prod_dim') }}
),
edw_perenso_order_fact as (
select * from {{ ref('pcfedw_integration__edw_perenso_order_fact') }}
),
edw_perenso_account_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_dim') }}
),
itg_perenso_diary_item as (
select * from {{ ref('pcfitg_integration__itg_perenso_diary_item') }}
),
itg_perenso_users as (
select * from {{ ref('pcfitg_integration__itg_perenso_users') }}
),
itg_perenso_distributor_detail as (
select * from {{ ref('pcfitg_integration__itg_perenso_distributor_detail') }}
),
itg_perenso_prod_branch_identifier as (
select * from {{ ref('pcfitg_integration__itg_perenso_prod_branch_identifier') }}
),
edw_ps_msl_items as (
select * from {{ ref('pcfedw_integration__edw_ps_msl_items') }}
),
vw_jjbr_curr_exch_dim as (
select * from {{ ref('pcfedw_integration__vw_jjbr_curr_exch_dim') }}
),
vw_bwar_curr_exch_dim as (
select * from {{ ref('pcfedw_integration__vw_bwar_curr_exch_dim') }}
),
edw_pharm_sellout_fact as (
select * from {{ ref('pcfedw_integration__edw_pharm_sellout_fact') }}
),
edw_perenso_account_probeid_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_probeid_dim') }}
),
edw_perenso_prod_probeid_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_prod_probeid_dim') }}
),
itg_ims_field_order_history as (
select * from {{ source('pcfitg_integration', 'itg_ims_field_order_history') }}
),
edw_metcash_ind_grocery_fact as (
select * from {{ ref('pcfedw_integration__edw_metcash_ind_grocery_fact') }}
),
edw_perenso_account_metcash_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_metcash_dim') }}
),
itg_perenso_fssi_sales as (
select * from {{ ref('pcfitg_integration__itg_perenso_fssi_sales') }}
),
itg_perenso_fsni_sales as (
select * from {{ ref('pcfitg_integration__itg_perenso_fsni_sales') }}
),
edw_perenso_account_hist_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_hist_dim') }}
),
itg_perenso_prod_chk_distribution as (
select * from {{ ref('pcfitg_integration__itg_perenso_prod_chk_distribution') }}
),
edw_perenso_account_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_account_dim') }}
),
edw_perenso_prod_metcashid_dim as (
select * from {{ ref('pcfedw_integration__edw_perenso_prod_metcashid_dim') }}
),

ETD AS
(SELECT ETD.*,
             ETDW.JJ_MNTH_WK,
             ETDC.CAL_WK,
             ETDCM.CAL_MNTH_WK
      FROM EDW_TIME_DIM ETD,
           (SELECT ETD.JJ_YEAR,
                   ETD.JJ_MNTH_ID,
                   ETD.JJ_WK,
                   ROW_NUMBER() OVER (PARTITION BY ETD.JJ_YEAR,ETD.JJ_MNTH_ID ORDER BY ETD.JJ_YEAR,ETD.JJ_MNTH_ID,ETD.JJ_WK) AS JJ_MNTH_WK
            FROM (SELECT DISTINCT ETD.JJ_YEAR,
                         ETD.JJ_MNTH_ID,
                         ETD.JJ_WK
                  FROM EDW_TIME_DIM ETD) ETD) ETDW,
           (SELECT ETD.*,
                   CASE
                     WHEN ((ROW_NUMBER () OVER (PARTITION BY ETD.CAL_YEAR ORDER BY ETD.CAL_DATE::date)) % 7::BIGINT) = 0
                     THEN floor((ROW_NUMBER () OVER (PARTITION BY ETD.CAL_YEAR ORDER BY ETD.CAL_DATE::date)) / 7)
                     ELSE floor((ROW_NUMBER () OVER (PARTITION BY ETD.CAL_YEAR ORDER BY ETD.CAL_DATE::date)) / 7 + 1)
                   END AS CAL_WK
            FROM EDW_TIME_DIM ETD) ETDC,
           (SELECT ETDCW.CAL_YEAR,
                   ETDCW.CAL_MNTH_ID,
                   ETDCW.CAL_WK,
                   ROW_NUMBER() OVER (PARTITION BY ETDCW.CAL_YEAR,ETDCW.CAL_MNTH_ID ORDER BY ETDCW.CAL_YEAR,ETDCW.CAL_MNTH_ID,ETDCW.CAL_WK) AS CAL_MNTH_WK
            FROM (SELECT DISTINCT ETDC.CAL_YEAR,
                         ETDC.CAL_MNTH_ID,
                         ETDC.CAL_WK
                  FROM (SELECT ETD.CAL_YEAR,
                               ETD.CAL_MNTH_ID,
                               CASE
                                 WHEN ((ROW_NUMBER () OVER (PARTITION BY ETD.CAL_YEAR ORDER BY ETD.CAL_DATE::date)) % 7::BIGINT) = 0
                                 THEN floor((ROW_NUMBER () OVER (PARTITION BY ETD.CAL_YEAR ORDER BY ETD.CAL_DATE::date)) / 7)
                                 ELSE floor((ROW_NUMBER () OVER (PARTITION BY ETD.CAL_YEAR ORDER BY ETD.CAL_DATE::date)) / 7 + 1)
                               END AS CAL_WK
                        FROM EDW_TIME_DIM ETD) ETDC) ETDCW) ETDCM
      WHERE ETD.JJ_YEAR = ETDW.JJ_YEAR
      AND   ETD.JJ_MNTH_ID = ETDW.JJ_MNTH_ID
      AND   ETD.JJ_WK = ETDW.JJ_WK
      AND   ETD.CAL_DATE::date = ETDC.CAL_DATE::date
      AND   ETDC.CAL_WK = ETDCM.CAL_WK
      AND   ETDC.CAL_YEAR = ETDCM.CAL_YEAR
      AND   ETDC.CAL_MNTH_ID = ETDCM.CAL_MNTH_ID)
     ,

EAN_MAP AS
(SELECT DISTINCT JJ_MONTH_ID,
       BAR_CD,
       MATERIAL_ID,
       SALES_OFFICE_DESC
FROM (SELECT DISTINCT JJ_MONTH_ID,
             MASTER_CODE,
             BAR_CD,
             SALES_GRP_DESC,
             SALES_OFFICE_DESC,
             MATERIAL_COUNT,
             GTS_VAL,
             COUNT(DISTINCT MASTER_CODE),
             CASE
               WHEN COUNT(DISTINCT MASTER_CODE) > 1 THEN (
                 CASE
                   WHEN (MASTER_CODE IS NOT NULL ) THEN MAX_MATERIAL_ID
                   WHEN (MASTER_CODE IS NULL  ) THEN 'NULL'
                 END )
               WHEN COUNT(DISTINCT MASTER_CODE) = 1 THEN (
                 CASE
                   WHEN (MATERIAL_COUNT > 1) THEN MAX_MATERIAL_ID
                   WHEN (MATERIAL_COUNT = 1) THEN MAX_MATERIAL_ID
                 END )
               ELSE MATERIAL_ID
             END AS MATERIAL_ID
      FROM         (SELECT DISTINCT A.JJ_MONTH_ID,
                   A.MASTER_CODE,
                   A.BAR_CD,
                   A.MATL_ID AS MATERIAL_ID,
                   COUNT(A.MATL_ID) OVER (PARTITION BY A.JJ_MONTH_ID,A.BAR_CD,A.SALES_GRP_DESC) MATERIAL_COUNT,
                   MAX(A.MATL_ID) OVER (PARTITION BY A.JJ_MONTH_ID,A.MASTER_CODE,A.BAR_CD,A.SALES_GRP_DESC,SUM(A.GTS_VAL)) AS MAX_MATERIAL_ID,
                   ROW_NUMBER() OVER (PARTITION BY A.JJ_MONTH_ID,A.BAR_CD,A.SALES_GRP_DESC ORDER BY SUM(A.GTS_VAL) DESC) AS sales_rank,
                   COUNT(NVL (A.MASTER_CODE,'NA')) OVER (PARTITION BY A.JJ_MONTH_ID,A.BAR_CD,A.SALES_GRP_DESC,A.MATL_ID) AS MASTER_CODE_COUNT,
                   SUM(A.GTS_VAL) AS GTS_VAL,
                   A.CHANNEL_DESC,
                   A.SALES_GRP_DESC,
                   A.SALES_OFFICE_DESC,
                   B.MATL_BAR_COUNT

            FROM (SELECT VSF.JJ_MONTH_ID,
                         VSF.GTS_VAL,
                         VMD.MATL_ID,
                         VMD.BAR_CD,
                         MSTRCD.MASTER_CODE,
                         VCD.CUST_NO,
                         VCD.CHANNEL_DESC,
                         VCD.SALES_GRP_DESC,
                         VCD.SALES_OFFICE_DESC
                  FROM (SELECT * FROM VW_SAPBW_CIW_FACT WHERE CMP_ID = '8361' AND
						CAST(SUBSTRING(JJ_MONTH_ID,1,4) AS INTEGER) >= (YEAR(CURRENT_TIMESTAMP) - 3)) VSF
                    LEFT JOIN VW_CUSTOMER_DIM VCD ON VSF.CUST_NO = VCD.CUST_NO
                    LEFT JOIN EDW_MATERIAL_DIM VMD ON VSF.MATL_ID = VMD.MATL_ID
                    LEFT JOIN (VW_APO_PARENT_CHILD_DIM VAPCD
                    LEFT JOIN (SELECT DISTINCT VW_APO_PARENT_CHILD_DIM.MASTER_CODE,
                                      VW_APO_PARENT_CHILD_DIM.PARENT_MATL_DESC
                               FROM VW_APO_PARENT_CHILD_DIM
                               WHERE trim(VW_APO_PARENT_CHILD_DIM.CMP_ID)::varchar = '7470'
                               UNION ALL
                               SELECT DISTINCT VW_APO_PARENT_CHILD_DIM.MASTER_CODE,
                                      VW_APO_PARENT_CHILD_DIM.PARENT_MATL_DESC
                               FROM VW_APO_PARENT_CHILD_DIM
                               WHERE  VW_APO_PARENT_CHILD_DIM.MASTER_CODE not IN (SELECT DISTINCT VW_APO_PARENT_CHILD_DIM.MASTER_CODE
                                                                                  FROM VW_APO_PARENT_CHILD_DIM
                                                                                  WHERE trim(VW_APO_PARENT_CHILD_DIM.CMP_ID)::varchar = '7470')) MSTRCD ON VAPCD.MASTER_CODE = MSTRCD.MASTER_CODE)
                           ON VSF.CMP_ID = VAPCD.CMP_ID
                          AND VSF.MATL_ID = VAPCD.MATL_ID
                          WHERE VSF.GTS_VAL > 0) AS A
              INNER JOIN (SELECT DISTINCT BAR_CD,
                                 COUNT(DISTINCT MATL_ID) AS MATL_BAR_COUNT
                          FROM EDW_MATERIAL_DIM
                          WHERE NVL(BAR_CD,'NA') IN (SELECT DISTINCT NVL(BAR_CD,'NA')
                                                     FROM (SELECT COUNT(*),
                                                                  BAR_CD
                                                           FROM EDW_MATERIAL_DIM
                                                           GROUP BY BAR_CD
                                                           HAVING COUNT(*) > 1))
                          GROUP BY BAR_CD) AS B ON A.BAR_CD = B.BAR_CD
            GROUP BY A.JJ_MONTH_ID,
                     A.MASTER_CODE,
                     A.BAR_CD,
                     A.MATL_ID,
                     A.CHANNEL_DESC,
                     A.SALES_GRP_DESC,
                     A.SALES_OFFICE_DESC,
                     B.MATL_BAR_COUNT)

      WHERE SALES_RANK = 1 AND  LTRIM(BAR_CD,0) IN (SELECT LTRIM(PROD_EAN,0) FROM EDW_PERENSO_PROD_DIM WHERE TRIM(PROD_EAN)<>'' GROUP BY 1 HAVING COUNT(*) > 1)
      GROUP BY JJ_MONTH_ID,
               MASTER_CODE,
               BAR_CD,
               SALES_GRP_DESC,
               SALES_OFFICE_DESC,
               MATERIAL_ID,
               MATERIAL_COUNT,
               MAX_MATERIAL_ID,
               MASTER_CODE_COUNT,
               CHANNEL_DESC,
               GTS_VAL,
               MATL_BAR_COUNT))   ,
union_1 as (
       SELECT EPOF.ORDER_TYPE_DESC AS ORDER_TYPE,
       EPOF.DELVRY_DT,
       CASE WHEN ETD.TIME_ID IS NOT NULL
			      THEN ETD.TIME_ID
			       ELSE CAST(TO_CHAR(EPOF.DELVRY_DT::date,'YYYYMMDD') AS NUMERIC)
	     END AS TIME_ID,
       CASE WHEN ETD.JJ_YEAR IS NOT NULL
			      THEN ETD.JJ_YEAR
			      ELSE CAST(EXTRACT(YEAR FROM EPOF.DELVRY_DT::date) AS NUMERIC)
	     END AS JJ_YEAR,
       CASE WHEN ETD.JJ_QRTR IS NOT NULL
			      THEN ETD.JJ_QRTR
			      ELSE CAST(EXTRACT(QTR FROM EPOF.DELVRY_DT::date) AS NUMERIC)
	     END AS JJ_QRTR,
       CASE WHEN ETD.JJ_MNTH IS NOT NULL
			      THEN ETD.JJ_MNTH
			 ELSE CAST(EXTRACT(MON FROM EPOF.DELVRY_DT::date) AS NUMERIC)
	     END AS JJ_MNTH,
       ETD.JJ_WK,
       ETD.JJ_MNTH_WK,
       CASE WHEN ETD.JJ_MNTH_ID IS NOT NULL
		  	THEN ETD.JJ_MNTH_ID
		  	ELSE CAST((EXTRACT(YEAR FROM EPOF.DELVRY_DT::date)||LPAD(EXTRACT(MON FROM EPOF.DELVRY_DT::date),2,0)) AS NUMERIC)
	     END	AS JJ_MNTH_ID,
       ETD.JJ_MNTH_TOT,
       ETD.JJ_MNTH_DAY,
       ETD.JJ_MNTH_SHRT,
       ETD.JJ_MNTH_LONG,
       ETD.CAL_YEAR,
       ETD.CAL_QRTR,
	   CASE WHEN upper(ORDER_TYPE)='SHIPPED ORDERS'
	   THEN CAST(EXTRACT(MON FROM EPOF.ORDER_DATE::date) AS NUMERIC)
       ELSE ETD.CAL_MNTH END AS CAL_MNTH,
       ETD.CAL_WK,
       ETD.CAL_MNTH_WK,
       ETD.CAL_MNTH_ID,
       ETD.CAL_MNTH_NM,
       EPOF.PROD_KEY,
	     NULL::number AS PROD_PROBE_ID,
       EPPD.PROD_DESC,
       EPPD.PROD_ID AS PROD_SAPBW_CODE,
       EPPD.PROD_EAN,
       EPPD.PROD_JJ_FRANCHISE,
       EPPD.PROD_JJ_CATEGORY,
       EPPD.PROD_JJ_BRAND,
       EPPD.PROD_SAP_FRANCHISE,
       EPPD.PROD_SAP_PROFIT_CENTRE,
       EPPD.PROD_SAP_PRODUCT_MAJOR,
       EPPD.PROD_GROCERY_FRANCHISE,
       EPPD.PROD_GROCERY_CATEGORY,
       EPPD.PROD_GROCERY_BRAND,
       CASE
             WHEN (EPPD.PROD_ACTIVE_NZ_PHARMA != 'NOT ASSIGNED'
                  OR EPPD.PROD_ACTIVE_AU_GROCERY != 'NOT ASSIGNED'
                  OR EPPD.PROD_ACTIVE_METCASH != 'NOT ASSIGNED'
                  OR EPPD.PROD_ACTIVE_NZ_GROCERY != 'NOT ASSIGNED'
                  OR EPPD.PROD_ACTIVE_AU_PHARMA != 'NOT ASSIGNED')
             THEN 1
             ELSE 0
       END PROD_ACTIVE_STATUS,
       EPPD.PROD_PBS,
       EPPD.PROD_IMS_BRAND,
       EPPD.PROD_NZ_CODE,
       EPPD.PROD_METCASH_CODE,
	     EPPD.PROD_OLD_ID,
       EPPD.PROD_OLD_EAN,
       EPPD.PROD_TAX,
       EPPD.PROD_BWP_AUD,
       EPPD.PROD_BWP_NZD,
	     IPPBI.IDENTIFIER AS PROD_WHOLESALER_CODE,
       EPOF.ACCT_KEY AS ACCT_KEY,
	    NULL::number AS ACCT_PROBE_ID,
	     NULL AS ACCT_METCASH_STORE_CODE,
       EPAD.ACCT_DISPLAY_NAME,
       EPAD.ACCT_TYPE_DESC,
       EPAD.ACCT_STREET_1,
       EPAD.ACCT_STREET_2,
       EPAD.ACCT_STREET_3,
       EPAD.ACCT_SUBURB,
       EPAD.ACCT_POSTCODE,
       EPAD.ACCT_PHONE_NUMBER,
       EPAD.ACCT_FAX_NUMBER,
       EPAD.ACCT_EMAIL,
       EPAD.ACCT_COUNTRY,
       EPAD.ACCT_REGION,
       EPAD.ACCT_STATE,
       EPAD.ACCT_BANNER_COUNTRY,
       EPAD.ACCT_BANNER_DIVISION,
       EPAD.ACCT_BANNER_TYPE,
       EPAD.ACCT_BANNER,
       EPAD.ACCT_TYPE,
       EPAD.ACCT_SUB_TYPE,
       EPAD.ACCT_GRADE,
       EPAD.ACCT_NZ_PHARMA_COUNTRY,
       EPAD.ACCT_NZ_PHARMA_STATE,
       EPAD.ACCT_NZ_PHARMA_TERRITORY,
       EPAD.ACCT_NZ_GROC_COUNTRY,
       EPAD.ACCT_NZ_GROC_STATE,
       EPAD.ACCT_NZ_GROC_TERRITORY,
       EPAD.ACCT_SSR_COUNTRY,
       EPAD.ACCT_SSR_STATE,
       EPAD.ACCT_SSR_TEAM_LEADER,
       EPAD.ACCT_SSR_TERRITORY,
       EPAD.ACCT_SSR_SUB_TERRITORY,
       EPAD.ACCT_IND_GROC_COUNTRY,
       EPAD.ACCT_IND_GROC_STATE,
       EPAD.ACCT_IND_GROC_TERRITORY,
       EPAD.ACCT_IND_GROC_SUB_TERRITORY,
       EPAD.ACCT_AU_PHARMA_COUNTRY,
       EPAD.ACCT_AU_PHARMA_STATE,
       EPAD.ACCT_AU_PHARMA_TERRITORY,
       EPAD.ACCT_AU_PHARMA_SSR_COUNTRY,
       EPAD.ACCT_AU_PHARMA_SSR_STATE,
       EPAD.ACCT_AU_PHARMA_SSR_TERRITORY,
       CASE
              WHEN UPPER(EPAD.ACCT_IND_GROC_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_STATE
              WHEN UPPER(EPAD.ACCT_AU_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_STATE
			        WHEN UPPER(EPAD.ACCT_NZ_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_STATE
              ELSE 'NOT ASSIGNED'
       END  ACCT_TSM,
       CASE
              WHEN UPPER(EPAD.ACCT_IND_GROC_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_TERRITORY
              WHEN UPPER(EPAD.ACCT_AU_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_TERRITORY
			        WHEN UPPER(EPAD.ACCT_NZ_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_TERRITORY
              ELSE 'NOT ASSIGNED'
       END  ACCT_TERRIROTY,
	     EPAD.ACCT_STORE_CODE AS ACCT_STORE_CODE,
	     EPAD.ACCT_FAX_OPT_OUT,
       EPAD.ACCT_EMAIL_OPT_OUT,
       EPAD.ACCT_CONTACT_METHOD,
       EPAD.SSR_GRADE,
       EPOF.ORDER_KEY,
       EPOF.ORDER_DATE,
       EPOF.SENT_DT,
       EPOF.DELVRY_INSTNS,
       EPOF.BRANCH_KEY,
       EPOF.BATCH_KEY,
       EPOF.LINE_KEY,
       EPOF.UNIT_QTY,
       EPOF.ENTERED_QTY,
       EPOF.ENTERED_UNIT_KEY,
       EPOF.LIST_PRICE,
       (EPOF.ENTERED_QTY*EPOF.NIS) AS NIS,
	     --NULL AS WS_DERIVED_PRICE,
       EPOF.RRP,
       EPOF.DEAL_DESC,
       EPOF.START_DATE AS DEAL_START_DATE,
       EPOF.END_DATE AS DEAL_END_DATE,
       EPOF.SHORT_DESC AS DEAL_SHORT_DESC,
       EPOF.DISCOUNT_DESC,
       NULL AS DISCOUNT_PER,
       NULL AS DISCOUNT_AMT,
       EPOF.ORDER_HEADER_STATUS AS ORDER_STATUS,
       EPOF.ORDER_BATCH_STATUS AS BATCH_STATUS,
       EPOF.ORDER_CURRENCY_CD,
	     IPU.DISPLAY_NAME AS CREATE_USER_NAME,
	     IPU.USER_TYPE_DESC AS CREATE_USER_DESC,
       IPU.EMAIL_ADDRESS AS CREATE_USER_EMAIL_ADDRESS,
       IPDD.DISTRIBUTOR,
       IPDD.DISPLAY_NAME AS DISTRIBUTOR_DISPLAY_NAME,
       cast(EPMI.MSL_RANK  as varchar(5)) AS PHARMACY_MSL_RANK,
       EPMI.MSL_FLAG AS PHARMACY_MSL_FLAG,
       CASE WHEN ETD.JJ_MNTH_ID IS NOT NULL
			  THEN JJBR.EXCH_RATE
			  ELSE JJBRN.EXCH_RATE
	  	 END AS EXCH_RATE_TO_AUD,
		   CASE WHEN ETD.JJ_MNTH_ID IS NOT NULL
			  THEN (EPOF.ENTERED_QTY*EPOF.NIS)*JJBR.EXCH_RATE
			  ELSE (EPOF.ENTERED_QTY*EPOF.NIS)*JJBRN.EXCH_RATE
		   END AS AUD_NIS,
       CASE WHEN ETD.JJ_MNTH_ID IS NOT NULL
			  THEN BWAR.EXCH_RATE
			  ELSE BWARN.EXCH_RATE
	     END AS EXCH_RATE_TO_USD,
       CASE WHEN ETD.JJ_MNTH_ID IS NOT NULL
			  THEN (EPOF.ENTERED_QTY*EPOF.NIS)*BWAR.EXCH_RATE
			  ELSE (EPOF.ENTERED_QTY*EPOF.NIS)*BWARN.EXCH_RATE
	     END AS USD_NIS,
       NULL::number AS EXCH_RATE_TO_NZD,
       NULl::float AS NZD_NIS,
       NULL AS DISTRIBUTION_FLAG
FROM EDW_PERENSO_ORDER_FACT EPOF
left join  ETD
ON EPOF.DELVRY_DT::date = ETD.CAL_DATE::date
LEFT JOIN EDW_PERENSO_PROD_DIM EPPD ON EPOF.PROD_KEY = EPPD.PROD_KEY
LEFT JOIN EDW_PERENSO_ACCOUNT_DIM EPAD ON EPOF.ACCT_KEY = EPAD.ACCT_ID
LEFT JOIN ITG_PERENSO_DIARY_ITEM IPDI ON EPOF.DIARY_ITEM_KEY = IPDI.DIARY_ITEM_KEY
LEFT JOIN ITG_PERENSO_USERS IPU ON IPDI.CREATE_USER_KEY = IPU.USER_KEY
LEFT JOIN ITG_PERENSO_DISTRIBUTOR_DETAIL IPDD ON EPOF.BRANCH_KEY = IPDD.BRANCH_KEY
LEFT JOIN ITG_PERENSO_PROD_BRANCH_IDENTIFIER IPPBI ON EPOF.BRANCH_KEY = IPPBI.BRANCH_KEY AND EPOF.PROD_KEY = IPPBI.PROD_KEY
LEFT JOIN EDW_PS_MSL_ITEMS EPMI
ON ltrim (EPPD.PROD_EAN,0) = ltrim(EPMI.EAN,0) and upper(EPMI.RETAIL_ENVIRONMENT) = 'AU INDY PHARMACY'
and EPMI.latest_record='Y' ----(we need to add)
left JOIN (SELECT *
            FROM VW_JJBR_CURR_EXCH_DIM
            WHERE TO_CCY = 'AUD') JJBR
ON ETD.JJ_MNTH_ID = JJBR.JJ_MNTH_ID AND EPOF.ORDER_CURRENCY_CD = JJBR.FROM_CCY
left JOIN (SELECT *
            FROM VW_BWAR_CURR_EXCH_DIM
            WHERE TO_CCY = 'USD') BWAR
ON ETD.JJ_MNTH_ID = BWAR.JJ_MNTH_ID AND EPOF.ORDER_CURRENCY_CD = BWAR.FROM_CCY
left JOIN (SELECT *
			FROM VW_BWAR_CURR_EXCH_DIM
			WHERE TO_CCY = 'USD'
			AND   jj_mnth_id = (SELECT DISTINCT jj_mnth_id
									FROM EDW_TIME_DIM
									WHERE cal_date = current_timestamp()::date)) BWARN
ON EPOF.ORDER_CURRENCY_CD = BWARN.FROM_CCY
left JOIN (SELECT *
			FROM VW_JJBR_CURR_EXCH_DIM
            WHERE TO_CCY = 'AUD'
			AND   jj_mnth_id = (SELECT DISTINCT jj_mnth_id
									FROM EDW_TIME_DIM
									WHERE cal_date = current_timestamp()::date)) JJBRN
ON EPOF.ORDER_CURRENCY_CD = JJBRN.FROM_CCY
) ,
union_2 as (

SELECT 'Shipped Weekly' AS ORDER_TYPE,
       TO_DATE ((SUBSTRING(PSF.TIME_PERIOD,5,4) ||SUBSTRING (PSF.TIME_PERIOD,3,2) ||SUBSTRING (PSF.TIME_PERIOD,1,2)),'YYYYMMDD') AS DELVRY_DT,
       ETD.TIME_ID,
       ETD.JJ_YEAR,
       ETD.JJ_QRTR,
       ETD.JJ_MNTH,
       ETD.JJ_WK,
       ETD.JJ_MNTH_WK,
       ETD.JJ_MNTH_ID,
       ETD.JJ_MNTH_TOT,
       ETD.JJ_MNTH_DAY,
       ETD.JJ_MNTH_SHRT,
       ETD.JJ_MNTH_LONG,
       ETD.CAL_YEAR,
       ETD.CAL_QRTR,
       ETD.CAL_MNTH,
       ETD.CAL_WK,
       ETD.CAL_MNTH_WK,
       ETD.CAL_MNTH_ID,
       ETD.CAL_MNTH_NM,
	     EPPD.PROD_KEY,
       PSF.PRODUCT_PROBE_ID::number AS PROD_PROBE_ID,
       CASE WHEN EPPD.PROD_DESC IS NULL
			  THEN PSF.PRODUCT_DESCRIPTION
		   ELSE EPPD.PROD_DESC
	     END PROD_DESC,
       EPPD.PROD_ID AS PROD_SAPBW_CODE,
       EPPD.PROD_EAN,
       EPPD.PROD_JJ_FRANCHISE,
       EPPD.PROD_JJ_CATEGORY,
       EPPD.PROD_JJ_BRAND,
       EPPD.PROD_SAP_FRANCHISE,
       EPPD.PROD_SAP_PROFIT_CENTRE,
       EPPD.PROD_SAP_PRODUCT_MAJOR,
       EPPD.PROD_GROCERY_FRANCHISE,
       EPPD.PROD_GROCERY_CATEGORY,
       EPPD.PROD_GROCERY_BRAND,
       CASE
            WHEN (EPPD.PROD_ACTIVE_NZ_PHARMA != 'NOT ASSIGNED'
                OR EPPD.PROD_ACTIVE_AU_GROCERY != 'NOT ASSIGNED'
                OR EPPD.PROD_ACTIVE_METCASH != 'NOT ASSIGNED'
                OR EPPD.PROD_ACTIVE_NZ_GROCERY != 'NOT ASSIGNED'
                OR EPPD.PROD_ACTIVE_AU_PHARMA != 'NOT ASSIGNED')
            THEN 1
            ELSE 0
       END PROD_ACTIVE_STATUS,
       EPPD.PROD_PBS,
       EPPD.PROD_IMS_BRAND,
       EPPD.PROD_NZ_CODE,
       EPPD.PROD_METCASH_CODE,
	     EPPD.PROD_OLD_ID,
       EPPD.PROD_OLD_EAN,
       EPPD.PROD_TAX,
       EPPD.PROD_BWP_AUD,
       EPPD.PROD_BWP_NZD,
	     NULL AS PROD_WHOLESALER_CODE,
	     EPAD.ACCT_ID AS ACCT_KEY,
	     PSF.STORE_PROBE_ID::number AS ACCT_PROBE_ID,
	     NULL AS ACCT_METCASH_STORE_CODE,
       CASE WHEN EPAD.ACCT_DISPLAY_NAME IS NULL
			     THEN PSF.STORE_NAME
		 	 ELSE EPAD.ACCT_DISPLAY_NAME
	     END ACCT_DISPLAY_NAME,
       EPAD.ACCT_TYPE_DESC,
       EPAD.ACCT_STREET_1,
       EPAD.ACCT_STREET_2,
       EPAD.ACCT_STREET_3,
       EPAD.ACCT_SUBURB,
       EPAD.ACCT_POSTCODE,
       EPAD.ACCT_PHONE_NUMBER,
       EPAD.ACCT_FAX_NUMBER,
       EPAD.ACCT_EMAIL,
       EPAD.ACCT_COUNTRY,
       EPAD.ACCT_REGION,
       EPAD.ACCT_STATE,
       EPAD.ACCT_BANNER_COUNTRY,
       EPAD.ACCT_BANNER_DIVISION,
       EPAD.ACCT_BANNER_TYPE,
       EPAD.ACCT_BANNER,
       EPAD.ACCT_TYPE,
       EPAD.ACCT_SUB_TYPE,
       EPAD.ACCT_GRADE,
       EPAD.ACCT_NZ_PHARMA_COUNTRY,
       EPAD.ACCT_NZ_PHARMA_STATE,
       EPAD.ACCT_NZ_PHARMA_TERRITORY,
       EPAD.ACCT_NZ_GROC_COUNTRY,
       EPAD.ACCT_NZ_GROC_STATE,
       EPAD.ACCT_NZ_GROC_TERRITORY,
       EPAD.ACCT_SSR_COUNTRY,
       EPAD.ACCT_SSR_STATE,
       EPAD.ACCT_SSR_TEAM_LEADER,
       EPAD.ACCT_SSR_TERRITORY,
       EPAD.ACCT_SSR_SUB_TERRITORY,
       EPAD.ACCT_IND_GROC_COUNTRY,
       EPAD.ACCT_IND_GROC_STATE,
       EPAD.ACCT_IND_GROC_TERRITORY,
       EPAD.ACCT_IND_GROC_SUB_TERRITORY,
       EPAD.ACCT_AU_PHARMA_COUNTRY,
       EPAD.ACCT_AU_PHARMA_STATE,
       EPAD.ACCT_AU_PHARMA_TERRITORY,
       EPAD.ACCT_AU_PHARMA_SSR_COUNTRY,
       EPAD.ACCT_AU_PHARMA_SSR_STATE,
       EPAD.ACCT_AU_PHARMA_SSR_TERRITORY,
       CASE
         WHEN UPPER(EPAD.ACCT_IND_GROC_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_STATE
         WHEN UPPER(EPAD.ACCT_AU_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_STATE
         WHEN UPPER(EPAD.ACCT_NZ_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_STATE
         ELSE 'NOT ASSIGNED'
       END ACCT_TSM,
       CASE
         WHEN UPPER(EPAD.ACCT_IND_GROC_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_TERRITORY
         WHEN UPPER(EPAD.ACCT_AU_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_TERRITORY
         WHEN UPPER(EPAD.ACCT_NZ_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_TERRITORY
         ELSE 'NOT ASSIGNED'
       END ACCT_TERRIROTY,
	     EPAD.ACCT_STORE_CODE AS ACCT_STORE_CODE,
	     EPAD.ACCT_FAX_OPT_OUT,
       EPAD.ACCT_EMAIL_OPT_OUT,
       EPAD.ACCT_CONTACT_METHOD,
       NULL as SSR_GRADE,
       NULL::number AS ORDER_KEY,
       NULL AS ORDER_DATE,--PSF.TIME_PERIOD AS ORDER_DATE, -- Different from OTHERS
       NULL AS SENT_DT,
       NULL AS DELVRY_INSTNS,
       null::number AS BRANCH_KEY,
       null::number AS BATCH_KEY,
       null::number AS LINE_KEY,
       PSF.UNITS AS UNIT_QTY,
       null::number AS ENTERED_QTY,
       null::number AS ENTERED_UNIT_KEY,
       PSF.DERIVED_PRICE AS LIST_PRICE,
       PSF.AMOUNT AS NIS,
       --PSF.DERIVED_PRICE AS WS_DERIVED_PRICE, -- Check for this column
       null::number AS RRP,
       NULL AS DEAL_DESC,
       NULL AS DEAL_START_DATE,
       NULL AS DEAL_END_DATE,
       NULL AS DEAL_SHORT_DESC,
       NULL AS DISCOUNT_DESC,
       NULL AS DISCOUNT_PER,
       NULL AS DISCOUNT_AMT,
       NULL AS ORDER_STATUS,
       NULL AS BATCH_STATUS,
       PSF.CRNY_CD AS ORDER_CURRENCY_CD,
	     NULL AS CREATE_USER_NAME,
	     NULL AS CREATE_USER_DESC,
       NULL AS CREATE_USER_EMAIL_ADDRESS,
       NULL AS DISTRIBUTOR,
       NULL AS DISTRIBUTOR_DISPLAY_NAME,
       cast(EPMI.MSL_RANK  as varchar(5)) AS PHARMACY_MSL_RANK,
       EPMI.MSL_FLAG AS PHARMACY_MSL_FLAG,
       JJBR.EXCH_RATE AS EXCH_RATE_TO_AUD,
       psf.amount*JJBR.EXCH_RATE AS AUD_NIS,
       BWAR.EXCH_RATE AS EXCH_RATE_TO_USD,
       psf.amount*BWAR.EXCH_RATE AS USD_NIS,
       null::number AS EXCH_RATE_TO_NZD,
      null::float AS NZD_NIS,
       NULL AS DISTRIBUTION_FLAG
FROM (SELECT PSF.*,
       EPSR.ACCT_COUNTRY,
       NVL(DECODE(EPSR.ACCT_COUNTRY,'Australia','AUD','New Zealand','NZD'),'NOT ASSIGNED') CRNY_CD
      FROM EDW_PHARM_SELLOUT_FACT PSF,
      EDW_PERENSO_ACCOUNT_PROBEID_DIM EPSR
      WHERE PSF.STORE_PROBE_ID = EPSR.ACCOUNT_PROBE_ID(+))PSF
inner join 	 ETD
ON (SUBSTRING(psf.time_period,5,4) ||substring (psf.time_period,3,2) ||substring (psf.time_period,1,2))::number = etd.time_id
LEFT JOIN EDW_PERENSO_ACCOUNT_PROBEID_DIM EPAD ON psf.store_probe_id = EPAD.account_probe_id
LEFT JOIN EDW_PERENSO_PROD_PROBEID_DIM EPPD ON psf.product_probe_id = eppd.product_probe_id
LEFT JOIN EDW_PS_MSL_ITEMS EPMI
ON ltrim(eppd.prod_ean,0) = ltrim(EPMI.ean,0) and upper(epmi.retail_environment) = 'AU INDY PHARMACY'
and EPMI.latest_record='Y'-----(we need to add)
LEFT JOIN (SELECT *
            FROM VW_JJBR_CURR_EXCH_DIM
            WHERE TO_CCY = 'AUD') JJBR
ON ETD.JJ_MNTH_ID = JJBR.JJ_MNTH_ID AND psf.CRNY_CD = JJBR.FROM_CCY
LEFT JOIN (SELECT *
             FROM VW_BWAR_CURR_EXCH_DIM
             WHERE TO_CCY = 'USD') BWAR
ON ETD.JJ_MNTH_ID = BWAR.JJ_MNTH_ID AND psf.CRNY_CD = BWAR.FROM_CCY
) ,
union_3 as (SELECT ORDER_TYPE,
       DELVRY_DT,
       TIME_ID,
       JJ_YEAR,
       JJ_QRTR,
       JJ_MNTH,
       JJ_WK,
       JJ_MNTH_WK,
       JJ_MNTH_ID,
       JJ_MNTH_TOT,
       JJ_MNTH_DAY,
       JJ_MNTH_SHRT,
       JJ_MNTH_LONG,
       CAL_YEAR,
       CAL_QRTR,
       CAL_MNTH,
       CAL_WK,
       CAL_MNTH_WK,
       CAL_MNTH_ID,
       CAL_MNTH_NM,
       PROD_KEY,
	     null::numeric AS PROD_PROBE_ID,
       PROD_DESC,
       PROD_SAPBW_CODE,
       PROD_EAN,
       PROD_JJ_FRANCHISE,
       PROD_JJ_CATEGORY,
       PROD_JJ_BRAND,
       PROD_SAP_FRANCHISE,
       PROD_SAP_PROFIT_CENTRE,
       PROD_SAP_PRODUCT_MAJOR,
       PROD_GROCERY_FRANCHISE,
       PROD_GROCERY_CATEGORY,
       PROD_GROCERY_BRAND,
       PROD_ACTIVE_STATUS,
       PROD_PBS,
       PROD_IMS_BRAND,
       PROD_NZ_CODE,
       PROD_METCASH_CODE,
	     PROD_OLD_ID,
       PROD_OLD_EAN,
       PROD_TAX,
       PROD_BWP_AUD,
       PROD_BWP_NZD,
	     NULL AS PROD_WHOLESALER_CODE,
       ACCT_KEY,
	     null:number AS ACCT_PROBE_ID,
	     NULL AS ACCT_METCASH_STORE_CODE,
       ACCT_DISPLAY_NAME,
       ACCT_TYPE_DESC,
       ACCT_STREET_1,
       ACCT_STREET_2,
       ACCT_STREET_3,
       ACCT_SUBURB,
       ACCT_POSTCODE,
       ACCT_PHONE_NUMBER,
       ACCT_FAX_NUMBER,
       ACCT_EMAIL,
       ACCT_COUNTRY,
       ACCT_REGION,
       ACCT_STATE,
       ACCT_BANNER_COUNTRY,
       ACCT_BANNER_DIVISION,
       ACCT_BANNER_TYPE,
       ACCT_BANNER,
       ACCT_TYPE,
       ACCT_SUB_TYPE,
       ACCT_GRADE,
       ACCT_NZ_PHARMA_COUNTRY,
       ACCT_NZ_PHARMA_STATE,
       ACCT_NZ_PHARMA_TERRITORY,
       ACCT_NZ_GROC_COUNTRY,
       ACCT_NZ_GROC_STATE,
       ACCT_NZ_GROC_TERRITORY,
       ACCT_SSR_COUNTRY,
       ACCT_SSR_STATE,
       ACCT_SSR_TEAM_LEADER,
       ACCT_SSR_TERRITORY,
       ACCT_SSR_SUB_TERRITORY,
       ACCT_IND_GROC_COUNTRY,
       ACCT_IND_GROC_STATE,
       ACCT_IND_GROC_TERRITORY,
       ACCT_IND_GROC_SUB_TERRITORY,
       ACCT_AU_PHARMA_COUNTRY,
       ACCT_AU_PHARMA_STATE,
       ACCT_AU_PHARMA_TERRITORY,
       ACCT_AU_PHARMA_SSR_COUNTRY,
       ACCT_AU_PHARMA_SSR_STATE,
       ACCT_AU_PHARMA_SSR_TERRITORY,
       ACCT_TSM,
       ACCT_TERRIROTY,
	     ACCT_STORE_CODE,
	     ACCT_FAX_OPT_OUT,
       ACCT_EMAIL_OPT_OUT,
       ACCT_CONTACT_METHOD,
       NULL as SSR_GRADE,
       ORDER_KEY,
       ORDER_DATE,
       SENT_DT,
       DELVRY_INSTNS,
       BRANCH_KEY,
       BATCH_KEY,
       LINE_KEY,
       UNIT_QTY,
       ENTERED_QTY,
       ENTERED_UNIT_KEY,
       LIST_PRICE,
       NIS,
       RRP,
       DEAL_DESC,
       DEAL_START_DATE,
       DEAL_END_DATE,
       DEAL_SHORT_DESC,
       DISCOUNT_DESC,
       DISCOUNT_PER,
       DISCOUNT_AMT,
       ORDER_STATUS,
       BATCH_STATUS,
       ORDER_CURRENCY_CD,
	     NULL AS CREATE_USER_NAME,
	     NULL AS CREATE_USER_DESC,
       NULL AS CREATE_USER_EMAIL_ADDRESS,
       DISTRIBUTOR,
       DISTRIBUTOR_DISPLAY_NAME,
       PHARMACY_MSL_RANK,
       PHARMACY_MSL_FLAG,
       EXCH_RATE_TO_AUD,
       AUD_NIS,
       EXCH_RATE_TO_USD,
       USD_NIS,
       null::number AS EXCH_RATE_TO_NZD,
       null::float AS NZD_NIS,
       NULL AS DISTRIBUTION_FLAG
FROM ITG_IMS_FIELD_ORDER_HISTORY) ,
union_4 as (
       SELECT 'Metcash Weekly' AS ORDER_TYPE,
       BASE.CAL_DATE AS DELVRY_DT,
       ETD.TIME_ID,
       ETD.JJ_YEAR,
       ETD.JJ_QRTR,
       ETD.JJ_MNTH,
       ETD.JJ_WK,
       ETD.JJ_MNTH_WK,
       ETD.JJ_MNTH_ID,
       ETD.JJ_MNTH_TOT,
       ETD.JJ_MNTH_DAY,
       ETD.JJ_MNTH_SHRT,
       ETD.JJ_MNTH_LONG,
       ETD.CAL_YEAR,
       ETD.CAL_QRTR,
       ETD.CAL_MNTH,
       ETD.CAL_WK,
       ETD.CAL_MNTH_WK,
       ETD.CAL_MNTH_ID,
       ETD.CAL_MNTH_NM,
       EPPD.PROD_KEY,
       NULL AS PROD_PROBE_ID,
       CASE
          WHEN EPPD.PROD_DESC IS NULL
          THEN BASE.PRODUCT
          ELSE EPPD.PROD_DESC
       END PROD_DESC,
       EPPD.PROD_ID AS PROD_SAPBW_CODE,
       EPPD.PROD_EAN,
       EPPD.PROD_JJ_FRANCHISE,
       EPPD.PROD_JJ_CATEGORY,
       EPPD.PROD_JJ_BRAND,
       EPPD.PROD_SAP_FRANCHISE,
       EPPD.PROD_SAP_PROFIT_CENTRE,
       EPPD.PROD_SAP_PRODUCT_MAJOR,
       EPPD.PROD_GROCERY_FRANCHISE,
       EPPD.PROD_GROCERY_CATEGORY,
       EPPD.PROD_GROCERY_BRAND,
       CASE
         WHEN (EPPD.PROD_ACTIVE_NZ_PHARMA != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_AU_GROCERY != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_METCASH != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_NZ_GROCERY != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_AU_PHARMA != 'NOT ASSIGNED')
         THEN 1
         ELSE 0
       END PROD_ACTIVE_STATUS,
       EPPD.PROD_PBS,
       EPPD.PROD_IMS_BRAND,
       EPPD.PROD_NZ_CODE,
       BASE.PRODUCT_ID::varchar AS PROD_METCASH_CODE,
       EPPD.PROD_OLD_ID,
       EPPD.PROD_OLD_EAN,
       EPPD.PROD_TAX,
       EPPD.PROD_BWP_AUD,
       EPPD.PROD_BWP_NZD,
       NULL AS PROD_WHOLESALER_CODE,
       EPAD.ACCT_ID AS ACCT_KEY,
       NULL AS ACCT_PROBE_ID,
       BASE.METCASH_STORE_CODE AS ACCT_METCASH_STORE_CODE,
       CASE
           WHEN EPAD.ACCT_DISPLAY_NAME IS NULL
           THEN METCASH_DISPLAY_NAME
           ELSE EPAD.ACCT_DISPLAY_NAME
       END ACCT_DISPLAY_NAME,
       EPAD.ACCT_TYPE_DESC,
       EPAD.ACCT_STREET_1,
       EPAD.ACCT_STREET_2,
       EPAD.ACCT_STREET_3,
       EPAD.ACCT_SUBURB,
       EPAD.ACCT_POSTCODE,
       EPAD.ACCT_PHONE_NUMBER,
       EPAD.ACCT_FAX_NUMBER,
       EPAD.ACCT_EMAIL,
       EPAD.ACCT_COUNTRY,
       EPAD.ACCT_REGION,
       EPAD.ACCT_STATE,
       EPAD.ACCT_BANNER_COUNTRY,
       EPAD.ACCT_BANNER_DIVISION,
       EPAD.ACCT_BANNER_TYPE,
	     CASE
           WHEN EPAD.ACCT_BANNER IS NULL
           THEN BASE.BANNER
           ELSE EPAD.ACCT_BANNER
       END ACCT_BANNER,
	     EPAD.ACCT_TYPE,
       EPAD.ACCT_SUB_TYPE,
       EPAD.ACCT_GRADE,
       EPAD.ACCT_NZ_PHARMA_COUNTRY,
       EPAD.ACCT_NZ_PHARMA_STATE,
       EPAD.ACCT_NZ_PHARMA_TERRITORY,
       EPAD.ACCT_NZ_GROC_COUNTRY,
       EPAD.ACCT_NZ_GROC_STATE,
       EPAD.ACCT_NZ_GROC_TERRITORY,
       EPAD.ACCT_SSR_COUNTRY,
       EPAD.ACCT_SSR_STATE,
       EPAD.ACCT_SSR_TEAM_LEADER,
       EPAD.ACCT_SSR_TERRITORY,
       EPAD.ACCT_SSR_SUB_TERRITORY,
       EPAD.ACCT_IND_GROC_COUNTRY,
       EPAD.ACCT_IND_GROC_STATE,
       EPAD.ACCT_IND_GROC_TERRITORY,
       EPAD.ACCT_IND_GROC_SUB_TERRITORY,
       EPAD.ACCT_AU_PHARMA_COUNTRY,
       EPAD.ACCT_AU_PHARMA_STATE,
       EPAD.ACCT_AU_PHARMA_TERRITORY,
       EPAD.ACCT_AU_PHARMA_SSR_COUNTRY,
       EPAD.ACCT_AU_PHARMA_SSR_STATE,
       EPAD.ACCT_AU_PHARMA_SSR_TERRITORY,
       CASE
         WHEN UPPER(EPAD.ACCT_IND_GROC_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_STATE
         WHEN UPPER(EPAD.ACCT_AU_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_STATE
         WHEN UPPER(EPAD.ACCT_NZ_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_STATE
         ELSE 'NOT ASSIGNED'
       END ACCT_TSM,
       CASE
         WHEN UPPER(EPAD.ACCT_IND_GROC_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_TERRITORY
         WHEN UPPER(EPAD.ACCT_AU_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_TERRITORY
         WHEN UPPER(EPAD.ACCT_NZ_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_TERRITORY
         ELSE 'NOT ASSIGNED'
       END ACCT_TERRIROTY,
       EPAD.ACCT_STORE_CODE,
       EPAD.ACCT_FAX_OPT_OUT,
       EPAD.ACCT_EMAIL_OPT_OUT,
       EPAD.ACCT_CONTACT_METHOD,
       NULL as SSR_GRADE,
       NULL::number AS ORDER_KEY,
       NULL AS ORDER_DATE,
       NULL AS SENT_DT,
       NULL AS DELVRY_INSTNS,
       Null::number AS BRANCH_KEY,
       Null::number AS BATCH_KEY,
       Null::number AS LINE_KEY,
       BASE.GROSS_UNITS AS UNIT_QTY,
       Null::number AS ENTERED_QTY,
       Null::number AS ENTERED_UNIT_KEY,
       Null::number AS LIST_PRICE,
       BASE.GROSS_SALES AS NIS,
       --PSF.DERIVED_PRICE AS WS_DERIVED_PRICE, -- Check for this column
       Null::number AS RRP,
       NULL AS DEAL_DESC,
       NULL AS DEAL_START_DATE,
       NULL AS DEAL_END_DATE,
       NULL AS DEAL_SHORT_DESC,
       NULL AS DISCOUNT_DESC,
       NULL AS DISCOUNT_PER,
       NULL AS DISCOUNT_AMT,
       NULL AS ORDER_STATUS,
       NULL AS BATCH_STATUS,
       BASE.CRNY_CD AS ORDER_CURRENCY_CD,
       NULL AS CREATE_USER_NAME,
       NULL AS CREATE_USER_DESC,
       NULL AS CREATE_USER_EMAIL_ADDRESS,
       NULL AS DISTRIBUTOR,
       NULL AS DISTRIBUTOR_DISPLAY_NAME,
       cast(EPMI.MSL_RANK  as varchar(5)) AS PHARMACY_MSL_RANK,
       EPMI.MSL_FLAG AS PHARMACY_MSL_FLAG,
       JJBR.EXCH_RATE AS EXCH_RATE_TO_AUD,
       BASE.GROSS_SALES*JJBR.EXCH_RATE AS AUD_NIS,
       BWAR.EXCH_RATE AS EXCH_RATE_TO_USD,
       BASE.GROSS_SALES*BWAR.EXCH_RATE AS USD_NIS,
       Null::number AS EXCH_RATE_TO_NZD,
       Null::float AS NZD_NIS,
       NULL AS DISTRIBUTION_FLAG
FROM (SELECT EMIGF.CAL_DATE,
             EMIGF.JJ_WK,
             EMIGF.WEEK_NUM,
             EMIGF.MONTH_NUMBER,
             EMIGF.SUPP_ID,
             EMIGF.SUPP_NAME,
             EMIGF.STATE,
             EMIGF.BANNER_ID,
             EMIGF.BANNER,
             EMIGF.CUSTOMER_ID AS METCASH_STORE_CODE,
             EMIGF.CUSTOMER AS METCASH_DISPLAY_NAME,
             EMIGF.PRODUCT_ID,
             EMIGF.PRODUCT,
             EMIGF.GROSS_SALES,
             EMIGF.GROSS_CASES,
             EMIGF.GROSS_UNITS,
             EPAD.ACCT_COUNTRY,
             EPAD.ACCT_STATE,
             EMIGF.UNIT,
             EMIGF.BASE_UOM,
             EMIGF.FROM_UOM,
             EMIGF.TO_UOM,
             NVL(DECODE(EPAD.ACCT_COUNTRY,'Australia','AUD','New Zealand','NZD'),'AUD') CRNY_CD --'NOT ASSIGNED'
             FROM EDW_METCASH_IND_GROCERY_FACT EMIGF,
           (SELECT *
				FROM EDW_PERENSO_ACCOUNT_METCASH_DIM
				WHERE ACCT_METCASH_ID NOT IN (SELECT ACCT_METCASH_ID
                          FROM EDW_PERENSO_ACCOUNT_METCASH_DIM
                          GROUP BY ACCT_METCASH_ID
                          HAVING COUNT(*) > 1)) EPAD
      WHERE EMIGF.CUSTOMER_ID = EPAD.ACCT_METCASH_ID(+)
      --AND EMIGF.CUSTOMER = EPAD.ACCT_DISPLAY_NAME(+)
	  ) BASE
inner join  ETD
on BASE.CAL_DATE = ETD.CAL_DATE
LEFT JOIN (SELECT *
FROM EDW_PERENSO_ACCOUNT_METCASH_DIM
WHERE ACCT_METCASH_ID NOT IN (SELECT ACCT_METCASH_ID
                              FROM EDW_PERENSO_ACCOUNT_METCASH_DIM
                              GROUP BY ACCT_METCASH_ID
                              HAVING COUNT(*) > 1)) EPAD
ON rtrim(BASE.METCASH_STORE_CODE) = rtrim(EPAD.ACCT_METCASH_ID)
-- LEFT JOIN EDW_PERENSO_PROD_DIM EPPD
-- ON BASE.PRODUCT_ID::varchar = LTRIM(EPPD.PROD_METCASH_CODE,0)
LEFT JOIN edw_perenso_prod_metcashid_dim EPPD ------------------added as part od metcash mapping issue
ON BASE.PRODUCT_ID::varchar = LTRIM(EPPD.product_probe_id,0) 
LEFT JOIN EDW_PS_MSL_ITEMS EPMI
ON ltrim(EPPD.PROD_EAN,0) = ltrim(EPMI.EAN,0) and upper(EPMI.RETAIL_ENVIRONMENT) = 'AU INDY PHARMACY'
and EPMI.latest_record='Y' ----(we need to add)
LEFT JOIN (SELECT *
            FROM VW_JJBR_CURR_EXCH_DIM
            WHERE TO_CCY = 'AUD') JJBR
ON ETD.JJ_MNTH_ID = JJBR.JJ_MNTH_ID AND BASE.CRNY_CD = JJBR.FROM_CCY
LEFT JOIN (SELECT *
            FROM VW_BWAR_CURR_EXCH_DIM
            WHERE TO_CCY = 'USD') BWAR
ON ETD.JJ_MNTH_ID = BWAR.JJ_MNTH_ID AND BASE.CRNY_CD = BWAR.FROM_CCY
) ,
union_5 as (
--For single PROD EAN
SELECT 'Foodstuffs' AS ORDER_TYPE,
       BASE.CAL_DATE AS DELVRY_DT,
       ETD.TIME_ID,
       ETD.JJ_YEAR,
       ETD.JJ_QRTR,
       ETD.JJ_MNTH,
       ETD.JJ_WK,
       ETD.JJ_MNTH_WK,
       ETD.JJ_MNTH_ID,
       ETD.JJ_MNTH_TOT,
       ETD.JJ_MNTH_DAY,
       ETD.JJ_MNTH_SHRT,
       ETD.JJ_MNTH_LONG,
       ETD.CAL_YEAR,
       ETD.CAL_QRTR,
       ETD.CAL_MNTH,
       ETD.CAL_WK,
       ETD.CAL_MNTH_WK,
       ETD.CAL_MNTH_ID,
       ETD.CAL_MNTH_NM,
       EPPD.PROD_KEY,
       NULL AS PROD_PROBE_ID,
       NVL(EPPD.PROD_DESC,BASE.ARTICLE_DESCRIPTION) AS PROD_DESC,
       EPPD.PROD_ID AS PROD_SAPBW_CODE,
       BASE.PROD_EAN,
       EPPD.PROD_JJ_FRANCHISE,
       EPPD.PROD_JJ_CATEGORY,
       EPPD.PROD_JJ_BRAND,
       EPPD.PROD_SAP_FRANCHISE,
       EPPD.PROD_SAP_PROFIT_CENTRE,
       EPPD.PROD_SAP_PRODUCT_MAJOR,
       EPPD.PROD_GROCERY_FRANCHISE,
       EPPD.PROD_GROCERY_CATEGORY,
       EPPD.PROD_GROCERY_BRAND,
       CASE
         WHEN (EPPD.PROD_ACTIVE_NZ_PHARMA != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_AU_GROCERY != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_METCASH != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_NZ_GROCERY != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_AU_PHARMA != 'NOT ASSIGNED')
         THEN 1
         ELSE 0
       END PROD_ACTIVE_STATUS,
       EPPD.PROD_PBS,
       EPPD.PROD_IMS_BRAND,
       EPPD.PROD_NZ_CODE,
       EPPD.PROD_METCASH_CODE,
       EPPD.PROD_OLD_ID,
       EPPD.PROD_OLD_EAN,
       EPPD.PROD_TAX,
       EPPD.PROD_BWP_AUD,
       EPPD.PROD_BWP_NZD,
       NULL AS PROD_WHOLESALER_CODE,
       EPAD.ACCT_ID AS ACCT_KEY,
       NULL AS ACCT_PROBE_ID,
       NULL AS ACCT_METCASH_STORE_CODE,
       NVL(EPAD.ACCT_DISPLAY_NAME,BASE.STORE_NAME) AS ACCT_DISPLAY_NAME ,
       EPAD.ACCT_TYPE_DESC,
       EPAD.ACCT_STREET_1,
       EPAD.ACCT_STREET_2,
       EPAD.ACCT_STREET_3,
       EPAD.ACCT_SUBURB,
       EPAD.ACCT_POSTCODE,
       EPAD.ACCT_PHONE_NUMBER,
       EPAD.ACCT_FAX_NUMBER,
       EPAD.ACCT_EMAIL,
       EPAD.ACCT_COUNTRY,
       EPAD.ACCT_REGION,
       EPAD.ACCT_STATE,
       EPAD.ACCT_BANNER_COUNTRY,
       EPAD.ACCT_BANNER_DIVISION,
       EPAD.ACCT_BANNER_TYPE,
	     EPAD.ACCT_BANNER,
	     EPAD.ACCT_TYPE,
       EPAD.ACCT_SUB_TYPE,
       EPAD.ACCT_GRADE,
       EPAD.ACCT_NZ_PHARMA_COUNTRY,
       EPAD.ACCT_NZ_PHARMA_STATE,
       EPAD.ACCT_NZ_PHARMA_TERRITORY,
       EPAD.ACCT_NZ_GROC_COUNTRY,
       EPAD.ACCT_NZ_GROC_STATE,
       EPAD.ACCT_NZ_GROC_TERRITORY,
       EPAD.ACCT_SSR_COUNTRY,
       EPAD.ACCT_SSR_STATE,
       EPAD.ACCT_SSR_TEAM_LEADER,
       EPAD.ACCT_SSR_TERRITORY,
       EPAD.ACCT_SSR_SUB_TERRITORY,
       EPAD.ACCT_IND_GROC_COUNTRY,
       EPAD.ACCT_IND_GROC_STATE,
       EPAD.ACCT_IND_GROC_TERRITORY,
       EPAD.ACCT_IND_GROC_SUB_TERRITORY,
       EPAD.ACCT_AU_PHARMA_COUNTRY,
       EPAD.ACCT_AU_PHARMA_STATE,
       EPAD.ACCT_AU_PHARMA_TERRITORY,
       EPAD.ACCT_AU_PHARMA_SSR_COUNTRY,
       EPAD.ACCT_AU_PHARMA_SSR_STATE,
       EPAD.ACCT_AU_PHARMA_SSR_TERRITORY,
       CASE
         WHEN UPPER(EPAD.ACCT_IND_GROC_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_STATE
         WHEN UPPER(EPAD.ACCT_AU_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_STATE
         WHEN UPPER(EPAD.ACCT_NZ_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_STATE
         ELSE 'NOT ASSIGNED'
       END ACCT_TSM,
       CASE
         WHEN UPPER(EPAD.ACCT_IND_GROC_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_TERRITORY
         WHEN UPPER(EPAD.ACCT_AU_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_TERRITORY
         WHEN UPPER(EPAD.ACCT_NZ_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_TERRITORY
         ELSE 'NOT ASSIGNED'
       END ACCT_TERRIROTY,
       BASE.STORE_CD AS ACCT_STORE_CODE,
       EPAD.ACCT_FAX_OPT_OUT,
       EPAD.ACCT_EMAIL_OPT_OUT,
       EPAD.ACCT_CONTACT_METHOD,
       NULL AS SSR_GRADE,
       null::number AS ORDER_KEY,
       null::timestamp_ntz AS ORDER_DATE,
       null::timestamp_ntz AS SENT_DT,
       NULL AS DELVRY_INSTNS,
       null::number AS BRANCH_KEY,
       null::number AS BATCH_KEY,
       null::number AS LINE_KEY,
       BASE.SLS_UNIT AS UNIT_QTY,
       null::number AS ENTERED_QTY,
       null::number AS ENTERED_UNIT_KEY,
       null::number AS LIST_PRICE,
       BASE.SLS_VALUE AS NIS,
       --PSF.DERIVED_PRICE AS WS_DERIVED_PRICE, -- Check for this column
       null::number AS RRP,
       NULL AS DEAL_DESC,
       NULL AS DEAL_START_DATE,
       NULL AS DEAL_END_DATE,
       NULL AS DEAL_SHORT_DESC,
       NULL AS DISCOUNT_DESC,
       NULL AS DISCOUNT_PER,
       NULL AS DISCOUNT_AMT,
       NULL AS ORDER_STATUS,
       NULL AS BATCH_STATUS,
       BASE.CRNCY AS ORDER_CURRENCY_CD,
       NULL AS CREATE_USER_NAME,
       NULL AS CREATE_USER_DESC,
       NULL AS CREATE_USER_EMAIL_ADDRESS,
       NULL AS DISTRIBUTOR,
       NULL AS DISTRIBUTOR_DISPLAY_NAME,
       NULL AS PHARMACY_MSL_RANK,
       NULL AS PHARMACY_MSL_FLAG,
       null::number AS EXCH_RATE_TO_AUD,
       null::float AS AUD_NIS,
       BWAR.EXCH_RATE AS EXCH_RATE_TO_USD,
       BASE.SLS_VALUE*BWAR.EXCH_RATE AS USD_NIS,
       JJBR.EXCH_RATE AS EXCH_RATE_TO_NZD,
       BASE.SLS_VALUE*JJBR.EXCH_RATE AS NZD_NIS,
       NULL AS DISTRIBUTION_FLAG
FROM (SELECT * FROM (SELECT CAL_DT AS CAL_DATE,
             'FSSI' ACCT_BANNER,
             PROD_EAN,
			 ARTICLE_DESCRIPTION,
             STORE_CD,
             STORE_NAME,
             SLS_UNIT,
             SLS_VALUE,
             CRNCY
       FROM ITG_PERENSO_FSSI_SALES
       UNION ALL
       SELECT BILL_DT AS CAL_DATE,
              'FSNI' ACCT_BANNER,
              PROD_EAN,
			  ARTICLE_DESCRIPTION,
              STORE_CD,
              STORE_NAME,
              BASE_QTY AS SLS_UNIT,
              SLS_VALUE,
              CRNCY
        FROM ITG_PERENSO_FSNI_SALES)
        WHERE PROD_EAN NOT IN (SELECT LTRIM(PROD_EAN,0) FROM EDW_PERENSO_PROD_DIM WHERE TRIM(PROD_EAN)<>'' GROUP BY 1 HAVING COUNT(*) > 1)) BASE
LEFT JOIN  ETD
on BASE.CAL_DATE = ETD.CAL_DATE
LEFT JOIN (SELECT * FROM EDW_PERENSO_ACCOUNT_HIST_DIM WHERE ACCT_BANNER_TYPE IN ('FSSI','FSNI') and
ACCT_STORE_CODE NOT in (SELECT ACCT_STORE_CODE from EDW_PERENSO_ACCOUNT_HIST_DIM where ACCT_BANNER_TYPE IN ('FSSI','FSNI') group by 1 HAVING COUNT(*) > 1 )) EPAD
ON BASE.STORE_CD = EPAD.ACCT_STORE_CODE
LEFT JOIN (SELECT * FROM EDW_PERENSO_PROD_DIM WHERE LTRIM(PROD_EAN,0) NOT IN (SELECT LTRIM(PROD_EAN,0) FROM EDW_PERENSO_PROD_DIM WHERE TRIM(PROD_EAN)<>'' GROUP BY 1 HAVING COUNT(*) > 1)) EPPD
ON LTRIM(BASE.PROD_EAN,0) =LTRIM(EPPD.PROD_EAN,0)
LEFT JOIN (SELECT *
            FROM VW_JJBR_CURR_EXCH_DIM
            WHERE TO_CCY = 'NZD') JJBR
ON ETD.JJ_MNTH_ID = JJBR.JJ_MNTH_ID AND BASE.CRNCY = JJBR.FROM_CCY
LEFT JOIN (SELECT *
            FROM VW_BWAR_CURR_EXCH_DIM
            WHERE TO_CCY = 'USD') BWAR
ON ETD.JJ_MNTH_ID = BWAR.JJ_MNTH_ID AND BASE.CRNCY = BWAR.FROM_CCY
WHERE DATEDIFF(MONTH,BASE.CAL_DATE,CURRENT_DATE) <= 25) ,
subq_1 as (
SELECT CAL_DT AS CAL_DATE,
             'FSSI' ACCT_BANNER,
             'FOODSTUFF-STH ISLAND' AS SALES_OFFICE,
             PROD_EAN,
             ARTICLE_DESCRIPTION,
             STORE_CD,
             STORE_NAME,
             SLS_UNIT,
             SLS_VALUE,
             CRNCY
       FROM ITG_PERENSO_FSSI_SALES
) ,
subq_2 as ( SELECT BILL_DT AS CAL_DATE,
              'FSNI' ACCT_BANNER,
              'FOODSTUFF-NTH ISLAND' AS SALES_OFFICE,
              PROD_EAN,
              ARTICLE_DESCRIPTION,
              STORE_CD,
              STORE_NAME,
              BASE_QTY AS SLS_UNIT,
              SLS_VALUE,
              CRNCY
        FROM ITG_PERENSO_FSNI_SALES) ,
base as (SELECT ETD.JJ_MNTH_ID,A.* FROM (
       select * from subq_1
       UNION ALL
      select * from subq_2) A
        LEFT JOIN ETD ON A.CAL_DATE = ETD.CAL_DATE
        WHERE PROD_EAN IN (SELECT LTRIM(PROD_EAN,0) FROM EDW_PERENSO_PROD_DIM WHERE TRIM(PROD_EAN)<>'' GROUP BY 1 HAVING COUNT(*) > 1)) ,
union_6 as (--For Multiple PROD EANs
SELECT
'Foodstuffs' AS ORDER_TYPE,
       BASE.CAL_DATE AS DELVRY_DT,
       ETD.TIME_ID,
       ETD.JJ_YEAR,
       ETD.JJ_QRTR,
       ETD.JJ_MNTH,
       ETD.JJ_WK,
       ETD.JJ_MNTH_WK,
       ETD.JJ_MNTH_ID,
       ETD.JJ_MNTH_TOT,
       ETD.JJ_MNTH_DAY,
       ETD.JJ_MNTH_SHRT,
       ETD.JJ_MNTH_LONG,
       ETD.CAL_YEAR,
       ETD.CAL_QRTR,
       ETD.CAL_MNTH,
       ETD.CAL_WK,
       ETD.CAL_MNTH_WK,
       ETD.CAL_MNTH_ID,
       ETD.CAL_MNTH_NM,
       EPPD.PROD_KEY,
       NULL AS PROD_PROBE_ID,
       NVL(EPPD.PROD_DESC,BASE.ARTICLE_DESCRIPTION) AS PROD_DESC,
       EPPD.PROD_ID AS PROD_SAPBW_CODE,
       BASE.PROD_EAN,
       EPPD.PROD_JJ_FRANCHISE,
       EPPD.PROD_JJ_CATEGORY,
       EPPD.PROD_JJ_BRAND,
       EPPD.PROD_SAP_FRANCHISE,
       EPPD.PROD_SAP_PROFIT_CENTRE,
       EPPD.PROD_SAP_PRODUCT_MAJOR,
       EPPD.PROD_GROCERY_FRANCHISE,
       EPPD.PROD_GROCERY_CATEGORY,
       EPPD.PROD_GROCERY_BRAND,
       CASE
         WHEN (EPPD.PROD_ACTIVE_NZ_PHARMA != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_AU_GROCERY != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_METCASH != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_NZ_GROCERY != 'NOT ASSIGNED'
              OR EPPD.PROD_ACTIVE_AU_PHARMA != 'NOT ASSIGNED')
         THEN 1
         ELSE 0
       END PROD_ACTIVE_STATUS,
       EPPD.PROD_PBS,
       EPPD.PROD_IMS_BRAND,
       EPPD.PROD_NZ_CODE,
       EPPD.PROD_METCASH_CODE,
       EPPD.PROD_OLD_ID,
       EPPD.PROD_OLD_EAN,
       EPPD.PROD_TAX,
       EPPD.PROD_BWP_AUD,
       EPPD.PROD_BWP_NZD,
       NULL AS PROD_WHOLESALER_CODE,
       EPAD.ACCT_ID AS ACCT_KEY,
       NULL AS ACCT_PROBE_ID,
       NULL AS ACCT_METCASH_STORE_CODE,
       NVL(EPAD.ACCT_DISPLAY_NAME,BASE.STORE_NAME) AS ACCT_DISPLAY_NAME ,
       EPAD.ACCT_TYPE_DESC,
       EPAD.ACCT_STREET_1,
       EPAD.ACCT_STREET_2,
       EPAD.ACCT_STREET_3,
       EPAD.ACCT_SUBURB,
       EPAD.ACCT_POSTCODE,
       EPAD.ACCT_PHONE_NUMBER,
       EPAD.ACCT_FAX_NUMBER,
       EPAD.ACCT_EMAIL,
       EPAD.ACCT_COUNTRY,
       EPAD.ACCT_REGION,
       EPAD.ACCT_STATE,
       EPAD.ACCT_BANNER_COUNTRY,
       EPAD.ACCT_BANNER_DIVISION,
       EPAD.ACCT_BANNER_TYPE,
	   EPAD.ACCT_BANNER,
	   EPAD.ACCT_TYPE,
       EPAD.ACCT_SUB_TYPE,
       EPAD.ACCT_GRADE,
       EPAD.ACCT_NZ_PHARMA_COUNTRY,
       EPAD.ACCT_NZ_PHARMA_STATE,
       EPAD.ACCT_NZ_PHARMA_TERRITORY,
       EPAD.ACCT_NZ_GROC_COUNTRY,
       EPAD.ACCT_NZ_GROC_STATE,
       EPAD.ACCT_NZ_GROC_TERRITORY,
       EPAD.ACCT_SSR_COUNTRY,
       EPAD.ACCT_SSR_STATE,
       EPAD.ACCT_SSR_TEAM_LEADER,
       EPAD.ACCT_SSR_TERRITORY,
       EPAD.ACCT_SSR_SUB_TERRITORY,
       EPAD.ACCT_IND_GROC_COUNTRY,
       EPAD.ACCT_IND_GROC_STATE,
       EPAD.ACCT_IND_GROC_TERRITORY,
       EPAD.ACCT_IND_GROC_SUB_TERRITORY,
       EPAD.ACCT_AU_PHARMA_COUNTRY,
       EPAD.ACCT_AU_PHARMA_STATE,
       EPAD.ACCT_AU_PHARMA_TERRITORY,
       EPAD.ACCT_AU_PHARMA_SSR_COUNTRY,
       EPAD.ACCT_AU_PHARMA_SSR_STATE,
       EPAD.ACCT_AU_PHARMA_SSR_TERRITORY,
       CASE
         WHEN UPPER(EPAD.ACCT_IND_GROC_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_STATE
         WHEN UPPER(EPAD.ACCT_AU_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_STATE
         WHEN UPPER(EPAD.ACCT_NZ_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_STATE
         ELSE 'NOT ASSIGNED'
       END ACCT_TSM,
       CASE
         WHEN UPPER(EPAD.ACCT_IND_GROC_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_TERRITORY
         WHEN UPPER(EPAD.ACCT_AU_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_TERRITORY
         WHEN UPPER(EPAD.ACCT_NZ_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_TERRITORY
         ELSE 'NOT ASSIGNED'
       END ACCT_TERRIROTY,
       BASE.STORE_CD AS ACCT_STORE_CODE,
       EPAD.ACCT_FAX_OPT_OUT,
       EPAD.ACCT_EMAIL_OPT_OUT,
       EPAD.ACCT_CONTACT_METHOD,
       NULL as SSR_GRADE,
       null::number AS ORDER_KEY,
       null::TIMESTAMP AS ORDER_DATE,
       null::TIMESTAMP AS SENT_DT,
       NULL AS DELVRY_INSTNS,
       null::number AS BRANCH_KEY,
       null::number AS BATCH_KEY,
       null::number AS LINE_KEY,
       BASE.SLS_UNIT AS UNIT_QTY,
       null::number AS ENTERED_QTY,
       null::number AS ENTERED_UNIT_KEY,
       null::number AS LIST_PRICE,
       BASE.SLS_VALUE AS NIS,
       --PSF.DERIVED_PRICE AS WS_DERIVED_PRICE, -- Check for this column
       null::number AS RRP,
       NULL AS DEAL_DESC,
       NULL AS DEAL_START_DATE,
       NULL AS DEAL_END_DATE,
       NULL AS DEAL_SHORT_DESC,
       NULL AS DISCOUNT_DESC,
       NULL AS DISCOUNT_PER,
       NULL AS DISCOUNT_AMT,
       NULL AS ORDER_STATUS,
       NULL AS BATCH_STATUS,
       BASE.CRNCY AS ORDER_CURRENCY_CD,
       NULL AS CREATE_USER_NAME,
       NULL AS CREATE_USER_DESC,
       NULL AS CREATE_USER_EMAIL_ADDRESS,
       NULL AS DISTRIBUTOR,
       NULL AS DISTRIBUTOR_DISPLAY_NAME,
       NULL AS PHARMACY_MSL_RANK,
       NULL AS PHARMACY_MSL_FLAG,
       null::number AS EXCH_RATE_TO_AUD,
       null::float AS AUD_NIS,
       BWAR.EXCH_RATE AS EXCH_RATE_TO_USD,
       BASE.SLS_VALUE*BWAR.EXCH_RATE AS USD_NIS,
       JJBR.EXCH_RATE AS EXCH_RATE_TO_NZD,
       BASE.SLS_VALUE*JJBR.EXCH_RATE AS NZD_NIS,
       NULL AS DISTRIBUTION_FLAG
FROM  BASE
LEFT JOIN ETD ON BASE.CAL_DATE::date = ETD.CAL_DATE::date
LEFT JOIN EAN_MAP ON EAN_MAP.JJ_MONTH_ID=BASE.JJ_MNTH_ID AND  LTRIM(BASE.PROD_EAN,0) = LTRIM(EAN_MAP.BAR_CD,0) AND UPPER(BASE.SALES_OFFICE)=UPPER(EAN_MAP.SALES_OFFICE_DESC)
LEFT JOIN
(SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY LTRIM(PROD_EAN,0),PROD_ID ORDER BY LENGTH(PROD_ACTIVE_NZ_GROCERY) DESC) RN,*
FROM EDW_PERENSO_PROD_DIM WHERE LTRIM(PROD_EAN,0) IN (SELECT LTRIM(PROD_EAN,0) FROM EDW_PERENSO_PROD_DIM WHERE TRIM(PROD_EAN)<>'' GROUP BY 1 HAVING COUNT(*) > 1)) WHERE RN=1) EPPD
ON LTRIM(EPPD.PROD_EAN,0) = EAN_MAP.BAR_CD AND LTRIM (EPPD.PROD_ID,0) = LTRIM (EAN_MAP.MATERIAL_ID,0)
LEFT JOIN (SELECT * FROM EDW_PERENSO_ACCOUNT_HIST_DIM WHERE ACCT_BANNER_TYPE IN ('FSSI','FSNI') and
ACCT_STORE_CODE NOT in (SELECT ACCT_STORE_CODE from EDW_PERENSO_ACCOUNT_HIST_DIM where ACCT_BANNER_TYPE IN ('FSSI','FSNI') group by 1 HAVING COUNT(*) > 1)) EPAD
ON BASE.STORE_CD = EPAD.ACCT_STORE_CODE
LEFT JOIN (SELECT *
            FROM VW_JJBR_CURR_EXCH_DIM
            WHERE TO_CCY = 'NZD') JJBR
ON ETD.JJ_MNTH_ID = JJBR.JJ_MNTH_ID AND BASE.CRNCY = JJBR.FROM_CCY
LEFT JOIN (SELECT *
            FROM VW_BWAR_CURR_EXCH_DIM
            WHERE TO_CCY = 'USD') BWAR
ON ETD.JJ_MNTH_ID = BWAR.JJ_MNTH_ID AND BASE.CRNCY = BWAR.FROM_CCY
WHERE DATEDIFF(MONTH,BASE.CAL_DATE,CURRENT_DATE) <= 25) ,
union_7 as (
       SELECT 'Distribution' AS ORDER_TYPE,
       DSTR.START_DATE AS DELVRY_DT,
       ETD.TIME_ID,
       ETD.JJ_YEAR,
       ETD.JJ_QRTR,
       ETD.JJ_MNTH,
       ETD.JJ_WK,
       ETD.JJ_MNTH_WK,
       ETD.JJ_MNTH_ID,
       ETD.JJ_MNTH_TOT,
       ETD.JJ_MNTH_DAY,
       ETD.JJ_MNTH_SHRT,
       ETD.JJ_MNTH_LONG,
       ETD.CAL_YEAR,
       ETD.CAL_QRTR,
       ETD.CAL_MNTH,
       ETD.CAL_WK,
       ETD.CAL_MNTH_WK,
       ETD.CAL_MNTH_ID,
       ETD.CAL_MNTH_NM,
       DSTR.PROD_KEY AS PROD_KEY,
	   NULL AS PROD_PROBE_ID,
       EPPD.PROD_DESC,
       EPPD.PROD_ID AS PROD_SAPBW_CODE,
       EPPD.PROD_EAN,
       EPPD.PROD_JJ_FRANCHISE,
       EPPD.PROD_JJ_CATEGORY,
       EPPD.PROD_JJ_BRAND,
       EPPD.PROD_SAP_FRANCHISE,
       EPPD.PROD_SAP_PROFIT_CENTRE,
       EPPD.PROD_SAP_PRODUCT_MAJOR,
       EPPD.PROD_GROCERY_FRANCHISE,
       EPPD.PROD_GROCERY_CATEGORY,
       EPPD.PROD_GROCERY_BRAND,
       CASE
             WHEN (EPPD.PROD_ACTIVE_NZ_PHARMA != 'NOT ASSIGNED'
                  OR EPPD.PROD_ACTIVE_AU_GROCERY != 'NOT ASSIGNED'
                  OR EPPD.PROD_ACTIVE_METCASH != 'NOT ASSIGNED'
                  OR EPPD.PROD_ACTIVE_NZ_GROCERY != 'NOT ASSIGNED'
                  OR EPPD.PROD_ACTIVE_AU_PHARMA != 'NOT ASSIGNED')
             THEN 1
             ELSE 0
       END PROD_ACTIVE_STATUS,
       EPPD.PROD_PBS,
       EPPD.PROD_IMS_BRAND,
       EPPD.PROD_NZ_CODE,
       EPPD.PROD_METCASH_CODE,
	   EPPD.PROD_OLD_ID,
       EPPD.PROD_OLD_EAN,
       EPPD.PROD_TAX,
       EPPD.PROD_BWP_AUD,
       EPPD.PROD_BWP_NZD,
	   NULL AS PROD_WHOLESALER_CODE,
       DSTR.ACCT_KEY AS ACCT_KEY,
	   NULL AS ACCT_PROBE_ID,
	   NULL AS ACCT_METCASH_STORE_CODE,
       EPAD.ACCT_DISPLAY_NAME,
       EPAD.ACCT_TYPE_DESC,
       EPAD.ACCT_STREET_1,
       EPAD.ACCT_STREET_2,
       EPAD.ACCT_STREET_3,
       EPAD.ACCT_SUBURB,
       EPAD.ACCT_POSTCODE,
       EPAD.ACCT_PHONE_NUMBER,
       EPAD.ACCT_FAX_NUMBER,
       EPAD.ACCT_EMAIL,
       EPAD.ACCT_COUNTRY,
       EPAD.ACCT_REGION,
       EPAD.ACCT_STATE,
       EPAD.ACCT_BANNER_COUNTRY,
       EPAD.ACCT_BANNER_DIVISION,
       EPAD.ACCT_BANNER_TYPE,
       EPAD.ACCT_BANNER,
       EPAD.ACCT_TYPE,
       EPAD.ACCT_SUB_TYPE,
       EPAD.ACCT_GRADE,
       EPAD.ACCT_NZ_PHARMA_COUNTRY,
       EPAD.ACCT_NZ_PHARMA_STATE,
       EPAD.ACCT_NZ_PHARMA_TERRITORY,
       EPAD.ACCT_NZ_GROC_COUNTRY,
       EPAD.ACCT_NZ_GROC_STATE,
       EPAD.ACCT_NZ_GROC_TERRITORY,
       EPAD.ACCT_SSR_COUNTRY,
       EPAD.ACCT_SSR_STATE,
       EPAD.ACCT_SSR_TEAM_LEADER,
       EPAD.ACCT_SSR_TERRITORY,
       EPAD.ACCT_SSR_SUB_TERRITORY,
       EPAD.ACCT_IND_GROC_COUNTRY,
       EPAD.ACCT_IND_GROC_STATE,
       EPAD.ACCT_IND_GROC_TERRITORY,
       EPAD.ACCT_IND_GROC_SUB_TERRITORY,
       EPAD.ACCT_AU_PHARMA_COUNTRY,
       EPAD.ACCT_AU_PHARMA_STATE,
       EPAD.ACCT_AU_PHARMA_TERRITORY,
       EPAD.ACCT_AU_PHARMA_SSR_COUNTRY,
       EPAD.ACCT_AU_PHARMA_SSR_STATE,
       EPAD.ACCT_AU_PHARMA_SSR_TERRITORY,
       CASE
              WHEN UPPER(EPAD.ACCT_IND_GROC_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_STATE
              WHEN UPPER(EPAD.ACCT_AU_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_STATE
			        WHEN UPPER(EPAD.ACCT_NZ_PHARMA_STATE) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_STATE
              ELSE 'NOT ASSIGNED'
       END  ACCT_TSM,
       CASE
              WHEN UPPER(EPAD.ACCT_IND_GROC_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_IND_GROC_TERRITORY
              WHEN UPPER(EPAD.ACCT_AU_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_AU_PHARMA_TERRITORY
			        WHEN UPPER(EPAD.ACCT_NZ_PHARMA_TERRITORY) != 'NOT ASSIGNED' THEN EPAD.ACCT_NZ_PHARMA_TERRITORY
              ELSE 'NOT ASSIGNED'
       END  ACCT_TERRIROTY,
	   EPAD.ACCT_STORE_CODE AS ACCT_STORE_CODE,
	   EPAD.ACCT_FAX_OPT_OUT,
       EPAD.ACCT_EMAIL_OPT_OUT,
       EPAD.ACCT_CONTACT_METHOD,
       EPAD.SSR_GRADE,
       NULL AS ORDER_KEY,
       NULL AS ORDER_DATE,
       NULL AS SENT_DT,
       NULL AS DELVRY_INSTNS,
       NULL AS BRANCH_KEY,
       NULL AS BATCH_KEY,
       NULL AS LINE_KEY,
       NULL AS UNIT_QTY,
       NULL AS ENTERED_QTY,
       NULL AS ENTERED_UNIT_KEY,
       NULL AS LIST_PRICE,
       NULL AS NIS,
       NULL AS RRP,
       NULL AS DEAL_DESC,
       NULL AS DEAL_START_DATE,
       NULL AS DEAL_END_DATE,
       NULL AS DEAL_SHORT_DESC,
       NULL AS DISCOUNT_DESC,
       NULL AS DISCOUNT_PER,
       NULL AS DISCOUNT_AMT,
       NULL AS ORDER_STATUS,
       NULL AS BATCH_STATUS,
       NULL AS ORDER_CURRENCY_CD,
	   NULL AS CREATE_USER_NAME,
	   NULL AS CREATE_USER_DESC,
       NULL AS CREATE_USER_EMAIL_ADDRESS,
       NULL AS DISTRIBUTOR,
       NULL AS DISTRIBUTOR_DISPLAY_NAME,
       NULL AS PHARMACY_MSL_RANK,
       NULL AS PHARMACY_MSL_FLAG,
       NULL AS EXCH_RATE_TO_AUD,
       NULL AS AUD_NIS,
       NULL AS EXCH_RATE_TO_USD,
       NULL AS USD_NIS,
       NULL AS EXCH_RATE_TO_NZD,
       NULL AS NZD_NIS,
	   DSTR.IN_DISTRIBUTION AS DISTRIBUTION_FLAG
FROM ITG_PERENSO_PROD_CHK_DISTRIBUTION DSTR
LEFT JOIN  ETD
ON DSTR.START_DATE = ETD.CAL_DATE::date
LEFT JOIN EDW_PERENSO_PROD_DIM EPPD ON DSTR.PROD_KEY = EPPD.PROD_KEY
LEFT JOIN EDW_PERENSO_ACCOUNT_DIM EPAD ON DSTR.ACCT_KEY = EPAD.ACCT_ID
WHERE DELVRY_DT > add_months(convert_timezone('UTC',current_timestamp())::date,-24)
),
transformed as (
select * from union_1
UNION ALL
select * from union_2

UNION all

select * from union_3

UNION ALL

select * from union_4

UNION ALL
select * from union_5

UNION ALL
select * from union_6

UNION ALL
select * from union_7),
final as (
    select
    order_type::varchar(255) as order_type,
    delvry_dt::timestamp_ntz(9) as delvry_dt,
    time_id::number(18,0) as time_id,
    jj_year::number(18,0) as jj_year,
    jj_qrtr::number(18,0) as jj_qrtr,
    jj_mnth::number(18,0) as jj_mnth,
    jj_wk::number(18,0) as jj_wk,
    jj_mnth_wk::number(38,0) as jj_mnth_wk,
    jj_mnth_id::number(18,0) as jj_mnth_id,
    jj_mnth_tot::number(18,0) as jj_mnth_tot,
    jj_mnth_day::number(18,0) as jj_mnth_day,
    jj_mnth_shrt::varchar(3) as jj_mnth_shrt,
    jj_mnth_long::varchar(10) as jj_mnth_long,
    cal_year::number(18,0) as cal_year,
    cal_qrtr::number(18,0) as cal_qrtr,
    cal_mnth::number(18,0) as cal_mnth,
    cal_wk::number(38,0) as cal_wk,
    cal_mnth_wk::number(38,0) as cal_mnth_wk,
    cal_mnth_id::number(18,0) as cal_mnth_id,
    cal_mnth_nm::varchar(10) as cal_mnth_nm,
    prod_key::number(10,0) as prod_key,
    prod_probe_id::number(18,0) as prod_probe_id,
    prod_desc::varchar(1000) as prod_desc,
    prod_sapbw_code::varchar(50) as prod_sapbw_code,
    prod_ean::varchar(50) as prod_ean,
    prod_jj_franchise::varchar(100) as prod_jj_franchise,
    prod_jj_category::varchar(100) as prod_jj_category,
    prod_jj_brand::varchar(100) as prod_jj_brand,
    prod_sap_franchise::varchar(100) as prod_sap_franchise,
    prod_sap_profit_centre::varchar(100) as prod_sap_profit_centre,
    prod_sap_product_major::varchar(100) as prod_sap_product_major,
    prod_grocery_franchise::varchar(100) as prod_grocery_franchise,
    prod_grocery_category::varchar(100) as prod_grocery_category,
    prod_grocery_brand::varchar(100) as prod_grocery_brand,
    prod_active_status::number(18,0) as prod_active_status,
    prod_pbs::varchar(100) as prod_pbs,
    prod_ims_brand::varchar(100) as prod_ims_brand,
    prod_nz_code::varchar(100) as prod_nz_code,
    prod_metcash_code::varchar(100) as prod_metcash_code,
    prod_old_id::varchar(50) as prod_old_id,
    prod_old_ean::varchar(50) as prod_old_ean,
    prod_tax::varchar(50) as prod_tax,
    prod_bwp_aud::varchar(50) as prod_bwp_aud,
    prod_bwp_nzd::varchar(50) as prod_bwp_nzd,
    prod_wholesaler_code::varchar(250) as prod_wholesaler_code,
    acct_key::number(10,0) as acct_key,
    acct_probe_id::number(18,0) as acct_probe_id,
    acct_metcash_store_code::varchar(50) as acct_metcash_store_code,
    acct_display_name::varchar(256) as acct_display_name,
    acct_type_desc::varchar(50) as acct_type_desc,
    acct_street_1::varchar(256) as acct_street_1,
    acct_street_2::varchar(256) as acct_street_2,
    acct_street_3::varchar(256) as acct_street_3,
    acct_suburb::varchar(255) as acct_suburb,
    acct_postcode::varchar(255) as acct_postcode,
    acct_phone_number::varchar(255) as acct_phone_number,
    acct_fax_number::varchar(50) as acct_fax_number,
    acct_email::varchar(256) as acct_email,
    acct_country::varchar(256) as acct_country,
    acct_region::varchar(256) as acct_region,
    acct_state::varchar(256) as acct_state,
    acct_banner_country::varchar(256) as acct_banner_country,
    acct_banner_division::varchar(256) as acct_banner_division,
    acct_banner_type::varchar(256) as acct_banner_type,
    acct_banner::varchar(256) as acct_banner,
    acct_type::varchar(256) as acct_type,
    acct_sub_type::varchar(256) as acct_sub_type,
    acct_grade::varchar(256) as acct_grade,
    acct_nz_pharma_country::varchar(256) as acct_nz_pharma_country,
    acct_nz_pharma_state::varchar(256) as acct_nz_pharma_state,
    acct_nz_pharma_territory::varchar(256) as acct_nz_pharma_territory,
    acct_nz_groc_country::varchar(256) as acct_nz_groc_country,
    acct_nz_groc_state::varchar(256) as acct_nz_groc_state,
    acct_nz_groc_territory::varchar(256) as acct_nz_groc_territory,
    acct_ssr_country::varchar(256) as acct_ssr_country,
    acct_ssr_state::varchar(256) as acct_ssr_state,
    acct_ssr_team_leader::varchar(256) as acct_ssr_team_leader,
    acct_ssr_territory::varchar(256) as acct_ssr_territory,
    acct_ssr_sub_territory::varchar(256) as acct_ssr_sub_territory,
    acct_ind_groc_country::varchar(256) as acct_ind_groc_country,
    acct_ind_groc_state::varchar(256) as acct_ind_groc_state,
    acct_ind_groc_territory::varchar(256) as acct_ind_groc_territory,
    acct_ind_groc_sub_territory::varchar(256) as acct_ind_groc_sub_territory,
    acct_au_pharma_country::varchar(256) as acct_au_pharma_country,
    acct_au_pharma_state::varchar(256) as acct_au_pharma_state,
    acct_au_pharma_territory::varchar(256) as acct_au_pharma_territory,
    acct_au_pharma_ssr_country::varchar(256) as acct_au_pharma_ssr_country,
    acct_au_pharma_ssr_state::varchar(256) as acct_au_pharma_ssr_state,
    acct_au_pharma_ssr_territory::varchar(256) as acct_au_pharma_ssr_territory,
    acct_tsm::varchar(256) as acct_tsm,
    acct_terriroty::varchar(256) as acct_terriroty,
    acct_store_code::varchar(256) as acct_store_code,
    acct_fax_opt_out::varchar(256) as acct_fax_opt_out,
    acct_email_opt_out::varchar(256) as acct_email_opt_out,
    acct_contact_method::varchar(256) as acct_contact_method,
    ssr_grade::varchar(256) as ssr_grade,
    order_key::number(25,0) as order_key,
    order_date::timestamp_ntz(9) as order_date,
    sent_dt::timestamp_ntz(9) as sent_dt,
    delvry_instns::varchar(256) as delvry_instns,
    branch_key::number(18,0) as branch_key,
    batch_key::number(18,0) as batch_key,
    line_key::number(18,0) as line_key,
    unit_qty::number(22,4) as unit_qty,
    entered_qty::number(18,0) as entered_qty,
    entered_unit_key::number(18,0) as entered_unit_key,
    list_price::number(24,6) as list_price,
    nis::float as nis,
    rrp::number(20,2) as rrp,
    deal_desc::varchar(255) as deal_desc,
    deal_start_date::date as deal_start_date,
    deal_end_date::date as deal_end_date,
    deal_short_desc::varchar(255) as deal_short_desc,
    discount_desc::varchar(255) as discount_desc,
    discount_per::float as discount_per,
    discount_amt::float as discount_amt,
    order_status::varchar(255) as order_status,
    batch_status::varchar(255) as batch_status,
    order_currency_cd::varchar(12) as order_currency_cd,
    create_user_name::varchar(100) as create_user_name,
    create_user_desc::varchar(100) as create_user_desc,
    create_user_email_address::varchar(100) as create_user_email_address,
    distributor::varchar(255) as distributor,
    distributor_display_name::varchar(255) as distributor_display_name,
    pharmacy_msl_rank::varchar(5) as pharmacy_msl_rank,
    pharmacy_msl_flag::varchar(5) as pharmacy_msl_flag,
    exch_rate_to_aud::number(23,5) as exch_rate_to_aud,
    aud_nis::float as aud_nis,
    exch_rate_to_usd::number(23,5) as exch_rate_to_usd,
    usd_nis::float as usd_nis,
    exch_rate_to_nzd::number(23,5) as exch_rate_to_nzd,
    nzd_nis::float as nzd_nis,
    distribution_flag::varchar(5) as distribution_flag
    from transformed
)
select * from final