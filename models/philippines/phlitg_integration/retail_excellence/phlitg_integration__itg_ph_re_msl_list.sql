--import cte
/*with itg_re_msl_input_definition as (
    select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),*/
with itg_re_msl_input_definition as (
    select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim')}}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
itg_mds_ph_lav_customer as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_customer')}}
),
wks_philippines_base_retail_excellence as (
    select * from {{ ref('phlwks_integration__wks_philippines_base_retail_excellence')}}
),
itg_mds_ph_gt_customer as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_gt_customer')}}
),
itg_mds_ph_ref_parent_customer as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_parent_customer')}}
),
itg_mds_ph_pos_customers as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_customers')}}
),

MSL as 
(
    SELECT DISTINCT CAL.FISC_YR AS YEAR,
       CAL.JJ_MNTH_ID,
       MSL_DEF.SUB_CHANNEL,
       MSL_DEF.SKU_UNIQUE_IDENTIFIER,
	   MSL_DEF.RETAIL_ENVIRONMENT
    FROM itg_re_msl_input_definition MSL_DEF
    LEFT JOIN (SELECT DISTINCT FISC_YR,
                    SUBSTRING(FISC_PER,1,4)||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM edw_calendar_dim) CAL
         ON TO_CHAR (TO_DATE (MSL_DEF.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID
        AND TO_CHAR (TO_DATE (MSL_DEF.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID
    WHERE market = 'Philippines'
AND   active_status_code = 'Y'
),

LAV as
(SELECT DISTINCT ltrim(cust_id,'0') AS Customer_L0,
                  rpt_grp_6_desc AS account_group,
                  channel_desc AS channel,
                  ltrim(dstrbtr_grp_cd,'0') AS distributor_code,
				  MAX(dstrbtr_grp_nm) OVER (PARTITION BY ltrim(dstrbtr_grp_cd,'0') ORDER BY crtd_dttm DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS distributor_name,
                  upper(ltrim(parent_cust_cd,'0')) AS parent_customer
           FROM ITG_MDS_PH_LAV_CUSTOMER WHERE UPPER(active) = 'Y'),

REG_SO as		   
(SELECT DISTINCT SOLDTO_CODE,
                          data_source AS data_src,
						  MASTER_CODE as msl_product_code,
						  MAPPED_SKU_CD,
						  MSL_PRODUCT_DESC,
                          DISTRIBUTOR_CODE as dist_cd,
						  DISTRIBUTOR_NAME as dist_nm,
						FROM WKS_PHILIPPINES_BASE_RETAIL_EXCELLENCE
                         WHERE DATA_SOURCE IN ('SELL-OUT','POS')),
GT_CUST as 					 
(SELECT DISTINCT rep_grp2_desc AS region,
                          rep_grp3_desc AS "Area/Zone/Province",
                          rep_grp5_desc AS city,
                          sls_dist_desc AS Sell_Out_Parent_Customer_L2,
                          ltrim(dstrbtr_grp_cd,'0') AS Sell_Out_Parent_Customer_L1,
                          UPPER(rpt_grp11_desc) AS sell_out_channel,
						  UPPER(rpt_grp9_desc) AS retail_environment,
                          ltrim(dstrbtr_cust_id,'0') AS store_code,
						  MAX(dstrbtr_cust_nm) OVER (PARTITION BY ltrim(dstrbtr_cust_id,'0') ORDER BY crtd_dttm DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS store_name,
                          address AS store_address,
                          zip_code AS store_postcode,
                          latitude::VARCHAR AS store_lat,
                          longitude::VARCHAR AS store_long,
                          ROW_NUMBER() OVER (PARTITION BY ltrim(dstrbtr_cust_id,'0') ORDER BY crtd_dttm DESC) AS rno
                   FROM ITG_MDS_PH_GT_CUSTOMER WHERE UPPER(active) = 'Y'),
PRNT_CUST as
(SELECT DISTINCT rpt_grp_1_desc AS trade_type,
                rpt_grp_12_desc AS sales_group,
                parent_cust_cd
        FROM ITG_MDS_PH_REF_PARENT_CUSTOMER WHERE UPPER(trade_type) IN ('GENERAL TRADE','NATIONAL KEY ACCOUNT') AND UPPER(active) = 'Y'),

POS_CUST as 
(SELECT DISTINCT region_nm AS region,
                         prov_nm AS "Area/Zone/Province",
                         mncplty_nm AS city,
                         NULL::VARCHAR AS Sell_Out_Parent_Customer_L2,
                         jj_sold_to AS Sell_Out_Parent_Customer_L1,
                         UPPER(store_mtrx) AS sell_out_channel,
						 UPPER(chnl_sub_grp_cd) AS retail_environment,
						 ltrim(SPLIT_PART(code, '-', 2),'0') AS store_code,
						 MAX(brnch_nm) OVER (PARTITION BY ltrim(SPLIT_PART(code, '-', 2),'0') ORDER BY crtd_dttm DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS store_name,
                         (address1||address2) AS store_address,
                         zip_code AS store_postcode,
                         latitude::VARCHAR AS store_lat,
                         longitude::VARCHAR AS store_long,
                         ROW_NUMBER() OVER (PARTITION BY ltrim(SPLIT_PART(code, '-', 2),'0') ORDER BY upper(store_mtrx) ASC, crtd_dttm DESC) AS rno
                  FROM (select * from ITG_MDS_PH_POS_CUSTOMERS WHERE UPPER(active) = 'Y' AND store_mtrx <> ' ')),

--final cte

------------------------------------------------SELL-OUT-----------------------------
itg_ph_re_msl_list_so as 
(SELECT DISTINCT MSL.YEAR AS fisc_yr,
        MSL.JJ_MNTH_ID AS fisc_per,
        CUST.Customer_L0,
		CUST.TRADE_TYPE,
        CUST.SALES_GROUP,
        CUST.ACCOUNT_GROUP,
        CUST.DATA_SRC,
        CUST.CHANNEL,
        MSL.SUB_CHANNEL,
		CUST.RETAIL_ENVIRONMENT,
        CUST.DISTRIBUTOR_CODE,
		CUST.DISTRIBUTOR_NAME|| '#' ||ltrim(cust.distributor_code,'0') AS distributor_name,
		CUST.PARENT_CUSTOMER,
        CUST.REGION,
        CUST."Area/Zone/Province",
        CUST.CITY,
        CUST.SELL_OUT_PARENT_CUSTOMER_L2,
        CUST.SELL_OUT_PARENT_CUSTOMER_L1,
        CUST.SELL_OUT_CHANNEL,
        CUST.STORE_CODE,
		CUST.STORE_NAME|| '#' ||ltrim(cust.store_code,'0') AS store_name,
        CUST.STORE_ADDRESS,
        CUST.STORE_POSTCODE,
        CUST.STORE_LAT,
        CUST.STORE_LONG,
        LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0') AS master_code,
		CUST.MAPPED_SKU_CD,
		CUST.MSL_PRODUCT_DESC,
        SYSDATE() AS CRTD_DTTM
FROM MSL
JOIN (select * from LAV	
	 JOIN (select * from REG_SO WHERE data_src = 'SELL-OUT') REG_SO_GT ON LTRIM(LAV.CUSTOMER_L0,'0') = LTRIM(REG_SO_GT.SOLDTO_CODE,'0') AND LTRIM(LAV.DISTRIBUTOR_CODE,'0') = LTRIM(REG_SO_GT.DIST_CD,'0')
	 JOIN (select * from GT_CUST) GT_CUST ON ltrim(LAV.distributor_code,'0') = ltrim(GT_CUST.Sell_Out_Parent_Customer_L1,'0')
	 JOIN (select * from PRNT_CUST where trade_type = 'GENERAL TRADE') PRNT_CUST_GT ON upper(ltrim(LAV.parent_customer,'0')) = upper(ltrim(PRNT_CUST_GT.parent_cust_cd,'0'))) cust
     ON  LOWER (MSL.SUB_CHANNEL) = LOWER (CUST.SELL_OUT_CHANNEL)
	 AND LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0') = LTRIM(CUST.MSL_PRODUCT_CODE,'0')
 WHERE MSL.JJ_MNTH_ID >= (SELECT last_16mnths
                        FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)
 AND   MSL.JJ_MNTH_ID <= (SELECT prev_mnth FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)
 AND   CUST.RNO = 1),

------------------------------------------POS----------------------------------------
itg_ph_re_msl_list_pos as 
(SELECT DISTINCT MSL.YEAR AS fisc_yr,
       MSL.JJ_MNTH_ID AS fisc_per,
       CUST.Customer_L0,
       CUST.TRADE_TYPE,
       CUST.SALES_GROUP,
       CUST.ACCOUNT_GROUP,
       CUST.DATA_SRC,
       CUST.CHANNEL,
       MSL.SUB_CHANNEL,
	   CUST.RETAIL_ENVIRONMENT,
       CUST.DIST_CD AS DISTRIBUTOR_CODE,
	   CUST.DIST_NM AS DISTRIBUTOR_NAME,
       CUST.PARENT_CUSTOMER,
       CUST.REGION,
       CUST."Area/Zone/Province",
       CUST.CITY,
       CUST.SELL_OUT_PARENT_CUSTOMER_L2,
       CUST.SELL_OUT_PARENT_CUSTOMER_L1,
       CUST.SELL_OUT_CHANNEL,
       CUST.STORE_CODE,
	   CUST.STORE_NAME|| '#' ||ltrim(cust.store_code,'0') AS store_name,
       CUST.STORE_ADDRESS,
       CUST.STORE_POSTCODE,
       CUST.STORE_LAT,
       CUST.STORE_LONG,
       LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0') AS MASTER_CODE,
	   CUST.MAPPED_SKU_CD,
	   CUST.MSL_PRODUCT_DESC,
	   SYSDATE() AS CRTD_DTTM
FROM MSL
JOIN ( select * from LAV
		JOIN (select * from REG_SO WHERE data_src = 'POS') REG_SO_MT
		  ON LTRIM(LAV.CUSTOMER_L0,'0') = LTRIM(REG_SO_MT.SOLDTO_CODE,'0')
		JOIN (select * from POS_CUST) POS_CUST ON ltrim(LAV.Customer_L0,'0') = ltrim(POS_CUST.Sell_Out_Parent_Customer_L1,'0')		
		JOIN (select * from PRNT_CUST where trade_type = 'NATIONAL KEY ACCOUNT') PRNT_CUST_MT ON UPPER(LTRIM(LAV.PARENT_CUSTOMER,'0')) = UPPER(LTRIM(PRNT_CUST_MT.PARENT_CUST_CD,'0'))) cust		
    ON  LOWER (MSL.SUB_CHANNEL) = LOWER (CUST.SELL_OUT_CHANNEL)
	AND LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0') = LTRIM(CUST.MSL_PRODUCT_CODE,'0')
 WHERE MSL.JJ_MNTH_ID >= (SELECT last_16mnths
                        FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)
 AND   MSL.JJ_MNTH_ID <= (SELECT prev_mnth FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)
 AND   CUST.RNO = 1),

itg_ph_re_msl_list as 
(
    select * from itg_ph_re_msl_list_so
    union
    select * from itg_ph_re_msl_list_pos
),

final as 
(
    select 
    fisc_yr	:: numeric(18,0) as fisc_yr,
    fisc_per :: numeric(18,0) as fisc_per,
    customer_l0 :: varchar(50) as customer_l0,
    trade_type :: varchar(255) as trade_type,
    sales_group :: varchar(255) as sales_group,
    account_group :: varchar(255) as account_group,
    data_src :: varchar(14) as data_src,
    channel :: varchar(255) as channel,
    sub_channel :: varchar(200) as sub_channel,
    retail_environment :: varchar(382) as retail_environment,
    distributor_code :: varchar(100) as distributor_code,
    distributor_name :: varchar(356) as distributor_name,
    parent_customer :: varchar(75) as parent_customer,
    region :: varchar(255) as region,
    "Area/Zone/Province" :: varchar(255) as "Area/Zone/Province",
    city :: varchar(255) as city,
    sell_out_parent_customer_l2 :: varchar(255) as sell_out_parent_customer_l2,
    sell_out_parent_customer_l1 :: varchar(255) as sell_out_parent_customer_l1,
    sell_out_channel :: varchar(382) as sell_out_channel,
    store_code :: varchar(50) as store_code,
    store_name :: varchar(500) as store_name,
    store_address :: varchar(510) as store_address,
    store_postcode :: varchar(100) as store_postcode,
    store_lat :: varchar(50) as store_lat,
    store_long :: varchar(255) as store_long,
    master_code :: varchar(200) as master_code,
    mapped_sku_cd :: varchar(40) as mapped_sku_cd,
    msl_product_desc :: varchar(300) as msl_product_desc,
    crtd_dttm :: timestamp without time zone as crtd_dttm
from itg_ph_re_msl_list
)

--final select

select * from final