--import cte

with MSL as (
    select * from {{ ref('phlwks_integration__wrk_ph_msl_list')}}
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
itg_mds_ph_ref_parent_customer as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_parent_customer')}}
),
itg_mds_ph_pos_customers as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_customers')}}
),


LAV as
(SELECT DISTINCT ltrim(cust_id,'0') AS Customer_L0,
                  rpt_grp_6_desc AS account_group,
                  channel_desc AS channel,
                  upper(ltrim(parent_cust_cd,'0')) AS parent_customer
           FROM ITG_MDS_PH_LAV_CUSTOMER WHERE UPPER(active) = 'Y'),
BASE_POS as		   
(SELECT DISTINCT SOLDTO_CODE,
                          data_source AS data_src,
						  MASTER_CODE as msl_product_code,
						  MAPPED_SKU_CD,
						  MSL_PRODUCT_DESC,
                          DISTRIBUTOR_CODE,
						  DISTRIBUTOR_NAME
						FROM WKS_PHILIPPINES_BASE_RETAIL_EXCELLENCE
                         WHERE DATA_SOURCE = 'POS'),
PRNT_CUST as
(SELECT DISTINCT rpt_grp_1_desc AS trade_type,
                rpt_grp_12_desc AS sales_group,
                parent_cust_cd
        FROM ITG_MDS_PH_REF_PARENT_CUSTOMER WHERE UPPER(trade_type) = 'NATIONAL KEY ACCOUNT' AND UPPER(active) = 'Y'),
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
                         ROW_NUMBER() OVER (PARTITION BY ltrim(SPLIT_PART(code, '-', 2),'0') ORDER BY crtd_dttm DESC) AS rno
                  FROM ITG_MDS_PH_POS_CUSTOMERS WHERE UPPER(active) = 'Y'),

--final cte

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
       CUST.DISTRIBUTOR_CODE,
	   CUST.DISTRIBUTOR_NAME,
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
		JOIN BASE_POS ON LTRIM(LAV.CUSTOMER_L0,'0') = LTRIM(BASE_POS.SOLDTO_CODE,'0')
		JOIN POS_CUST ON ltrim(LAV.Customer_L0,'0') = ltrim(POS_CUST.Sell_Out_Parent_Customer_L1,'0')		
		JOIN PRNT_CUST ON UPPER(LTRIM(LAV.PARENT_CUSTOMER,'0')) = UPPER(LTRIM(PRNT_CUST.PARENT_CUST_CD,'0'))) cust		
    ON  LOWER (MSL.SUB_CHANNEL) = LOWER (CUST.SELL_OUT_CHANNEL)
	AND LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0') = LTRIM(CUST.MSL_PRODUCT_CODE,'0')
 WHERE MSL.JJ_MNTH_ID >= (SELECT last_16mnths
                        FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)
 AND   MSL.JJ_MNTH_ID <= (SELECT prev_mnth FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)
 AND   CUST.RNO = 1),

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
from itg_ph_re_msl_list_pos
)

--final select

select * from final