--overwriding default sql header as we dont want to change timezone to singapore
--import cte
with itg_re_msl_input_definition as (
    select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
itg_mds_sg_customer_hierarchy as (
    select * from {{ ref('sgpitg_integration__itg_mds_sg_customer_hierarchy') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_korea_base_retail_excellence as (
    select * from {{ ref('ntawks_integration__wks_korea_base_retail_excellence') }}
),
 edw_customer_attr_flat_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
 ),
 msl_data as
 (
 select * from {{ ref('ntawks_integration__wks_msl_list') }}
 
 ),
 MSL1 as 
 (
    SELECT DISTINCT CAL.FISC_YR AS YEAR,		--// SELECT DISTINCT CAL.FISC_YR AS YEAR,
       --CAL.CAL_MO_1 AS MNTH_ID,
       MARKET,
       CAL.JJ_MNTH_ID,		--//        CAL.JJ_MNTH_ID,
       MSL_DEF.SUB_CHANNEL,		--//        MSL_DEF.sub_channel,
       LTRIM(MSL_DEF.SKU_UNIQUE_IDENTIFIER,'0') AS sku_unique_identifier,		--//        LTRIM(MSL_DEF.sku_unique_identifier,'0') AS sku_unique_identifier,
       UPPER(MSL_DEF.RETAIL_ENVIRONMENT) AS RETAIL_ENVIRONMENT		--//        UPPER(MSL_DEF.retail_environment) AS RETAIL_ENVIRONMENT
FROM itg_re_msl_input_definition MSL_DEF		--// FROM RG_ITG.ITG_RE_MSL_INPUT_DEFINITION MSL_DEF
  LEFT JOIN (SELECT DISTINCT FISC_YR,
                    --CAL_MO_1,
                    SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM edw_calendar_dim) CAL		--//              FROM RG_EDW.EDW_CALENDAR_DIM) CAL
         ON TO_CHAR (TO_DATE (MSL_DEF.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID		--//          ON TO_CHAR (TO_DATE (MSL_DEF.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID
        AND TO_CHAR (TO_DATE (MSL_DEF.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID		--//         AND TO_CHAR (TO_DATE (MSL_DEF.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID
WHERE market = 'Korea'
 ),
REG_SO as 
(
    SELECT DISTINCT data_source,
                      SOLDTO_CODE as sold_to_code,
					   distributor_code,
					   distributor_name,
                       store_code,
					   store_name,
					   ltrim(ean,'0') as ean,
					   ltrim(sku_code,'0') as sku_code,
					   sku_description,
					   retail_environment,
                       store_grade,
                       channel AS CHANNEL_DESC,
                       CNTRY_CD
                       FROM wks_korea_base_retail_excellence		
                WHERE data_source IN ('SELL-OUT','POS')
 ),
ATTR  as 
 (
    SELECT DISTINCT county AS COUNTRY,
                              sls_ofc_desc AS SALES_OFFICE_DESC,
                              sls_grp AS SALES_GRP_DESC,
                              cust_nm AS CUST_NM,
                              sold_to_party AS CUST_NO,
                              aw_remote_key
                       FROM edw_customer_attr_flat_dim		--//                        FROM rg_edw.edw_customer_attr_flat_dim
                       WHERE UPPER(cntry) = 'KOREA'
 ),
 wks_korea_base_retail_excellence_1 as 
 (
 select * from  REG_SO
            LEFT JOIN  ATTR
                   ON ATTR.AW_REMOTE_KEY = REG_SO.SOLD_TO_CODE		
                  AND ATTR.COUNTRY = REG_SO.CNTRY_CD
 ),
--final cte
kr_itg_re_msl_list as (
SELECT DISTINCT MSL.YEAR AS fisc_yr,		--// SELECT DISTINCT MSL.YEAR AS fisc_yr,
       MSL.JJ_MNTH_ID AS fisc_per,		--//        MSL.JJ_MNTH_ID AS fisc_per,
       --MSL.MNTH_ID AS fisc_per,
       FINAL.COUNTRY,		--//        FINAL.COUNTRY,
       FINAL.DATA_SOURCE,		--//        FINAL.DATA_SOURCE,
       FINAL.CHANNEL_DESC,		--//        FINAL.CHANNEL_DESC,
       FINAL.SALES_OFFICE_DESC,		--//        FINAL.SALES_OFFICE_DESC,
       FINAL.SALES_GRP_DESC,		--//        FINAL.SALES_GRP_DESC,
       FINAL.CUST_NM,		--//        FINAL.CUST_NM,
       FINAL.CUST_NO,		--//        FINAL.CUST_NO,
       FINAL.RETAIL_ENVIRONMENT,		--//        FINAL.RETAIL_ENVIRONMENT,
       --FINAL.STORE_NAME,
       --FINAL.CUSTOMER_STORE_CODE,
       FINAL.DISTRIBUTOR_CODE,		--//        FINAL.DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       FINAL.SOLD_TO_CODE,		--//        FINAL.SOLD_TO_CODE,
       FINAL.STORE_CODE,		--//        FINAL.STORE_CODE,
       STORE_NAME,
       LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0') AS EAN,		--//        ltrim(MSL.sku_unique_identifier,'0') AS EAN,
	   sku_code,
	   sku_description,
SYSDATE() as crtd_dttm		--// SYSDATE
FROM  msl_data as MSL
inner join  (select * from wks_korea_base_retail_excellence_1) FINAL		--//                   AND ATTR.COUNTRY = REG_SO.CNTRY_CD) FINAL
				  ON  UPPER (MSL.RETAIL_ENVIRONMENT) = UPPER (FINAL.RETAIL_ENVIRONMENT)		--// 				  ON  UPPER (MSL.retail_environment) = UPPER (FINAL.retail_environment)
				  AND UPPER (LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0')) = UPPER (LTRIM(FINAL.EAN,'0'))		--// 				  AND UPPER (LTRIM(MSL.sku_unique_identifier,'0')) = UPPER (LTRIM(FINAL.EAN,'0'))
WHERE MSL.JJ_MNTH_ID >= (SELECT last_16mnths		--// WHERE MSL.JJ_MNTH_ID >= (SELECT last_18mnths
                         FROM edw_vw_cal_retail_excellence_dim)		--//                          FROM rg_edw.edw_vw_cal_Retail_excellence_Dim)
AND   MSL.JJ_MNTH_ID <= (SELECT prev_mnth FROM edw_vw_cal_retail_excellence_dim)		
),
final1 as
(
    select 
    fisc_yr::numeric(18,0) AS fisc_yr,
fisc_per::varchar(22) AS fisc_per,
country::varchar(100) AS country,
data_source::varchar(14) AS data_source,
channel_desc::varchar(225) AS channel_desc,
sales_office_desc::varchar(40) AS sales_office_desc,
sales_grp_desc::varchar(100) AS sales_grp_desc,
cust_nm::varchar(100) AS cust_nm,
cust_no::varchar(100) AS cust_no,
retail_environment::varchar(150) AS retail_environment,
distributor_code::varchar(150) AS distributor_code,
distributor_name::varchar(534) AS distributor_name,
sold_to_code::varchar(255) AS sold_to_code,
store_code::varchar(150) AS store_code,
store_name::varchar(901) AS store_name,
ean::varchar(100) AS ean,
sku_code::varchar(40) AS sku_code,
sku_description::varchar(300) AS sku_description,
crtd_dttm::timestamp without time zone AS crtd_dttm
from kr_itg_re_msl_list
)
--final select
select * from final1

