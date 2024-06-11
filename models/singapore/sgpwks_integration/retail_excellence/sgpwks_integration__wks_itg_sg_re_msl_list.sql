--overwriding default sql header as we dont want to change timezone to singapore
--import cte
with itg_re_msl_input_definition as (
    select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ source('aspedw_integration', 'edw_calendar_dim') }}
),
itg_mds_sg_customer_hierarchy as (
    select * from {{ source('sgpitg_integration', 'itg_mds_sg_customer_hierarchy') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
wks_singapore_base_retail_excellence as (
    select * from {{ ref('sgpwks_integration__wks_singapore_base_retail_excellence') }}
),

--final cte
sg_itg_re_msl_list as (
SELECT DISTINCT MSL.YEAR,		--// SELECT DISTINCT MSL.YEAR,
       MSL.JJ_MNTH_ID AS MNTH_ID,		--//        MSL.JJ_MNTH_ID AS MNTH_ID,
       STORE_DET.SOLD_TO_CODE,		--//        STORE_DET.SOLD_TO_CODE,
       STORE_DET.DISTRIBUTOR_CODE,		--//        STORE_DET.DISTRIBUTOR_CODE,
       STORE_DET.DISTRIBUTOR_NAME,		--//        STORE_DET.DISTRIBUTOR_NAME,
       STORE_DET.STORE_CODE,		--//        STORE_DET.STORE_CODE,
       STORE_DET.STORE_NAME,		--//        STORE_DET.STORE_NAME,
       STORE_DET.STORE_TYPE,		--//        STORE_DET.STORE_TYPE,
       STORE_DET.CUSTOMER_SEGMENTATION_CODE,		--//        STORE_DET.CUSTOMER_SEGMENTATION_CODE,
       STORE_DET.CUSTOMER_SEGMENTATION_LEVEL_2_CODE,		--//        STORE_DET.CUSTOMER_SEGMENTATION_LEVEL_2_CODE,
       STORE_DET.CHANNEL AS SELLOUT_CHANNEL,		--//        STORE_DET.CHANNEL AS SELLOUT_CHANNEL,
       STORE_DET.CUSTOMER_GROUP_CODE,		--//        STORE_DET.CUSTOMER_GROUP_CODE,
       STORE_DET.CUSTOMER_NUMBER,		--//        STORE_DET.CUSTOMER_NUMBER,
       'NA' AS REGION,
       'NA' AS ZONE_NAME,
       MSL.CHANNEL,		--//        MSL.CHANNEL,
       MSL.CUSTOMER AS CUSTOMER_NAME,		--//        MSL.CUSTOMER AS CUSTOMER_NAME,
       MSL.SKU_UNIQUE_IDENTIFIER AS master_code,		--//        MSL.SKU_UNIQUE_IDENTIFIER AS master_code,
       STORE_DET.CUSTOMER_PRODUCT_DESC,		--//        STORE_DET.CUSTOMER_PRODUCT_DESC,
       STORE_DET.MAPPED_SKU_CD,		--//        STORE_DET.MAPPED_SKU_CD,
SYSDATE()		--// SYSDATE
FROM (SELECT DISTINCT CAL.FISC_YR AS YEAR,		--// FROM (SELECT DISTINCT CAL.FISC_YR AS YEAR,
             --CAL.CAL_MO_1 AS CAL_MNTH_ID,
             CAL.JJ_MNTH_ID,		--//              CAL.JJ_MNTH_ID,
             A.CHANNEL,		--//              A.CHANNEL,
             A.CUSTOMER,		--//              A.CUSTOMER,
             LTRIM(A.SKU_UNIQUE_IDENTIFIER,'0') AS SKU_UNIQUE_IDENTIFIER		--//              LTRIM(A.SKU_UNIQUE_IDENTIFIER,'0') AS SKU_UNIQUE_IDENTIFIER
      FROM ITG_RE_MSL_INPUT_DEFINITION A		--//       FROM RG_ITG.ITG_RE_MSL_INPUT_DEFINITION A
        LEFT JOIN (SELECT DISTINCT FISC_YR,
                          --CAL_MO_1,
                          SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
                   FROM EDW_CALENDAR_DIM) CAL		--//                    FROM RG_EDW.EDW_CALENDAR_DIM) CAL
               ON TO_CHAR (TO_DATE (A.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID		--//                ON TO_CHAR (TO_DATE (A.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID
              AND TO_CHAR (TO_DATE (A.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID		--//               AND TO_CHAR (TO_DATE (A.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID
      WHERE MARKET = 'Singapore') MSL
  LEFT JOIN (SELECT DISTINCT BASE.SOLDTO_CODE AS SOLD_TO_CODE,		--//   LEFT JOIN (SELECT DISTINCT BASE.SOLDTO_CODE AS SOLD_TO_CODE,
                    BASE.DISTRIBUTOR_CODE,		--//                     BASE.DISTRIBUTOR_CODE,
                    BASE.DISTRIBUTOR_NAME,		--//                     BASE.DISTRIBUTOR_NAME,
                    LTRIM(BASE.STORE_CODE,0) AS STORE_CODE,		--//                     LTRIM(BASE.STORE_CODE,0) AS STORE_CODE,
                    BASE.STORE_NAME AS STORE_NAME,		--//                     BASE.STORE_NAME AS STORE_NAME,
                    BASE.MASTER_CODE,		--//                     BASE.MASTER_CODE,
                    BASE.CUSTOMER_PRODUCT_DESC,		--//                     BASE.CUSTOMER_PRODUCT_DESC,
                    BASE.MAPPED_SKU_CD,		--//                     BASE.MAPPED_SKU_CD,
                    BASE.STORE_TYPE,		--//                     BASE.STORE_TYPE,
                    CUST.CUSTOMER_SEGMENTATION_CODE,		--//                     CUST.CUSTOMER_SEGMENTATION_CODE,
                    CUST.CUSTOMER_SEGMENTATION_LEVEL_2_CODE,		--//                     CUST.CUSTOMER_SEGMENTATION_LEVEL_2_CODE,
                    CUST.CHANNEL,		--//                     CUST.CHANNEL,
                    CUST.CUSTOMER_GROUP_CODE,		--//                     CUST.CUSTOMER_GROUP_CODE,
                    CUST.CUSTOMER_NUMBER,		--//                     CUST.CUSTOMER_NUMBER,
                    CUST.CUSTOMER_NAME		--//                     CUST.CUSTOMER_NAME
             FROM (SELECT DISTINCT LTRIM(SOLDTO_CODE,'0') AS SOLDTO_CODE,
                          LTRIM(DISTRIBUTOR_CODE,'0') AS DISTRIBUTOR_CODE,
                          DISTRIBUTOR_NAME,
                          LTRIM(STORE_CODE,'0') AS STORE_CODE,
                          STORE_NAME,
                          LTRIM(MASTER_CODE,'0') AS MASTER_CODE,
                          CUSTOMER_PRODUCT_DESC,
                          MAPPED_SKU_CD,
                          STORE_TYPE
                   FROM wks_singapore_base_retail_excellence) BASE		--//                    FROM OS_WKS.WKS_SINGAPORE_BASE_RETAIL_EXCELLENCE) BASE
               LEFT JOIN (SELECT * FROM itg_mds_sg_customer_hierarchy) CUST ON LTRIM (BASE.SOLDTO_CODE,'0') = LTRIM (CUST.CUSTOMER_NUMBER,'0')		--//                LEFT JOIN (SELECT * FROM OS_ITG.ITG_MDS_SG_CUSTOMER_HIERARCHY) CUST ON LTRIM (BASE.SOLDTO_CODE,'0') = LTRIM (CUST.CUSTOMER_NUMBER,'0')
             GROUP BY BASE.SOLDTO_CODE,		--//              GROUP BY BASE.SOLDTO_CODE,
                      BASE.DISTRIBUTOR_CODE,		--//                       BASE.DISTRIBUTOR_CODE,
                      BASE.DISTRIBUTOR_NAME,		--//                       BASE.DISTRIBUTOR_NAME,
                      BASE.STORE_CODE,		--//                       BASE.STORE_CODE,
                      BASE.STORE_NAME,		--//                       BASE.STORE_NAME,
                      BASE.MASTER_CODE,		--//                       BASE.MASTER_CODE,
                      BASE.CUSTOMER_PRODUCT_DESC,		--//                       BASE.CUSTOMER_PRODUCT_DESC,
                      BASE.MAPPED_SKU_CD,		--//                       BASE.MAPPED_SKU_CD,
                      BASE.STORE_TYPE,		--//                       BASE.STORE_TYPE,
                      CUST.CUSTOMER_SEGMENTATION_CODE,		--//                       CUST.CUSTOMER_SEGMENTATION_CODE,
                      CUST.CUSTOMER_SEGMENTATION_LEVEL_2_CODE,		--//                       CUST.CUSTOMER_SEGMENTATION_LEVEL_2_CODE,
                      CUST.CHANNEL,		--//                       CUST.CHANNEL,
                      CUST.CUSTOMER_GROUP_CODE,		--//                       CUST.CUSTOMER_GROUP_CODE,
                      CUST.CUSTOMER_NUMBER,		--//                       CUST.CUSTOMER_NUMBER,
                      CUST.CUSTOMER_NAME) STORE_DET		--//                       CUST.CUSTOMER_NAME) STORE_DET
         ON UPPER (LTRIM (MSL.CUSTOMER,'0')) = UPPER (LTRIM (STORE_DET.CUSTOMER_GROUP_CODE,'0'))		--//          ON UPPER (LTRIM (MSL.CUSTOMER,'0')) = UPPER (LTRIM (STORE_DET.CUSTOMER_GROUP_CODE,'0'))
        AND MSL.SKU_UNIQUE_IDENTIFIER = STORE_DET.MASTER_CODE		--//         AND MSL.SKU_UNIQUE_IDENTIFIER = STORE_DET.MASTER_CODE
WHERE MSL.JJ_MNTH_ID >= (SELECT last_18mnths		--// WHERE MSL.JJ_MNTH_ID >= (SELECT last_18mnths
                         FROM edw_vw_cal_retail_excellence_dim)		--//                          FROM rg_edw.edw_vw_cal_Retail_excellence_Dim)
AND   MSL.JJ_MNTH_ID <= (SELECT prev_mnth FROM edw_vw_cal_retail_excellence_dim)		--// AND   MSL.JJ_MNTH_ID <= (SELECT prev_mnth FROM rg_edw.edw_vw_cal_Retail_excellence_Dim)
AND   STORE_CODE IS NOT NULL
AND   DISTRIBUTOR_CODE IS NOT NULL

),
final as
(
    select 
    year::numeric(18,0) AS year,
mnth_id::varchar(22) AS mnth_id,
sold_to_code::varchar(255) AS sold_to_code,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(356) AS distributor_name,
store_code::varchar(100) AS store_code,
store_name::varchar(601) AS store_name,
store_type::varchar(255) AS store_type,
customer_segmentation_code::varchar(256) AS customer_segmentation_code,
customer_segmentation_level_2_code::varchar(256) AS customer_segmentation_level_2_code,
sellout_channel::varchar(50) AS sellout_channel,
customer_group_code::varchar(100) AS customer_group_code,
customer_number::varchar(10) AS customer_number,
region::varchar(2) AS region,
zone_name::varchar(2) AS zone_name,
channel::varchar(50) AS channel,
customer_name::varchar(50) AS customer_name,
master_code::varchar(100) AS master_code,
customer_product_desc::varchar(150) AS customer_product_desc,
mapped_sku_cd::varchar(40) AS mapped_sku_cd,
crt_dttm::timestamp without time zone AS crt_dttm
from sg_itg_re_msl_list
)
--final select
select * from final

