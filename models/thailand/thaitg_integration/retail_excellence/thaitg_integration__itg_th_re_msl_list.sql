with itg_re_msl_input_definition as (
    select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
wks_th_base_retail_excellence as (
    select * from {{ ref('thawks_integration__wks_th_base_retail_excellence') }}
),

transformation as (SELECT FINAL.FISC_YR,		--// SELECT final.fisc_yr,
       FINAL.JJ_MNTH_ID AS fisc_per,		--//        final.JJ_MNTH_ID AS fisc_per,
       FINAL.MARKET AS Market,		--//        final.market AS Market,
       FINAL.RETAIL_ENV AS Retail_Environment,		--//        final.retail_env AS Retail_Environment,
       FINAL.DISTRIBUTOR_CODE AS Distributor_Code,		--//        final.Distributor_Code AS Distributor_Code,
       FINAL.DISTRIBUTOR_NAME,		--//        final.Distributor_Name,
       FINAL.CHANNEL AS channel,		--//        final.channel AS channel,
       FINAL.STORE_NAME,		--//        final.STORE_NAME,
       FINAL.STORE_CODE AS Store_Code,		--//        final.STORE_Code AS Store_Code,
       FINAL.STORE_TYPE AS Store_Type,		--//        final.store_type AS Store_Type,
       FINAL.REGION AS Region,		--//        final.region AS Region,
       FINAL.ZONE AS Zone,		--//        final.ZONE AS Zone,
       FINAL.DATA_SRC AS Data_Src,		--//        final.Data_Src AS Data_Src,
       FINAL.MSL_PRODUCT_CODE AS PRODUCT_CODE,		--//        final.msl_product_code AS PRODUCT_CODE,
       FINAL.MSL_PRODUCT_DESC AS product_description,		--//        final.msl_product_desc AS product_description,
       FINAL.SOLDTO_CODE,		--//        final.soldto_code,
       'Thailand' AS prod_hier_l1,
SYSDATE() as created_date		--// SYSDATE
FROM (SELECT BASE.START_DATE,		--// FROM (SELECT base.start_date,
             BASE.END_DATE,		--//              base.end_date,
             BASE.MARKET,		--//              base.market,
             BASE.RETAIL_ENVIRONMENT,		--//              base.retail_environment,
             BASE.CHANNEL AS channel1,		--//              base.Channel AS channel1,
             BASE.JJ_MNTH_ID,		--//              base.jj_mnth_id,
             BASE.FISC_YR,		--//              base.fisc_yr,
             BASE.SKU_UNIQUE_IDENTIFIER AS ean,		--//              base.sku_unique_identifier AS ean,
             OFFTAKE.DATA_SRC,		--//              offtake.Data_Src,
             OFFTAKE.CNTRY_CD,		--//              offtake.CNTRY_CD,
             OFFTAKE.CNTRY_NM,		--//              offtake.CNTRY_NM,
             OFFTAKE.DISTRIBUTOR_CODE AS distributor_code,		--//              offtake.distributor_code AS distributor_code,
             OFFTAKE.DISTRIBUTOR_NAME AS distributor_name,		--//              offtake.distributor_name AS distributor_name,
             OFFTAKE.STORE_CODE,		--//              offtake.store_code,
             OFFTAKE.STORE_NAME,		--//              offtake.store_name,
             OFFTAKE.STORE_TYPE AS store_type,		--//              offtake.store_type AS store_type,
             OFFTAKE.SOLDTO_CODE,		--//              offtake.soldto_code,
             OFFTAKE.REGION,		--//              offtake.region,
             OFFTAKE.ZONE,		--//              offtake.ZONE,
             OFFTAKE.EAN_CODE AS msl_product_code,		--//              offtake.EAN_CODE AS msl_product_code,
             OFFTAKE.MAPPED_SKU_DESCRIPTION AS msl_product_desc,		--//              offtake.MAPPED_sku_description AS msl_product_desc,
             OFFTAKE.RETAIL_ENVIRONMENT AS retail_env,		--//              offtake.RETAIL_ENVIRONMENT AS retail_env,
             OFFTAKE.CHANNEL		--//              offtake.channel
      FROM (SELECT DISTINCT start_date,
                   END_DATE,
                   market,
                   retail_environment,
                   channel,
                   sku_unique_identifier,
                   jj_mnth_id,
                   fisc_yr
            FROM (SELECT DISTINCT TO_CHAR(TO_DATE(start_date,'DD/MM/YYYY'),'YYYYMM')::NUMERIC AS start_date,
                         TO_CHAR(TO_DATE(END_DATE,'DD/MM/YYYY'),'YYYYMM')::NUMERIC AS END_DATE,
                         market,
                         retail_environment,
                         LTRIM(sku_unique_identifier,'0') AS sku_unique_identifier,
                         channel
                  FROM ITG_RE_MSL_INPUT_DEFINITION		--//                   FROM rg_itg.Itg_re_msl_input_definition
                  WHERE market = 'Thailand') msl
              LEFT JOIN (SELECT DISTINCT fisc_yr,
                                cal_mo_1,
                                (SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7))::NUMERIC AS JJ_MNTH_ID
                         FROM EDW_CALENDAR_DIM		--//                          FROM rg_edw.edw_calendar_dim
                         WHERE jj_mnth_id >= (SELECT last_18mnths
                                              FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC		--//                                               FROM rg_edw.edw_vw_cal_Retail_excellence_Dim)::NUMERIC
                         AND   jj_mnth_id <= (SELECT prev_mnth FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC) cal		--//                          AND   jj_mnth_id <= (SELECT prev_mnth FROM rg_edw.edw_vw_cal_Retail_excellence_Dim)::NUMERIC) cal
                     ON start_date <= CAL.JJ_MNTH_ID		--//                      ON start_date <= cal.JJ_MNTH_ID
                    AND END_DATE >= CAL.JJ_MNTH_ID) base		--//                     AND END_DATE >= cal.JJ_MNTH_ID) base
        LEFT JOIN (SELECT Data_Src,
                          CNTRY_CD,
                          CNTRY_NM,
                          Region,
                          ZONE,
                          Distributor_Name,
                          Distributor_code,
                          Channel,
                          store_code,
                          store_name,
                          store_type,
                          soldto_code,
                          --msl_product_desc,
                          EAN_CODE,
                          MAPPED_sku_description,
                          RETAIL_ENVIRONMENT
                   FROM WKS_TH_BASE_RETAIL_EXCELLENCE) offtake		--//                    FROM OS_WKS.WKS_TH_BASE_RETAIL_EXCELLENCE) offtake
               ON UPPER (OFFTAKE.RETAIL_ENVIRONMENT) = UPPER (BASE.RETAIL_ENVIRONMENT)		--//                ON UPPER (offtake.RETAIL_ENVIRONMENT) = UPPER (base.Retail_Environment)
              AND OFFTAKE.EAN_CODE = BASE.SKU_UNIQUE_IDENTIFIER) final		--//               AND offtake.EAN_CODE = base.sku_unique_identifier) final
WHERE FINAL.STORE_CODE IS NOT NULL		--// WHERE final.store_code IS NOT NULL
AND   FINAL.DISTRIBUTOR_CODE IS NOT NULL),

final as (
    select
    fisc_yr::integer AS fisc_yr,
fisc_per::numeric(18) AS fisc_per,
market::varchar(50) AS market,
retail_environment::varchar(150) AS retail_environment,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(356) AS distributor_name,
channel::varchar(150) AS channel,
store_name::varchar(601) AS store_name,
store_code::varchar(100) AS store_code,
store_type::varchar(255) AS store_type,
region::varchar(150) AS region,
zone::varchar(150) AS zone,
data_src::varchar(14) AS data_src,
product_code::varchar(150) AS product_code,
product_description::varchar(225) AS product_description,
soldto_code::varchar(255) AS soldto_code,
prod_hier_l1::varchar(8) AS prod_hier_l1,
created_date::timestamp AS created_date,
from transformation
)

select * from final