with ITG_RE_MSL_INPUT_DEFINITION as (
    select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),
WKS_CNSC_BASE_RETAIL_EXCELLENCE as (
    select * from {{ ref('chnwks_integration__wks_cnsc_base_retail_excellence') }}
),
edw_vw_cal_Retail_excellence_Dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
itg_mds_cn_otc_product_mapping as (
    select * from {{ source('chnitg_integration', 'itg_mds_cn_otc_product_mapping') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

transformation as (
SELECT derived_table1.fisc_yr,
       derived_table1.JJ_MNTH_ID AS fisc_per,
       derived_table1.Distributor_Code AS Distributor_Code,
       derived_table1.Distributor_Name AS Distributor_Name,
       derived_table1.channel AS Sell_Out_Channel,
       derived_table1.store_type AS Sell_Out_RE,
       derived_table1.store_code AS Store_Code,
       derived_table1.store_name AS Store_Name,
       derived_table1.region AS Region,
       derived_table1.zone AS zone_name,
       derived_table1.product_code AS product_code,
	   derived_table1.product_desc AS product_name,
       derived_table1.store_type,
       derived_table1.Product_Brand,
       'China Selfcare' AS prod_hier_l1
FROM (SELECT DISTINCT base.start_date,
             base.end_date,
             base.market,
             base.retail_environment,
             base.channel AS channel1,
             base.sku_code,
             --base.cal_mo_1,
             base.fisc_yr,
             base.JJ_MNTH_ID,
             st.store_Code,
             st.store_name,
             st.Region AS region,
             st.store_type,
             st.channel AS channel,
             st.Distributor_Code,
             st.Distributor_Name,
             st.Zone,
             st.product_code,
			 st.product_desc,
			 localbrand.brand_en as Product_Brand
             FROM (SELECT *
                   FROM (SELECT DISTINCT start_date,
                                 END_DATE,
                                market,
                                retail_environment,
                                channel,
                                sku_unique_identifier as sku_code
                         FROM ITG_RE_MSL_INPUT_DEFINITION
                         WHERE MARKET = 'China Selfcare') MSL
                     LEFT JOIN (SELECT DISTINCT fisc_yr,
                                       --cal_mo_1,
                                       SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
                                FROM edw_calendar_dim
                                --CHANGED MONTH LOGIC 19 -> 17
                                WHERE JJ_MNTH_ID >= (select last_17mnths from edw_vw_cal_Retail_excellence_Dim)
						          and JJ_MNTH_ID <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)  
								--cal_mo_1 > TO_CHAR(add_months(SYSDATE,-18),'YYYYMM')::NUMERIC
                                --AND cal_day <= SYSDATE
								) cal
                            ON TO_CHAR (TO_DATE (msl.start_date,'DD/MM/YYYY'),'YYYYMM')<= cal.JJ_MNTH_ID
                           AND TO_CHAR (TO_DATE (msl.END_DATE,'DD/MM/YYYY'),'YYYYMM')>= cal.JJ_MNTH_ID) base
        LEFT JOIN (SELECT store_Code,
                          store_name,
                          Region,
                          store_type,
                          distributor_code,
                          distributor_name,
                          ZONE_OR_AREA as zone,
                          channel,
                          MSL_PRODUCT_CODE as product_code,
						  MSL_PRODUCT_DESC as product_desc
                   FROM WKS_CNSC_BASE_RETAIL_EXCELLENCE) st
               ON base.Retail_Environment = st.store_type
              AND base.channel = st.channel
              AND base.sku_code = st.product_code
      LEFT JOIN (SELECT DISTINCT xjp_code, brand_en FROM itg_mds_cn_otc_product_mapping)localbrand ON localbrand.xjp_code=base.sku_code  

	     ) derived_table1 where store_type is not null),

final as (
select 
fisc_yr::integer AS fisc_yr,
fisc_per::varchar(22) AS fisc_per,
distributor_code::varchar(100) AS distributor_code,
distributor_name::varchar(356) AS distributor_name,
sell_out_channel::varchar(50) AS sell_out_channel,
sell_out_re::varchar(255) AS sell_out_re,
store_code::varchar(100) AS store_code,
store_name::varchar(601) AS store_name,
region::varchar(150) AS region,
zone_name::varchar(150) AS zone_name,
product_code::varchar(150) AS product_code,
product_name::varchar(300) AS product_name,
store_type::varchar(255) AS store_type,
product_brand::varchar(200) AS product_brand,
prod_hier_l1::varchar(14) AS prod_hier_l1,
from transformation
)         

select * from final         