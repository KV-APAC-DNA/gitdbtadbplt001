--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
--final cte
anz_pos_base_retail_excellence as 
(
SELECT CNTRY_CD,
       MD5(nvl (DISTRIBUTOR_CODE,'dc') ||nvl (DISTRIBUTOR_NAME,'dn') ||nvl (EAN,'ean') ||nvl (CHANNEL_NAME,'cn') ||nvl (RETAIL_ENVIRONMENT,'re') ||nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||nvl (GLOBAL_PRODUCT_BRAND,'gpb') ||nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps') ||nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') ||nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc') ||nvl(Region,'rg')||nvl(zone_or_area,'ar')||nvl(store_grade,'sg')||nvl(sku_code,'scd')) AS SELLOUT_DIM_KEY,
       CNTRY_NM,
       DATA_SRC,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       store_grade,list_price,sku_code,
       EAN,
       RETAIL_ENVIRONMENT,
	   Region,zone_or_area,
       CHANNEL_NAME,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       YEAR,
       MNTH_ID,
       sales_value_list_price,
       SO_SLS_QTY,
       SO_SLS_VALUE,
       SO_SLS_QTY as SO_AVG_QTY,
	   numeric_distribution,
	   store_count_where_scanned,
	   (store_count_where_scanned*100)/(case when numeric_distribution=0 then 1 else nvl(numeric_distribution,1)end) as universe_stores ,
       SYSDATE() as crt_dttm
FROM (SELECT country_code AS CNTRY_CD,
             country_name AS CNTRY_NM,
             data_source AS DATA_SRC,
             DISTRIBUTOR_NAME,
             DISTRIBUTOR_CODE,
             ltrim(msl_product_code,0) as EAN,
             
			max(sku_code) over ( PARTITION BY ltrim(ean,0) ORDER BY length(sku_code) DESC,cal_date desc,sku_code desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS sku_code,
			max(list_price) over ( PARTITION BY ltrim(ean,0) ORDER BY length(sku_code) DESC,cal_date desc,sku_code desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS list_price ,
             upper(retail_env) AS RETAIL_ENVIRONMENT,
			 store_grade,
			 Region,zone_or_area,
             channel AS CHANNEL_NAME,
             GLOBAL_PRODUCT_FRANCHISE,
             GLOBAL_PRODUCT_BRAND,
             GLOBAL_PRODUCT_SUB_BRAND,
             GLOBAL_PRODUCT_SEGMENT,
             GLOBAL_PRODUCT_SUBSEGMENT,
             GLOBAL_PRODUCT_CATEGORY,
             GLOBAL_PRODUCT_SUBCATEGORY,
             YEAR,
             MNTH_ID,
			 sellout_value_list_price AS sales_value_list_price,
             SELLOUT_SALES_QUANTITY AS SO_SLS_QTY,
             SELLOUT_SALES_VALUE AS SO_SLS_VALUE,
			 numeric_distribution,
			 nvl(store_count_where_scanned,0) as store_count_where_scanned,
			 row_number() over (partition by distributor_name,ean,mnth_id,country_code order by cal_date desc,nvl(store_count_where_scanned,0) desc,nvl(numeric_distribution,0) desc) as rno
      from  EDW_RPT_REGIONAL_SELLOUT_OFFTAKE base --rg_edw.edw_rpt_regional_sellout_offtake_AnZ_POS_bkp base
	  WHERE COUNTRY_CODE in  ('AU','NZ') and data_source='POS' and upper(DISTRIBUTOR_NAME) in ('AU WOOLWORTHS SCAN' , 'AU COLES GROUP SCAN','NZ WOOLWORTHS SCAN','AU MY CHEMIST GROUP SCAN','NEW ZEALAND PHARMACY COMBINED (INCL CWH)','TOTAL SUBSCRIBED PHARMACY NZ')
	  AND   MNTH_ID >= (SELECT last_28mnths
                  FROM edw_vw_cal_retail_excellence_dim)::NUMERIC
      AND   MNTH_ID <= (SELECT last_2mnths FROM edw_vw_cal_retail_excellence_dim)::NUMERIC
      ) SELLOUT where rno=1
),
final as
(
select 
cntry_cd::VARCHAR(2) AS cntry_cd,
sellout_dim_key::VARCHAR(32) AS sellout_dim_key,
cntry_nm::VARCHAR(50) AS cntry_nm,
data_src::VARCHAR(14) AS data_src,
distributor_code::VARCHAR(100) AS distributor_code,
distributor_name::VARCHAR(255) AS distributor_name,
store_grade::VARCHAR(150) AS store_grade,
list_price::NUMERIC(38,6) AS list_price,
sku_code::VARCHAR(40) AS sku_code,
ean::VARCHAR(150) AS ean,
retail_environment::VARCHAR(225) AS retail_environment,
region::VARCHAR(150) AS region,
zone_or_area::VARCHAR(150) AS zone_or_area,
channel_name::VARCHAR(150) AS channel_name,
global_product_franchise::VARCHAR(30) AS global_product_franchise,
global_product_brand::VARCHAR(30) AS global_product_brand,
global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
global_product_segment::VARCHAR(50) AS global_product_segment,
global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
global_product_category::VARCHAR(50) AS global_product_category,
global_product_subcategory::VARCHAR(50) AS global_product_subcategory,
year::numeric(18,0) AS year,
mnth_id::VARCHAR(23) AS mnth_id,
sales_value_list_price::NUMERIC(38,12) AS sales_value_list_price,
so_sls_qty::NUMERIC(38,6) AS so_sls_qty,
so_sls_value::NUMERIC(38,6) AS so_sls_value,
so_avg_qty::NUMERIC(38,6) AS so_avg_qty,
numeric_distribution::NUMERIC(20,4) AS numeric_distribution,
store_count_where_scanned::NUMERIC(20,4) AS store_count_where_scanned,
universe_stores::NUMERIC(38,14) AS universe_stores,
crt_dttm::timestamp without time zone as crt_dttm
from anz_pos_base_retail_excellence
)
--final select
select * from final

