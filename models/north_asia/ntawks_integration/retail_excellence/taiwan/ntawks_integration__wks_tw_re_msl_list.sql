--import cte   
with edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
itg_re_msl_input_definition as (
    select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
),
wks_tw_base_re as (
    select * from {{ ref('ntawks_integration__wks_tw_base_re') }}
),
edw_vw_pop6_products as (
    --select * from {{ source('ntaedw_integration', 'edw_vw_pop6_products') }}
    select * from {{ ref('aspedw_integration__edw_vw_pop6_products') }}
),

--Final CTE

TW_re_msl_list
AS
(SELECT DISTINCT jj_year,
       jj_mnth_id,
       soldto_code,
       data_src,
       distributor_code,
       distributor_name,
       store_code,
       store_name,
       store_type,
       ean,
       store_grade,
       Sell_Out_Channel,
       retail_environment,
       market,
       cntry_cd,
       prod_hier_l1,
       prod_hier_l2,
       prod_hier_l3,
       prod_hier_l4,
       prod_hier_l5,
       prod_hier_l6,
       prod_hier_l7,
       prod_hier_l8,
       prod_hier_l9,
       sku_code,
       --MAX(msl_product_desc) OVER (PARTITION BY LTRIM(ean,0) ORDER BY LENGTH(msl_product_desc) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msl_product_desc,
       msl_product_desc,
       region,
       zone_or_area
FROM (SELECT DISTINCT SUBSTRING(base.jj_mnth_id,1,4) AS jj_year,
             base.jj_mnth_id,
             noo.soldto_code,
             noo.data_src,
             noo.distributor_code,
             noo.distributor_name,
             noo.distributor_name_new,
             noo.store_code,
             noo.store_name,
             noo.store_type,
             LTRIM(base.sku_unique_identifier,0) AS ean,
             base.store_grade,
             noo.channel AS Sell_Out_Channel,
             UPPER(base.retail_environment) AS retail_environment,
             base.market,
             base.cntry_cd,
             epd.prod_hier_l1,
             epd.prod_hier_l2,
             epd.prod_hier_l3,
             epd.prod_hier_l4,
             epd.prod_hier_l5,
             prod_hier_l6,
             prod_hier_l7,
             prod_hier_l8,
             epd.prod_hier_l9,
             noo.sku_code,
             noo.msl_product_desc,
             noo.region,
             noo.zone_or_area,
             base.customer_name
      FROM (SELECT DISTINCT *,
                   CASE
                     WHEN msl.market = 'Taiwan' THEN 'TW'
                   END cntry_cd,
                   CASE
                     WHEN customer LIKE '%Carrefour%' THEN 'Carrefour'
                     WHEN customer LIKE '%PX%' THEN 'PX'
                     WHEN customer LIKE '%Watson%' THEN 'Watsons'
                     ELSE customer
                   END AS customer_name
            FROM itg_re_msl_input_definition msl
              LEFT JOIN (SELECT DISTINCT SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
                         FROM EDW_CALENDAR_DIM
                         WHERE jj_mnth_id >= (SELECT last_17mnths FROM edw_vw_cal_Retail_excellence_Dim)
                         AND   jj_mnth_id <= (SELECT last_2mnths FROM edw_vw_cal_Retail_excellence_Dim)) cal
                     ON TO_CHAR (TO_DATE (msl.start_date,'DD/MM/YYYY'),'YYYYMM') <= cal.jj_mnth_id
                    AND TO_CHAR (TO_DATE (msl.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= cal.jj_mnth_id
            WHERE UPPER(msl.market) = 'TAIWAN'
            --in ('Taiwan')	 and msl.retail_environment='MT'
            ) base
        LEFT JOIN (SELECT DISTINCT data_src,
                          distributor_code,
                          distributor_name,
                          CASE
                            WHEN distributor_name LIKE '%Carrefour%' THEN 'Carrefour'
                            WHEN distributor_name LIKE '%PX%' THEN 'PX'
                            WHEN distributor_name LIKE '%Watsons%' THEN 'Watsons'
                            ELSE distributor_name
                          END AS distributor_name_new,
                          store_code,
                          store_type,
                          region,
                          zone_or_area,
                          RETAIL_ENVIRONMENT,
                          store_grade,
                          CHANNEL_NAME AS channel,
                          CNTRY_CD,
                          soldto_code,
                          store_name,
                          msl_product_desc,
                          sku_code,
                          EAN
                   FROM WKS_TW_BASE_RE
                   --WHERE CNTRY_CD in  ('TW') and data_src='POS' and RETAIL_ENVIRONMENT='MT'
                   ) NOO
               ON UPPER (base.channel) = UPPER (noo.channel)
              AND UPPER(base.customer_name) = UPPER(noo.distributor_name_new)
              AND UPPER(base.Retail_Environment) = UPPER(noo.Retail_Environment)
              AND UPPER(base.cntry_cd) = UPPER(noo.CNTRY_CD)
              AND TRIM (base.sku_unique_identifier) = TRIM (noo.EAN)
        LEFT JOIN (SELECT DISTINCT prd.country_l1 AS prod_hier_l1,
                          prd.regional_franchise_l2 AS prod_hier_l2,
                          prd.franchise_l3 AS prod_hier_l3,
                          prd.brand_l4 AS prod_hier_l4,
                          prd.sub_category_l5 AS prod_hier_l5,
                          prd.platform_l6 AS prod_hier_l6,
                          prd.variance_l7 AS prod_hier_l7,
                          prd.pack_size_l8 AS prod_hier_l8,
                          NULL AS prod_hier_l9,
                          prd.barcode
                   FROM edw_vw_pop6_products prd
                   WHERE cntry_cd = 'TW') epd ON UPPER (TRIM (base.sku_unique_identifier)) = UPPER (TRIM (epd.barcode))
      -- and row_no=1
    )
    WHERE STORE_CODE IS NOT NULL
    AND   DISTRIBUTOR_CODE IS NOT NULL
),

--final select
final as (
    select
jj_year::varchar(16) as jj_year ,
jj_mnth_id::varchar(22) as jj_mnth_id ,
soldto_code::varchar(255) as soldto_code ,
data_src::varchar(20) as data_src,
distributor_code::varchar(32) as distributor_code ,
distributor_name::varchar(255) as distributor_name ,
store_code::varchar(100) as store_code ,
store_name::varchar(601) as store_name ,
store_type::varchar(255) as store_type ,
ean::varchar(500) as ean ,
store_grade::varchar(20) as store_grade ,
sell_out_channel::varchar(150) as sell_out_channel ,
retail_environment::varchar(75) as retail_environment ,
market::varchar(50) as market ,
cntry_cd::varchar(2) as cntry_cd ,
prod_hier_l1::varchar(200) as prod_hier_l1 ,
prod_hier_l2::varchar(200) as prod_hier_l2 ,
prod_hier_l3::varchar(200) as prod_hier_l3 ,
prod_hier_l4::varchar(200) as prod_hier_l4 ,
prod_hier_l5::varchar(200) as prod_hier_l5 ,
prod_hier_l6::varchar(200) as prod_hier_l6 ,
prod_hier_l7::varchar(200) as prod_hier_l7 ,
prod_hier_l8::varchar(200) as prod_hier_l8 ,
prod_hier_l9::varchar(1) as prod_hier_l9 ,
sku_code::varchar(40) as sku_code ,
msl_product_desc::varchar(300) as msl_product_desc ,
region::varchar(150) as region ,
zone_or_area::varchar(150) as zone_or_area 
from TW_re_msl_list
)

select * from final