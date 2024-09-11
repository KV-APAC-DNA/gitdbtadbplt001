--overwriding default sql header as we dont want to change timezone to singapore
--import cte
with itg_re_msl_input_definition as (
    select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
wks_anz_sellout_c360_mapped_sku_cd as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_c360_mapped_sku_cd') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_anz_sellout_base_retail_excellence as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_base_retail_excellence') }}
),
edw_material_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_sales_org_dim as (
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
itg_trax_md_product as (
    select * from {{ ref('pcfitg_integration__itg_trax_md_product') }}
),

--final cte
anz_sellout_itg_re_msl_list as (
SELECT DISTINCT jj_year,
       jj_mnth_id,
       soldto_code,
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
       prod_hier_l3,
       prod_hier_l4,
       prod_hier_l5,
       prod_hier_l9,
       sku_code,
       list_price,
       MAX(msl_product_desc) OVER (PARTITION BY LTRIM(ean,0) ORDER BY LENGTH(msl_product_desc) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS msl_product_desc,
       region,
       zone_or_area,
       COALESCE(NULLIF(LTRIM(sku_code_master,'0'),'NA'),LTRIM(MAPPED_SKU_CD,0)) AS MAPPED_SKU_CD,
       sysdate() as crt_dttm
FROM (SELECT DISTINCT SUBSTRING(base.jj_mnth_id,1,4) AS jj_year,
             base.jj_mnth_id,
             noo.soldto_code,
             distributor_code,
             noo.distributor_name,
             noo.store_code,
             noo.store_name,
             noo.store_type,
             LTRIM(base.sku_unique_identifier,0) AS ean,
             base.store_grade,
             noo.channel AS Sell_Out_Channel,
             UPPER(base.retail_environment) AS retail_environment,
             base.market,
             base.cntry_cd,
             epd.prod_hier_l3,
             epd.prod_hier_l4,
             epd.prod_hier_l5,
             epd.prod_hier_l9,
             noo.sku_code,
             noo.list_price,
             noo.msl_product_desc,
             noo.region,
             noo.zone_or_area,
             sku.sku_code AS sku_code_master,
             mat.MAPPED_SKU_CD
      FROM (SELECT DISTINCT *,
                   CASE
                     WHEN msl.market = 'Australia' THEN 'AU'
                     WHEN msl.market = 'New Zealand' THEN 'NZ'
                   END cntry_cd
            FROM itg_re_msl_input_definition msl
              LEFT JOIN (SELECT DISTINCT SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
                         FROM EDW_CALENDAR_DIM
                         WHERE jj_mnth_id >= (SELECT last_17mnths
                                              FROM edw_vw_cal_Retail_excellence_Dim)
                         AND   jj_mnth_id <= (SELECT last_2mnths
                                              FROM edw_vw_cal_Retail_excellence_Dim)
                         ) cal
                     ON TO_CHAR (TO_DATE (msl.start_date,'DD/MM/YYYY'),'YYYYMM') <= cal.jj_mnth_id
                    AND TO_CHAR (TO_DATE (msl.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= cal.jj_mnth_id
            WHERE msl.market IN ('Australia','New Zealand')) base
        LEFT JOIN (SELECT DISTINCT distributor_code,
                          distributor_name,
                          store_code,
                          store_type,
                          region,
                          zone_or_area,
                          RETAIL_ENVIRONMENT AS RE,
                          store_grade,
                          CHANNEL_NAME AS channel,
                          CNTRY_CD,
                          soldto_code,
                          store_name,
                          list_price,
                          msl_product_desc,
                          sku_code,
                          EAN
                   FROM wks_anz_sellout_base_retail_excellence
                   WHERE CNTRY_CD IN ('AU','NZ')
                   AND   data_src = 'SELL-OUT') NOO
               ON UPPER (base.retail_environment) = UPPER (noo.RE)
              AND base.store_grade = noo.store_grade
              AND base.cntry_cd = noo.CNTRY_CD
              AND TRIM (base.sku_unique_identifier) = TRIM (noo.EAN)
        LEFT JOIN (SELECT DISTINCT EAN,
                          sku_code,
                          COUNTRY_CODE
                   FROM wks_anz_sellout_c360_mapped_sku_cd) sku
               ON UPPER (LTRIM (base.sku_unique_identifier,0)) = UPPER (LTRIM (sku.EAN,0))
              AND base.cntry_cd = sku.COUNTRY_CODE
        LEFT JOIN (SELECT EAN_NUM,
                          MAPPED_SKU_CD,
                          ctry_key
                   FROM (SELECT DISTINCT EAN_NUM,
                                LTRIM(MATL_NUM,'0') AS MAPPED_SKU_CD,
                                ctry_key,
                                ROW_NUMBER() OVER (PARTITION BY EAN_NUM,ctry_key ORDER BY CRT_DTTM DESC) AS RN
                         FROM edw_material_sales_dim A
                           JOIN (SELECT DISTINCT sls_org,
                                        ctry_key
                                 FROM edw_sales_org_dim
                                 WHERE ctry_key IN ('AU','NZ')) B ON A.sls_org = B.sls_org)
                   WHERE RN = 1) MAT
               ON UPPER (LTRIM (base.sku_unique_identifier,0)) = UPPER (LTRIM (MAT.EAN_NUM,0))
              AND base.cntry_cd = MAT.ctry_key
        LEFT JOIN (SELECT DISTINCT LTRIM(product_client_code,0) AS ean_number,
                          UPPER(category_name) AS prod_hier_l3,
                          UPPER(subcategory_local_name) AS prod_hier_l4,
                          UPPER(brand_name) AS prod_hier_l5,
                          LTRIM(product_client_code,0) || ' - ' || product_local_name AS prod_hier_l9,
                          ROW_NUMBER() OVER (PARTITION BY LTRIM(product_client_code,0) ORDER BY (LTRIM(product_client_code,0) || ' - ' || product_local_name) DESC) AS row_no
                   FROM itg_trax_md_product
                   WHERE itg_trax_md_product.businessunitid::TEXT = 'PC'::TEXT
                   AND   itg_trax_md_product.manufacturer_name::TEXT = 'JOHNSON & JOHNSON'::TEXT) epd
               ON UPPER (TRIM (base.sku_unique_identifier)) = UPPER (TRIM (epd.ean_number))
              AND row_no = 1)

),
final as
(
    select 
jj_year::VARCHAR(16) AS jj_year,
jj_mnth_id::VARCHAR(22) AS jj_mnth_id,
soldto_code::VARCHAR(255) AS soldto_code,
distributor_code::VARCHAR(32) AS distributor_code,
distributor_name::VARCHAR(255) AS distributor_name,
store_code::VARCHAR(100) AS store_code,
store_name::VARCHAR(601) AS store_name,
store_type::VARCHAR(255) AS store_type,
ean::VARCHAR(100) AS ean,
store_grade::VARCHAR(20) AS store_grade,
sell_out_channel::VARCHAR(150) AS sell_out_channel,
retail_environment::VARCHAR(75) AS retail_environment,
market::VARCHAR(50) AS market,
cntry_cd::VARCHAR(2) AS cntry_cd,
prod_hier_l3::VARCHAR(384) AS prod_hier_l3,
prod_hier_l4::VARCHAR(384) AS prod_hier_l4,
prod_hier_l5::VARCHAR(384) AS prod_hier_l5,
prod_hier_l9::VARCHAR(2307) AS prod_hier_l9,
sku_code::VARCHAR(40) AS sku_code,
list_price::NUMERIC(38,6) AS list_price,
msl_product_desc::VARCHAR(300) AS msl_product_desc,
region::VARCHAR(150) AS region,
zone_or_area::VARCHAR(150) AS zone_or_area,
mapped_sku_cd::VARCHAR(40) AS mapped_sku_cd,
crt_dttm::timestamp without time zone AS crt_dttm
from anz_sellout_itg_re_msl_list
)
--final select
select * from final

