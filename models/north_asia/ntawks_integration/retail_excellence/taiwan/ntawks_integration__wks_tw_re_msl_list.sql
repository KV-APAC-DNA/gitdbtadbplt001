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
    select * from {{ source('ntaedw_integration', 'edw_vw_pop6_products') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
--Final CTE
TW_re_msl_list	  as (
select distinct jj_year,jj_mnth_id,soldto_code,distributor_code,distributor_name,store_code,store_name,store_type,ean,store_grade,Sell_Out_Channel ,retail_environment ,market, cntry_cd ,prod_hier_l1,prod_hier_l2,prod_hier_l3,prod_hier_l4,prod_hier_l5,prod_hier_l6,prod_hier_l7,prod_hier_l8,prod_hier_l9 
,sku_code,max(msl_product_desc) over ( PARTITION BY ltrim(ean,0) ORDER BY length(msl_product_desc) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS msl_product_desc ,region,zone_or_area from (
SELECT distinct SUBSTRING(base.jj_mnth_id,1,4) as jj_year,base.jj_mnth_id,noo.soldto_code,distributor_code,noo.distributor_name,noo.distributor_name_new,noo.store_code,noo.store_name,noo.store_type,ltrim(base.sku_unique_identifier,0) as ean,base.store_grade,noo.channel as Sell_Out_Channel ,upper(base.retail_environment) as retail_environment ,base.market, base.cntry_cd ,epd.prod_hier_l1,epd.prod_hier_l2,epd.prod_hier_l3,epd.prod_hier_l4,epd.prod_hier_l5,prod_hier_l6,prod_hier_l7,prod_hier_l8,epd.prod_hier_l9 
,noo.sku_code,noo.msl_product_desc ,noo.region,noo.zone_or_area,base.customer_name 
FROM (SELECT DISTINCT *,case when msl.market='Taiwan' then 'TW' end cntry_cd ,
case when customer like '%Carrefour%' then 'Carrefour'
										when customer like '%PX%' then 'PX'
										when customer like '%Watson%' then 'Watsons' else customer end as customer_name
                  FROM Itg_re_msl_input_definition msl
              LEFT JOIN (SELECT DISTINCT SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
                         FROM EDW_CALENDAR_DIM
						 where jj_mnth_id >= (select last_17mnths from edw_vw_cal_Retail_excellence_Dim)
						   and jj_mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)
                        --and jj_mnth_id='202308'  -- for one month
						 ) cal
                     ON TO_CHAR (TO_DATE (msl.start_date,'DD/MM/YYYY'),'YYYYMM') <= cal.jj_mnth_id
                    AND TO_CHAR (TO_DATE (msl.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= cal.jj_mnth_id
			WHERE msl.market in ('Taiwan')	 and msl.retail_environment='MT' 	
) base					
left join
 
(select distinct  distributor_code,distributor_name,case when distributor_name like '%Carrefour%' then 'Carrefour'
										when distributor_name like '%PX%' then 'PX'
										when distributor_name like '%Watsons%' then 'Watsons' else distributor_name end as distributor_name_new , store_code, store_type,
region,zone_or_area,RETAIL_ENVIRONMENT as RE,store_grade,CHANNEL_NAME as channel,CNTRY_CD,
soldto_code,store_name,msl_product_desc,sku_code,EAN 
	  FROM  WKS_TW_BASE_RE  WHERE CNTRY_CD in  ('TW') and data_src='POS' and RETAIL_ENVIRONMENT='MT' ) NOO
on upper(base.channel)= upper(noo.channel) and base.customer_name=noo.distributor_name_new 
and  base.cntry_cd = noo.CNTRY_CD and trim(base.sku_unique_identifier)=trim(noo.EAN)
 
left join (
select distinct prd.country_l1 AS prod_hier_l1, prd.regional_franchise_l2 AS prod_hier_l2, prd.franchise_l3 AS prod_hier_l3, prd.brand_l4 AS prod_hier_l4, 
prd.sub_category_l5 AS prod_hier_l5, prd.platform_l6 AS prod_hier_l6, prd.variance_l7 AS prod_hier_l7, prd.pack_size_l8 AS prod_hier_l8, NULL AS prod_hier_l9,prd.barcode 
from edw_vw_pop6_products prd where cntry_cd='TW')epd
on UPPER (TRIM (base.sku_unique_identifier)) = UPPER (TRIM (epd.barcode)) -- and row_no=1 
)
),
--final select
final as (
    select
jj_year::varchar(16) as jj_year ,
jj_mnth_id::varchar(22) as jj_mnth_id ,
soldto_code::varchar(255) as soldto_code ,
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