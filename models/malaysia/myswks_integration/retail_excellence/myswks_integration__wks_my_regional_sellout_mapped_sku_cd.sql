--import cte
with wks_my_base_re_raw as (
    select * from {{ ref('myswks_integration__wks_my_base_re_raw') }}
),

MY_REGIONAL_SELLOUT_MAPPED_SKU_CD  as (
SELECT ean,
       sku_code,
       msl_product_desc
FROM (SELECT DISTINCT ean,
             sku_code,
             msl_product_desc,
             ROW_NUMBER() OVER (PARTITION BY ean ORDER BY cal_date DESC,mnth_id DESC) AS rno
      FROM wks_my_base_re_raw		
      WHERE COUNTRY_NAME = 'Malaysia'
      AND   DATA_SOURCE IN ('SELL-OUT','POS')
      AND   sku_code IS NOT NULL)
WHERE rno = 1
),

--final select
final as (
select 
ean::varchar(500) as ean,
sku_code::varchar(40) as sku_code,	
msl_product_desc::varchar(300) as msl_product_desc 
 from MY_REGIONAL_SELLOUT_MAPPED_SKU_CD
)
select * from final