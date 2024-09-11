--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
cnpc_regional_sellout_mapped_sku_cd 
as 
(
select ean :: varchar(50) as ean,
       sku_code :: varchar(40) as sku_code
       --msl_product_desc :: varchar(300) as msl_product_desc
from (select distinct ean,
             sku_code,
		--	 msl_product_desc,
             row_number() over (partition by ean order by crtd_dttm desc,mnth_id desc) as rno
      from edw_rpt_regional_sellout_offtake
      where country_name = 'China Personal Care'
      and   data_source in ('SELL-OUT','POS')
      and   sku_code is not null and ean != 'NA')
where rno = 1)

select * from cnpc_regional_sellout_mapped_sku_cd 