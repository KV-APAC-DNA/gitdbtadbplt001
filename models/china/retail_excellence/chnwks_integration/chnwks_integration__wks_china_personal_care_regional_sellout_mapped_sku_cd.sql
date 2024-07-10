--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
cnpc_regional_sellout_mapped_sku_cd 
as 
(
select ean::varchar as ean,
       sku_code::varchar as sku_code,
       msl_product_desc::varchar as msl_product_desc
from (select distinct ltrim(msl_product_code,'0') as ean,
             sku_code,
             msl_product_desc,
             row_number() over (partition by ltrim(msl_product_code,'0') order by crtd_dttm desc,mnth_id desc) as rno
      from edw_rpt_regional_sellout_offtake
      where country_name = 'china personal care'
      and   data_source in ('sell-out','pos')
      and   sku_code is not null)
where rno = 1)

select * from cnpc_regional_sellout_mapped_sku_cd 