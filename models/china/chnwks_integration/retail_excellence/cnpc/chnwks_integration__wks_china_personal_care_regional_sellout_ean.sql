--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
cnpc_regional_sellout_ean as
(
    select mother_code ,
       ean
from (select distinct ltrim(msl_product_code,'0') as mother_code,
             ean,
             row_number() over (partition by ltrim(msl_product_code,'0') order by mnth_id desc) as rno
      from edw_rpt_regional_sellout_offtake
      where country_name = 'China Personal Care'
      and   data_source in ('SELL-OUT','POS')
	  and ean is not null)
where rno = 1
)
select mother_code :: varchar(150) as mother_code,
       ean :: varchar(50) as ean
        from cnpc_regional_sellout_ean