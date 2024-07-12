--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
cnpc_regional_sellout_ean as
(
    select mother_code :: varchar(150),
       ean :: varchar(50)
from (select distinct ltrim(msl_product_code,'0') as mother_code,
             ean,
             row_number() over (partition by ltrim(msl_product_code,'0') order by mnth_id desc) as rno
      from edw_rpt_regional_sellout_offtake
      where country_name = 'china personal care'
      and   data_source in ('sell-out','pos')
	  and ean is not null)
where rno = 1
)
select * from cnpc_regional_sellout_ean