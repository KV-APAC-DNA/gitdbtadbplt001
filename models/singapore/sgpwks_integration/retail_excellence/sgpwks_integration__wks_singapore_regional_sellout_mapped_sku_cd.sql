--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),

--final cte
singapore_regional_sellout_mapped_sku_cd  as (
select *
from (select distinct ltrim(msl_product_code,'0') as master_code,
             ltrim(sku_code,'0') as mapped_sku_cd,
             sku_description,
             row_number() over (partition by ltrim(msl_product_code,0) order by cal_date desc,length(ltrim(sku_code,'0')) desc) as rno
      from edw_rpt_regional_sellout_offtake
      where country_code = 'SG'
      and   data_source = 'POS')
where rno = 1
),
final as(

    select 
    master_code::varchar(150) AS master_code,
mapped_sku_cd::varchar(40) AS mapped_sku_cd,
sku_description::varchar(150) AS sku_description,
rno::numeric(38,0) AS rno
from singapore_regional_sellout_mapped_sku_cd
)

--final select
select * from final