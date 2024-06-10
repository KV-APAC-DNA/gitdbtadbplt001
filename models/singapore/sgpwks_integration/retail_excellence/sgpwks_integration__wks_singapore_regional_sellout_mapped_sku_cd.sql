--overwriding default sql header as we dont want to change timezone to singapore
{{
    config(
        sql_header= ""
    )
}}

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
)


--final select
select * from singapore_regional_sellout_mapped_sku_cd