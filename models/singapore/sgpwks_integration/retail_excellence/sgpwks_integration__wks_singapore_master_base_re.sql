
--overwriding default sql header as we dont want to change timezone to singapore
{{
    config(
        sql_header= ""
    )
}}
--import cte
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration','edw_rpt_regional_sellout_offtake') }}
),
regional_sellout_mapped_sku_cd as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_mapped_sku_cd') }}
),
--final cte
singapore_master_base_re  as (
select country_code as cntry_cd,
       ltrim(soldto_code,'0') as soldto_code,
       distributor_code,
       distributor_name|| '#' ||ltrim(distributor_code,'0') as distributor_name,
       store_code,
       store_name|| '#' ||ltrim(store_code,'0') as store_name,
       store_type,
       retail_environment,
       region,
       zone_or_area,
       msl_product_code as master_code,
       mscd.sku_description as customer_product_desc,		--//        mscd.sku_description as customer_product_desc,
       mscd.mapped_sku_cd as mapped_sku_cd,		--//        mscd.mapped_sku_cd as mapped_sku_cd,
sysdate()		--// sysdate
from (select country_code,
             max(soldto_code) over (partition by ltrim(distributor_code,'0') order by cal_date desc rows between unbounded preceding and unbounded following) as soldto_code,
             ltrim(distributor_code,'0') as distributor_code,
             max(distributor_name) over (partition by ltrim(distributor_code,'0') order by cal_date desc rows between unbounded preceding and unbounded following) as distributor_name,
             ltrim(store_code,'0') as store_code,
             max(store_name) over (partition by ltrim(store_code,'0') order by cal_date desc rows between unbounded preceding and unbounded following) as store_name,
             store_type,
             retail_env as retail_environment,
             region,
             zone_or_area,
             ltrim(msl_product_code,'0') as msl_product_code
      from edw_rpt_regional_sellout_offtake		
      where country_code = 'SG'
      and   data_source = 'POS'
      and   ((upper(distributor_name) != 'N/A' and upper(distributor_name) != 'NA' and nullif(distributor_name,'') is not null) and (upper(store_name) != 'N/A' and upper(store_name) != 'NA' and nullif(store_name,'') is not null))) master_base
  left join regional_sellout_mapped_sku_cd mscd on ltrim (master_base.msl_product_code,'0') = ltrim (mscd.master_code,'0')
)

--final select
select * from singapore_master_base_re


