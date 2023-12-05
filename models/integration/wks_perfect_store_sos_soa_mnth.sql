{{
    config(
        materialized='table'
    )
}}

--Import CTE
with rg_wks_wks_edw_perfect_store_hash 
as ( select * from {{ ref('stg_arsadpprd001_raw__rg_wks_wks_edw_perfect_store_hash') }}
),

--Logical CTE

--Final CTE
final as 
(
select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
  from rg_wks_wks_edw_perfect_store_hash
 where  kpi in ('SOS COMPLIANCE','SOA COMPLIANCE') 
   and  mkt_share  is NOT NULL 
   and  QUES_TYPE in ('DENOMINATOR','NUMERATOR')
   and  REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
--   and country = 'Taiwan'
   and (country, customername, scheduleddate, PROD_HIER_L4, PROD_HIER_L5)
   in 
   (
    select country, customername, scheduleddate, PROD_HIER_L4, PROD_HIER_L5 
      from rg_wks_wks_edw_perfect_store_hash
     where kpi in ('SOS COMPLIANCE','SOA COMPLIANCE') 
      and  PROD_HIER_L4 is NOT NULL 
      and  PROD_HIER_L5 is NOT NULL
      and  mkt_share  is NOT NULL 
      and  REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
      and  QUES_TYPE in ('DENOMINATOR','NUMERATOR')
--        and country = 'Taiwan'
     and country  in ('Hong Kong','Korea','Taiwan')
     group by country, customername, scheduleddate, PROD_HIER_L4, PROD_HIER_L5
     having count ( distinct QUES_TYPE) = 2
   )
)

select * from final