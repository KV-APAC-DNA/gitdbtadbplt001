{{
    config(
        materialized='table'
    )
}}

--Import CTE 
with rg_wks_wks_edw_perfect_store_hash as
( select * 
    from  {{ ref('stg_arsadpprd001_raw__rg_wks_wks_edw_perfect_store_hash') }}
),

wks_perfect_store_sos_soa_mnth as
(  select *
      from {{ ref('wks_perfect_store_sos_soa_mnth') }}
),
--Logical CTEb

--Final CTE 
final as 
(
select country, nvl(customername,'x') as customername, to_char(scheduleddate,'YYYYMM') scheduledmonth, kpi, min(cast(kpi_chnl_wt as numeric(8,4)) ) chnl_wt 
  from rg_wks_wks_edw_perfect_store_hash
 where 
--        nvl(kpi_chnl_wt,0) > 0 
--   and  
      kpi in ('MSL COMPLIANCE','OOS COMPLIANCE')
   and country in ('Hong Kong','Korea','Taiwan')
group by country, customername, to_char(scheduleddate,'YYYYMM'), kpi

union all

select country, nvl(customername,'x') as customername, to_char(scheduleddate,'YYYYMM') scheduledmonth, kpi, min(cast(kpi_chnl_wt as numeric(8,4))) chnl_wt 
  from rg_wks_wks_edw_perfect_store_hash
 where 
--        nvl(kpi_chnl_wt,0) > 0  
--   and  
  kpi in ('PROMO COMPLIANCE','PLANOGRAM COMPLIANCE','DISPLAY COMPLIANCE')
  and  REF_VALUE = 1 
  and country in ('Hong Kong','Korea','Taiwan')
group by country, customername, to_char(scheduleddate,'YYYYMM') , kpi

union all

SELECT country, nvl(customername,'x') as customername, to_char(scheduleddate,'YYYYMM') scheduledmonth, kpi, min(cast(kpi_chnl_wt as numeric(8,4))) chnl_wt 
  FROM wks_perfect_store_sos_soa_mnth
group by country, customername, to_char(scheduleddate,'YYYYMM') , kpi

)

select * 
 from final