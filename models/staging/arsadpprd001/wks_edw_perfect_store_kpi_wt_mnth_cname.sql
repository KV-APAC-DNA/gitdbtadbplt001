create table rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname
as
select country, nvl(customername,'x') as customername, to_char(scheduleddate,'YYYYMM') scheduledmonth, kpi, min(cast(kpi_chnl_wt as numeric(8,4)) ) chnl_wt 
  from {{ ref('stg_arsadpprd001__WKS_EDW_PERFECT_STORE_HASH') }}
 where 
--        nvl(kpi_chnl_wt,0) > 0 
--   and  
      kpi in ('MSL COMPLIANCE','OOS COMPLIANCE')
   and country in ('Hong Kong','Korea','Taiwan')
group by country, customername, to_char(scheduleddate,'YYYYMM'), kpi;