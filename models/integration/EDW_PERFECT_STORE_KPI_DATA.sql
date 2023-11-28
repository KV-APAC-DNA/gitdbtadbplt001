--Import CTE

with WKS_EDW_PERFECT_STORE_HASH 
  as ( select * from {{ ref('stg_arsadpprd001_raw__rg_wks_wks_edw_perfect_store_hash') }})
  ,
wks_edw_perfect_store_kpi_rebased_wt_mnth_cname
as ( select * from  {{ ref('wks_edw_perfect_store_kpi_rebased_wt_mnth_cname') }})
,
--Logical CTE 

MSL_OOS as (
select per_str.*
       ,reb_wt.total_weight
       ,calc_weight
       ,weight_msl
       ,weight_oos
       ,weight_soa
       ,weight_sos
       ,weight_promo
       ,weight_planogram
       ,weight_display 
 from ( select * 
          from WKS_EDW_PERFECT_STORE_HASH
          where  kpi in ('MSL COMPLIANCE','OOS COMPLIANCE')
            --and nvl(kpi_chnl_wt,0) > 0 
            and country  in ('Hong Kong','Korea','Taiwan')
        ) per_str
      ,wks_edw_perfect_store_kpi_rebased_wt_mnth_cname  reb_wt
where per_str.country       = reb_wt.country      
  and per_str.customername    = reb_wt.customername   
  and to_char(per_str.scheduleddate,'YYYYMM') = reb_wt.scheduledmonth 
  and per_str.kpi           = reb_wt.kpi 
)  
,

--Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
PROMO_PLANO as
(select per_str.*
       ,reb_wt.total_weight
       ,calc_weight
       ,weight_msl
       ,weight_oos
       ,weight_soa
       ,weight_sos
       ,weight_promo
       ,weight_planogram
       ,weight_display 
 from  (select * 
          from WKS_EDW_PERFECT_STORE_HASH
         where kpi in ('PROMO COMPLIANCE','PLANOGRAM COMPLIANCE','DISPLAY COMPLIANCE') 
           and REF_VALUE = 1 
           --and nvl(kpi_chnl_wt,0) > 0  
           and country in ('Hong Kong','Korea','Taiwan')
        )per_str
      ,wks_edw_perfect_store_kpi_rebased_wt_mnth_cname  reb_wt
where per_str.country       = reb_wt.country     
  and per_str.customername  = reb_wt.customername  
  and to_char(per_str.scheduleddate,'YYYYMM') = reb_wt.scheduledmonth 
  and per_str.kpi           = reb_wt.kpi        

 ),

   
--Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
DISPLAY_REM
as 
(
    select per_st.*
    from WKS_EDW_PERFECT_STORE_HASH per_st
            where  kpi in ('PROMO COMPLIANCE','PLANOGRAM COMPLIANCE','DISPLAY COMPLIANCE') 
                and country in ('Hong Kong','Korea','Taiwan')
    minus
    select per_str.*
    from WKS_EDW_PERFECT_STORE_HASH per_str
            where  kpi in ('PROMO COMPLIANCE','PLANOGRAM COMPLIANCE','DISPLAY COMPLIANCE') 
            and REF_VALUE = 1 
            --and nvl(kpi_chnl_wt,0) > 0  
                and country in ('Hong Kong','Korea','Taiwan')
) ,

DISPLAY 
as 
( select *
       ,null as total_weight
       ,null as calc_weight
       ,null as weight_msl
       ,null as weight_oos
       ,null as weight_soa
       ,null as weight_sos
       ,null as weight_promo
       ,null as weight_planogram
       ,null as weight_display
    from DISPLAY_REM
),

--Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA
SOA 
as 
(
select per_str.*
       ,reb_wt.total_weight
       ,calc_weight
       ,weight_msl
       ,weight_oos
       ,weight_soa
       ,weight_sos
       ,weight_promo
       ,weight_planogram
       ,weight_display 
 from  wks_perfect_store_sos_soa_mnth per_str
      ,wks_edw_perfect_store_kpi_rebased_wt_mnth_cname  reb_wt
where per_str.country       = reb_wt.country 
  and per_str.customername  = reb_wt.customername 
  and to_char(per_str.scheduleddate,'YYYYMM') = reb_wt.scheduledmonth 
  and per_str.kpi           = reb_wt.kpi

 ),

--Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
SOS_SOA_REM as 
(
select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
  from WKS_EDW_PERFECT_STORE_HASH 
 where kpi in ('SOS COMPLIANCE','SOA COMPLIANCE') 
 and country  in ('Hong Kong','Korea','Taiwan')
minus
select * 
  from wks_perfect_store_sos_soa_mnth
) 
,
SOS_SOA as
( 
    select * 
        ,null as total_weight
       ,null as calc_weight
       ,null as weight_msl
       ,null as weight_oos
       ,null as weight_soa
       ,null as weight_sos
       ,null as weight_promo
       ,null as weight_planogram
       ,null as weight_display
      from SOS_SOA_REM
)
,

--Final CTE
Final as (
select  *  from  MSL_OOS
union all
select  *  from  PROMO_PLANO
union all
select  *  from  DISPLAY
union all
select *   from  SOA
union all
select * from   SOS_SOA
)

select * from Final
