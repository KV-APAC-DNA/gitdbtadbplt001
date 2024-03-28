with itg_vn_dms_sales_stock_fact as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_VN_DMS_SALES_STOCK_FACT
),
itg_vn_dms_distributor_dim_rnk as (
select *,row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rnk  from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_VN_DMS_DISTRIBUTOR_DIM  
),
itg_vn_dms_distributor_dim as (
select * from itg_vn_dms_distributor_dim_rnk where rnk=1
),
edw_vw_os_time_dim as (
select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.SGPEDW_INTEGRATION__EDW_VW_OS_TIME_DIM
),
itg_vn_dms_customer_dim as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_VN_DMS_CUSTOMER_DIM
),
itg_vn_dms_yearly_target as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_VN_DMS_YEARLY_TARGET
),
wks_vn_si_st_so_details_forecast as (
select * from DEV_DNA_CORE.SNAPOSEWKS_INTEGRATION.WKS_VN_SI_ST_SO_DETAILS_FORECAST
),
itg_vn_dms_sales_org_dim as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_VN_DMS_SALES_ORG_DIM
),
edw_crncy_exch_rates as (
select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_CRNCY_EXCH_RATES
),
inv_dt as
(
  select stk1.dstrbtr_id,
         stk1.wh_code,
         stk1.DATE,
         mnth_id,
         max(stk1.date) over (partition by mnth_id,stk1.dstrbtr_id,stk1.wh_code) as dstrb_max_inv_date,
         row_number() over (partition by mnth_id,stk1.dstrbtr_id,stk1.wh_code ORDER by stk1.date DESC) date_rn
  from (select distinct dstrbtr_id,
               wh_code,
               date
        from itg_vn_dms_sales_stock_fact) stk1,
       edw_vw_os_time_dim timedim1
  where stk1.date = timedim1.cal_date
),
transformed as (
select * , case when current_timestamp() between mnth_start_date and mnth_end_date then 'Y' else 'N' end as curr_mnth_indc,
case when jj_year =extract(year from current_timestamp()) then 'Y' else 'N' end curr_year_indc
  from
(select gt_sc.*,jj_cal.mnth_start_date  , jj_cal.mnth_end_date, exch_rate.ex_rt/1000 as exchg_rate from 
(select jj_year,
       jj_qrtr,
       jj_mnth_id,
       null as territory_dist,
       dist_type,
       null as asm_id,
       sum(sell_out) as so_value,
       null as so_yearly_target,
	   null as ub_brand_lvl_yearly_target,
       null as so_target,
       null as inv_value,
	   null as inv_value_prev,
       null as brand,
       null as ub,
	   null as ub_yearly_target,
       null as pc,
	   null as pc_yearly_target,
       null as total_stores,
	   null as total_stores_yearly_target,
       null as sr_active,
	   null as sr_active_yearly_target,
       null as ss_active,
	   null as ss_active_yearly_target,
       null as sr_inactive,
       null as ss_inactive,
	   null as sr_resigned_yearly_target,
	   null as UB_Coverage_percent_yearly_target,
	   null as PC_SR_yearly_target,
	   null as UB_SR_yearly_target,
	   null as revenue_SR_M_yearly_target,
	   null as SR_attrition_percent_yearly_target
from (select jj_year,
             jj_qrtr,
             jj_mnth_id,
             CASE
               WHEN dstrbtr_type IN ('SP','SPK') THEN 'Direct Branch'
               ELSE dstrbtr_type
             END dist_type,
             sum(so_net_trd_sls) as sell_out
      from wks_vn_si_st_so_details_forecast
      where dstrbtr_type IS NOT null
      group by jj_year,
               jj_qrtr,
               jj_mnth_id,
               dstrbtr_type)
group by jj_year,
         jj_qrtr,
         jj_mnth_id,
         dist_type
union all ---- getting the monthly so_value and target
select jj_year, 
       jj_qrtr, 
       jj_mnth_id, 
       null as territory_dist,
       'GT' as dist_type,
       null as asm_id,
       sum(actual) as so_value,
       dt_tgt.target*1000000 as so_yearly_target,
	   null as ub_brand_lvl_yearly_target,
       sum(mnth_tgt) as so_target,
       null as inv_value,
	   null as inv_value_prev,
       null as brand,
       null as ub,
	   null as ub_yearly_target,
	   null as pc,
	   null as pc_yearly_target,
	   null as total_stores,
	   null as total_stores_yearly_target,
	   null as sr_active,
	   null as sr_active_yearly_target,
	   null as ss_active,
	   null as ss_active_yearly_target,
	   null as sr_inactive,
	   null as ss_inactive,
	   null as sr_resigned_yearly_target,
	   null as UB_Coverage_percent_yearly_target,
	   null as PC_SR_yearly_target,
	   null as UB_SR_yearly_target,
	   null as revenue_SR_M_yearly_target,
	   null as SR_attrition_percent_yearly_target
from
(select jj_year, jj_qrtr , jj_mnth_id, territory_dist,distributor_id_report,dstrbtr_grp_cd,slsmn_cd, sum(so_net_trd_sls) as actual,
max(so_mnth_tgt) as mnth_tgt from wks_vn_si_st_so_details_forecast 
group by jj_year, jj_mnth_id,  jj_qrtr ,territory_dist,distributor_id_report,dstrbtr_grp_cd,slsmn_cd) dt_actual
left join (select year, category, target from itg_vn_dms_yearly_target where kpi = 'Sell out' ) dt_tgt
on dt_tgt.year = dt_actual.jj_year
and dt_tgt.category = 'GT'
--where upper(dt_tgt.category) = upper(dist_type)
group by jj_year,jj_mnth_id,jj_qrtr,so_yearly_target
union all ------ getting the monthly sellout actuals for zone manager
select zm_actual.jj_year,
        zm_actual.jj_qrtr , 
       zm_actual.jj_mnth_id, 
       zm_actual.territory_dist,
       null as dist_type,
       zm_actual.asm_name, 
       zm_actual.so_value so_value,
       zm_tgt.target*1000000 as so_yearly_target,
	   null as ub_brand_lvl_yearly_target,
       zm_actual.so_tgt as so_target,
       null as inv_value,
	   null as inv_value_prev,
       null as brand,
	   null as ub,
	   null as ub_yearly_target,
	   null as pc,
	   null as pc_yearly_target,
	   null as total_stores,
	   null as total_stores_yearly_target,
	   null as sr_active,
	   null as sr_active_yearly_target,
	   null as ss_active,
	   null as ss_active_yearly_target,
	   null as sr_inactive,
	   null as ss_inactive,
	   null as sr_resigned_yearly_target,
	   null as UB_Coverage_percent_yearly_target,
	   null as PC_SR_yearly_target,
	   null as UB_SR_yearly_target,
	   null as revenue_SR_M_yearly_target,
	   null as SR_attrition_percent_yearly_target
from
   ( select jj_year, jj_qrtr,jj_mnth_id, territory_dist, asm_name, sum(so_value) as so_value , sum(so_tgt) as so_tgt from (select jj_year,
       jj_qrtr , 
       jj_mnth_id,
       territory_dist,
       dstrbtr_grp_cd,
       slsmn_cd,
       asm_name, 
       sum(so_net_trd_sls) as so_value,
      max(so_mnth_tgt) as so_tgt 
from wks_vn_si_st_so_details_forecast
where asm_name is not null
group by jj_year, jj_mnth_id,  jj_qrtr ,territory_dist,dstrbtr_grp_cd,slsmn_cd,asm_name) group by jj_year, jj_qrtr,jj_mnth_id, territory_dist, asm_name)zm_actual
left join (select year, category, target from itg_vn_dms_yearly_target where kpi = 'Sell out' )zm_tgt
on zm_tgt.year = zm_actual.jj_year
and upper(zm_tgt.category) = upper(zm_actual.asm_name)
union all --- getting actuals and targets by TD level
select jj_year, 
       jj_qrtr, 
       jj_mnth_id, 
       territory_dist,
       'TD' as dist_type,
       null as asm_id,
       sum(actual) as so_value,
	   td_tgt.target*1000000 as so_yearly_target,
	   null as ub_brand_lvl_yearly_target,
       sum(mnth_tgt) as so_target,
       null as inv_value,
	   null as inv_value_prev,
       null as brand,
       null as ub,
	   null as ub_yearly_target,
	   null as pc,
	   null as pc_yearly_target,
	   null as total_stores,
	   null as total_stores_yearly_target,
	   null as sr_active,
	   null as sr_active_yearly_target,
	   null as ss_active,
	   null as ss_active_yearly_target,
	   null as sr_inactive,
	   null as ss_inactive,
	   null as sr_resigned_yearly_target,
	   null as UB_Coverage_percent_yearly_target,
	   null as PC_SR_yearly_target,
	   null as UB_SR_yearly_target,
	   null as revenue_SR_M_yearly_target,
	   null as SR_attrition_percent_yearly_target
from
(select jj_year, jj_qrtr , jj_mnth_id, territory_dist,distributor_id_report,dstrbtr_grp_cd,slsmn_cd, sum(so_net_trd_sls) as actual,
max(so_mnth_tgt) as mnth_tgt from wks_vn_si_st_so_details_forecast
group by jj_year, jj_mnth_id,  jj_qrtr ,territory_dist,distributor_id_report,dstrbtr_grp_cd,slsmn_cd) td_actual
left join (select year, category, target from itg_vn_dms_yearly_target where kpi = 'Sell out' )td_tgt
on td_tgt.year = td_actual.jj_year
and upper(td_tgt.category) = upper(territory_dist)
group by jj_year,jj_mnth_id,jj_qrtr,territory_dist,td_tgt.target
union all ----- getting inventory at TD and Sub -D level
(select timedim."year" as jj_year,
       timedim.qrtr as jj_qrtr,
       timedim.mnth_id as jj_mnth_id,
       null as territory_dist,
       inv_dstrbtr_type as dist_type,
       null as asm_id,
       null as so_value,
       null as so_yearly_target,
       null as ub_brand_lvl_yearly_target,
       null as so_target,
       sum(dt1_amount) as inv_value,
	     sum(inv_2) as inv_value_prev,
       null as brand,
       null as ub,
       null as ub_yearly_target,
       null as pc,
       null as pc_yearly_target,
       null as total_stores,
       null as total_stores_yearly_target,
       null as sr_active,
       null as sr_active_yearly_target,
       null as ss_active,
       null as ss_active_yearly_target,
       null as sr_inactive,
       null as ss_inactive,
       null as sr_resigned_yearly_target,
       null as UB_Coverage_percent_yearly_target,
	   null as PC_SR_yearly_target,
	   null as UB_SR_yearly_target,
	   null as revenue_SR_M_yearly_target,
	   null as SR_attrition_percent_yearly_target
from (select stk.dstrbtr_id,
             stk.wh_code,
             dstrb.dstrbtr_type,
             CASE
               WHEN dstrbtr_type = 'SPK' THEN 'TD'
               WHEN dstrbtr_type = 'SP' THEN 'SubD'
               ELSE 'SubD'
             END inv_dstrbtr_type,
             stk.date,
             stk.amount dt1_amount,
             dt1.mnth_id mnth_id,
             dt1.date_rn date_rn,
             null as inv_2
      from itg_vn_dms_sales_stock_fact stk,
           itg_vn_dms_distributor_dim dstrb,
           inv_dt dt1
      where stk.dstrbtr_id = dt1.dstrbtr_id
      AND   stk.wh_code = dt1.wh_code
      AND   stk.date = dt1.date
      AND   stk.dstrbtr_id = dstrb.dstrbtr_id
      AND   dt1.date_rn = 1
      union all
      select stk.dstrbtr_id,
             stk.wh_code,
             dstrb.dstrbtr_type,
             CASE
               WHEN dstrbtr_type = 'SPK' THEN 'TD'
               WHEN dstrbtr_type = 'SP' THEN 'SubD'
               ELSE 'SubD'
             END inv_dstrbtr_type,
             stk.date,
             null as dt1_amount,
             dt1.mnth_id mnth_id,
             dt1.date_rn date_rn,
             stk.amount as inv_2
      from itg_vn_dms_sales_stock_fact stk,
           itg_vn_dms_distributor_dim dstrb,
           inv_dt dt1
      where stk.dstrbtr_id = dt1.dstrbtr_id
      AND   stk.wh_code = dt1.wh_code
      AND   stk.date = dt1.date
      AND   stk.dstrbtr_id = dstrb.dstrbtr_id
      AND   dt1.date_rn = 2) inv_td,
     (select distinct edw_vw_os_time_dim."year",
             edw_vw_os_time_dim.qrtr,
             edw_vw_os_time_dim.mnth_id,
             edw_vw_os_time_dim.mnth_no
      from edw_vw_os_time_dim) timedim
where inv_td.mnth_id = timedim.mnth_id
group by inv_dstrbtr_type,
         timedim."year",
         timedim.qrtr,
         timedim.mnth_id)
union all -- getting so value by brand
select jj_year, 
       jj_qrtr , 
       jj_mnth_id, 
       null as territory_dist,
       null as dist_type,
       null as asm_id, 
       sum(so_net_trd_sls) as so_value,
       brand_so_tgt.target*1000000 as so_yearly_target,
	   brand_ub_tgt.target as ub_brand_lvl_yearly_target,
       null as so_target,
       null as inv_value,
	   null as inv_value_prev,
       brand,
       count (distinct cust_cd ) as ub,
	   null as ub_yearly_target,
	   null as pc,
	   null as pc_yearly_target,
	   null as total_stores,
	   null as total_stores_yearly_target,
	   null as sr_active,
	   null as sr_active_yearly_target,
	   null as ss_active,
	   null as ss_active_yearly_target,
	   null as sr_inactive,
	   null as ss_inactive,
	   null as sr_resigned_yearly_target,
	   null as UB_Coverage_percent_yearly_target,
	   null as PC_SR_yearly_target,
	   null as UB_SR_yearly_target,
	   null as revenue_SR_M_yearly_target,
	   null as SR_attrition_percent_yearly_target
from wks_vn_si_st_so_details_forecast brand_actual
left join (select year, category, target from itg_vn_dms_yearly_target where kpi = 'Sell out' ) brand_so_tgt
on brand_so_tgt.year = brand_actual.jj_year
and upper(brand_so_tgt.category) = upper(brand_actual.brand)
left join (select year, category, target from itg_vn_dms_yearly_target where kpi = 'UB' ) brand_ub_tgt
on brand_ub_tgt.year = brand_actual.jj_year
and upper(brand_ub_tgt.category) = upper(brand_actual.brand)
where brand is not null
group by jj_year, jj_mnth_id,  jj_qrtr ,brand,brand_so_tgt.target,brand_ub_tgt.target
union all ----- getting ub, pc , vc and other values at monthly level
select store."year" as jj_year,
       store.qrtr as jj_qrtr,
       store.mnth_id as jj_mnth_id,  
       null as territory_dist,
       null as dist_type,
       null as asm_id,
       null as so_value,
       null as so_yearly_target,
	   null as ub_brand_lvl_yearly_target,
       null as so_target,
       null as inv_value,
	   null as inv_value_prev,
       null as brand,
       undup_buy.ub,
       null as ub_yearly_target,
	   prod_call.pc,
	   null as pc_yearly_target,
       store.total_stores,
	   null as total_stores_yearly_target,
       salesrep_Active.sr_active,
	   null as sr_active_yearly_target,
       supervisor_active.ss_active,
	   null as ss_active_yearly_target,
       salesrep_inactive.sr_inactive,
       supervisor_inactive.ss_inactive,
	   null as sr_resigned_yearly_target,
	   null as UB_Coverage_percent_yearly_target,
	   null as PC_SR_yearly_target,
	   null as UB_SR_yearly_target,
	   null as revenue_SR_M_yearly_target,
	   null as SR_attrition_percent_yearly_target
 from 
(select "year",qrtr,mnth_id, count( distinct outlet_id)as total_stores from 
(select "year",qrtr,mnth_id,mnth_no , outlet_id  from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr,
                      edw_vw_os_time_dim.mnth_id,
                      edw_vw_os_time_dim.mnth_no)timedim
            left join itg_vn_dms_customer_dim cust_cd
            on cust_cd.crt_date <= timedim.cal_date
            and (cust_cd.date_off > timedim.cal_date or cust_cd.date_off = '1900-01-01'))group by "year",qrtr,mnth_id)store
 left join  ---- for calculating ub
 (select jj_year,jj_qrtr,jj_mnth_id, count(distinct cust_cd) as ub from wks_vn_si_st_so_details_forecast group by jj_year, jj_qrtr,jj_mnth_id) undup_buy
 on undup_buy.jj_year = store."year"
 and undup_buy.jj_qrtr = store.qrtr
 and undup_buy.jj_mnth_id = store.mnth_id
 left join --- for calculating pc
 (select jj_year,jj_qrtr,jj_mnth_id, count(*) as pc from wks_vn_si_st_so_details_forecast where product_visit_call_date is not null group by jj_year, jj_qrtr,jj_mnth_id) prod_call
on prod_call.jj_year = store."year"
 and prod_call.jj_qrtr = store.qrtr
 and prod_call.jj_mnth_id = store.mnth_id
 left join --- for sr_active
 (select "year",qrtr,mnth_id, count( distinct salesrep_id)as sr_active from 
(select "year",qrtr,mnth_id,mnth_no , salesrep_id   from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr,
                      edw_vw_os_time_dim.mnth_id,
                      edw_vw_os_time_dim.mnth_no)timedim
            left join itg_vn_dms_sales_org_dim sls_org
            on sls_org.salesrep_crtdate <= timedim.cal_date
            and (sls_org.salesrep_dateoff > timedim.cal_date or sls_org.salesrep_dateoff = '1900-01-01'))group by "year",qrtr,mnth_id) salesrep_Active
on salesrep_Active."year" = store."year"
 and salesrep_Active.qrtr = store.qrtr
 and salesrep_Active.mnth_id = store.mnth_id
 left join ----- for ss_active
 (select "year",qrtr,mnth_id, count( distinct supervisor_code)as ss_active from 
(select "year",qrtr,mnth_id,mnth_no , supervisor_code   from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr,
                      edw_vw_os_time_dim.mnth_id,
                      edw_vw_os_time_dim.mnth_no)timedim
            left join (select distinct supervisor_code , sup_crtdate, sup_dateoff from itg_vn_dms_sales_org_dim where supervisor_code is not null ) sls_org
            on sls_org.sup_crtdate <= timedim.cal_date
            and (sls_org.sup_dateoff > timedim.cal_date or sls_org.sup_dateoff = '1900-01-01'))group by "year",qrtr,mnth_id) supervisor_active
on supervisor_active."year" = store."year"
 and supervisor_active.qrtr = store.qrtr
 and supervisor_active.mnth_id = store.mnth_id
 left join ----for sr_inactive
 (select "year",qrtr,mnth_id, count( distinct salesrep_id)as sr_inactive from 
(select "year",qrtr,mnth_id,mnth_no , salesrep_id   from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                    min(edw_vw_os_time_dim.cal_date) as start_date,
                   max(edw_vw_os_time_dim.cal_date) as end_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr,
                      edw_vw_os_time_dim.mnth_id,
                      edw_vw_os_time_dim.mnth_no)timedim
            left join itg_vn_dms_sales_org_dim  sls_org
            on sls_org.salesrep_dateoff between timedim.start_date and timedim.end_date)group by "year",qrtr,mnth_id) salesrep_inactive
on salesrep_inactive."year" = store."year"
 and salesrep_inactive.qrtr = store.qrtr
 and salesrep_inactive.mnth_id = store.mnth_id
 left join -------ss_inactive
 (select "year",qrtr,mnth_id, count( distinct supervisor_code)as ss_inactive from 
(select "year",qrtr,mnth_id,mnth_no , supervisor_code   from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                    min(edw_vw_os_time_dim.cal_date) as start_date,
                   max(edw_vw_os_time_dim.cal_date) as end_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr,
                      edw_vw_os_time_dim.mnth_id,
                      edw_vw_os_time_dim.mnth_no)timedim
            left join (select distinct supervisor_code , sup_dateoff from itg_vn_dms_sales_org_dim where supervisor_code is not null ) sls_org
            on sls_org.sup_dateoff between timedim.start_date and timedim.end_date)group by "year",qrtr,mnth_id) supervisor_inactive
on supervisor_inactive."year" = store."year"
 and supervisor_inactive.qrtr = store.qrtr
 and supervisor_inactive.mnth_id = store.mnth_id
 -------------------------------------- Quaterly level data-------------------------------
 union all 
 select store."year" as jj_year,
       store.qrtr as jj_qrtr,
       null as jj_mnth_id,  
       null as territory_dist,
       null as dist_type,
       null as asm_id,
       null as so_value,
       null as so_yearly_target,
	   null as ub_brand_lvl_yearly_target,
       null as so_target,
       null as inv_value,
	   null as inv_value_prev,
       null as brand,
       undup_buy.ub,
	   null as ub_yearly_target,
       prod_call.pc,
	   null as pc_yearly_target,
       store.total_stores,
	   null as total_stores_yearly_target,
       salesrep_Active.sr_active,
	   null as sr_active_yearly_target,
       supervisor_active.ss_active,
	   null as ss_active_yearly_target,
       salesrep_inactive.sr_inactive,
       supervisor_inactive.ss_inactive,
	   null as sr_resigned_yearly_target,
	   null as UB_Coverage_percent_yearly_target,
	   null as PC_SR_yearly_target,
	   null as UB_SR_yearly_target,
	   null as revenue_SR_M_yearly_target,
	   null as SR_attrition_percent_yearly_target
 from 
(select "year",qrtr, count( distinct outlet_id)as total_stores from 
(select "year",qrtr, outlet_id  from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr)timedim
            left join itg_vn_dms_customer_dim cust_cd
            on cust_cd.crt_date <= timedim.cal_date
            and (cust_cd.date_off > timedim.cal_date or cust_cd.date_off = '1900-01-01'))group by "year",qrtr)store
 left join  ---- for calculating ub
 (select jj_year,jj_qrtr, count(distinct cust_cd) as ub from wks_vn_si_st_so_details_forecast group by jj_year, jj_qrtr) undup_buy
 on undup_buy.jj_year = store."year"
 and undup_buy.jj_qrtr = store.qrtr
 left join --- for calculating pc
 (select jj_year,jj_qrtr, count(*) as pc from wks_vn_si_st_so_details_forecast where product_visit_call_date is not null group by jj_year, jj_qrtr) prod_call
on prod_call.jj_year = store."year"
 and prod_call.jj_qrtr = store.qrtr
 left join --- for sr_active
 (select "year",qrtr,count( distinct salesrep_id)as sr_active from 
(select "year",qrtr,salesrep_id   from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr)timedim
            left join itg_vn_dms_sales_org_dim sls_org
            on sls_org.salesrep_crtdate <= timedim.cal_date
            and (sls_org.salesrep_dateoff > timedim.cal_date or sls_org.salesrep_dateoff = '1900-01-01'))group by "year",qrtr) salesrep_Active
on salesrep_Active."year" = store."year"
 and salesrep_Active.qrtr = store.qrtr
 left join ----- for ss_active
 (select "year",qrtr,count( distinct supervisor_code)as ss_active from 
(select "year",qrtr, supervisor_code   from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr)timedim
            left join (select distinct supervisor_code , sup_crtdate, sup_dateoff from itg_vn_dms_sales_org_dim where supervisor_code is not null ) sls_org
            on sls_org.sup_crtdate <= timedim.cal_date
            and (sls_org.sup_dateoff > timedim.cal_date or sls_org.sup_dateoff = '1900-01-01'))group by "year",qrtr) supervisor_active
on supervisor_active."year" = store."year"
 and supervisor_active.qrtr = store.qrtr
 left join ----for sr_inactive
 (select "year",qrtr,count( distinct salesrep_id)as sr_inactive from 
(select "year",qrtr,salesrep_id   from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    min(edw_vw_os_time_dim.cal_date) as start_date,
                   max(edw_vw_os_time_dim.cal_date) as end_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr)timedim
            left join itg_vn_dms_sales_org_dim  sls_org
            on sls_org.salesrep_dateoff between timedim.start_date and timedim.end_date)group by "year",qrtr) salesrep_inactive
on salesrep_inactive."year" = store."year"
 and salesrep_inactive.qrtr = store.qrtr
 left join -------ss_inactive
 (select "year",qrtr,count( distinct supervisor_code)as ss_inactive from 
(select "year",qrtr, supervisor_code   from 
(select edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    min(edw_vw_os_time_dim.cal_date) as start_date,
                   max(edw_vw_os_time_dim.cal_date) as end_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr)timedim
            left join (select distinct supervisor_code , sup_dateoff from itg_vn_dms_sales_org_dim where supervisor_code is not null ) sls_org
            on sls_org.sup_dateoff between timedim.start_date and timedim.end_date)group by "year",qrtr) supervisor_inactive
on supervisor_inactive."year" = store."year"
 and supervisor_inactive.qrtr = store.qrtr
 ------------------------------------ "year"ly level data-----------------------------
 union all 
 select store."year" as jj_year,
       null as jj_qrtr,
       null as jj_mnth_id,  
       null as territory_dist,
       null as dist_type,
       null as asm_id,
       null as so_value,
	   null as so_yearly_target,
	   null as ub_brand_lvl_yearly_target,
       null as so_target,
       null as inv_value,
	   null as inv_value_prev,
       null as brand,
       undup_buy.ub as ub,
       undup_buy.ub_yearly_target as ub_yearly_target,
       prod_call.pc as pc,
       prod_call.pc_yearly_target as pc_yearly_target,
       store.total_stores,
	   store.total_stores_yearly_target,
       salesrep_Active.sr_active as sr_active,
       salesrep_Active.sr_active_yearly_target as sr_active_yearly_target,
       supervisor_active.ss_active as ss_active,
       supervisor_active.ss_active_yearly_target as ss_active_yearly_target,
       salesrep_inactive.sr_inactive as sr_inactive,
       supervisor_inactive.ss_inactive as ss_inactive,
	   sr_resigned_yearly_target as sr_resigned_yearly_target,
	   ub_coverage_percent_tgt as UB_Coverage_percent_yearly_target,
	   sr_pc_sr_yearly_tgt as PC_SR_yearly_target,
	   sr_ub_sr_yearly_tgt as UB_SR_yearly_target,
	   sr_Revenue_SR_M_yearly_tgt as revenue_SR_M_yearly_target,
	   sr_percent_attr_yearly_tgt as SR_attrition_percent_yearly_target
 from 
(select store_actual."year", count( distinct store_actual.outlet_id)as total_stores,store_target.cov_tagt as total_stores_yearly_target 
from 
(select "year", outlet_id  from 
(select edw_vw_os_time_dim."year",
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year")timedim
            left join itg_vn_dms_customer_dim cust_cd
            on cust_cd.crt_date <= timedim.cal_date
            and (cust_cd.date_off > timedim.cal_date or cust_cd.date_off = '1900-01-01')) store_actual
left join (select year,sum(target) as cov_tagt from itg_vn_dms_yearly_target where kpi = 'Coverage' group by year) store_target   
on  store_target.year = store_actual."year"         
group by store_actual."year",store_target.cov_tagt) store

 left join  ---- for calculating ub
 (select jj_year, count(distinct ub_actual.cust_cd) as ub,ub_tgt.ub_tagt as ub_yearly_target,ub_coverage_percent_tgt
from wks_vn_si_st_so_details_forecast  ub_actual 
left join (select year,sum(target) as ub_tagt from itg_vn_dms_yearly_target where kpi = 'UB' group by year) ub_tgt
on ub_tgt.year = ub_actual.jj_year
left join (select year,sum(target)*100 as ub_coverage_percent_tgt from itg_vn_dms_yearly_target where kpi = '% UB/ Coverage' group by year) ub_coverage_percent_yearly_tgt
on ub_coverage_percent_yearly_tgt.year = ub_actual.jj_year
group by ub_actual.jj_year,ub_tgt.ub_tagt,ub_coverage_percent_tgt
) undup_buy
 on undup_buy.jj_year = store."year"
 left join --- for calculating pc
 (select pc_actual.jj_year,count(pc_actual.*) as pc,pc_tgt.pc_tagt as pc_yearly_target from wks_vn_si_st_so_details_forecast pc_actual
  left join (select year,sum(target) as pc_tagt from itg_vn_dms_yearly_target where kpi = 'PC' group by year) pc_tgt
  on pc_tgt.year = pc_actual.jj_year
  where pc_actual.product_visit_call_date is not null 
 group by pc_actual.jj_year,pc_tgt.pc_tagt) prod_call
on prod_call.jj_year = store."year"
 left join --- for sr_active
 (select sr_active_actual."year", sr_active,sr_active_tgt.sr_active_tagt as sr_active_yearly_target,sr_pc_sr_yearly_tgt,sr_ub_sr_yearly_tgt,sr_Revenue_SR_M_yearly_tgt,
sr_percent_attr_yearly_tgt
from
(select sr_active_actual1."year",count( distinct sr_active_actual1.salesrep_id)as sr_active
from 
(select "year",salesrep_id   from 
(select edw_vw_os_time_dim."year",
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year")timedim
            left join itg_vn_dms_sales_org_dim sls_org
            on sls_org.salesrep_crtdate <= timedim.cal_date
            and (sls_org.salesrep_dateoff > timedim.cal_date or sls_org.salesrep_dateoff = '1900-01-01')) sr_active_actual1
group by sr_active_actual1."year") sr_active_actual                                        
left join (select year,sum(target) as sr_active_tagt from itg_vn_dms_yearly_target where kpi = 'Headcount SR' group by year) sr_active_tgt
on sr_active_tgt.year = sr_active_actual."year"
left join (select year,sum(target) as sr_pc_sr_yearly_tgt from itg_vn_dms_yearly_target where kpi = 'PC/SR' group by year) sr_pc_sr_tgt
on sr_pc_sr_tgt.year = sr_active_actual."year"
left join (select year,sum(target) as sr_ub_sr_yearly_tgt from itg_vn_dms_yearly_target where kpi = 'UB/SR' group by year) sr_ub_sr_tgt
on sr_ub_sr_tgt.year = sr_active_actual."year"
left join (select year,sum(target) as sr_Revenue_SR_M_yearly_tgt from itg_vn_dms_yearly_target where kpi = 'Revenue/ SR/ M' group by year) sr_Revenue_SR_M_tgt
on sr_Revenue_SR_M_tgt.year = sr_active_actual."year" 
left join (select year,sum(target) as sr_percent_attr_yearly_tgt from itg_vn_dms_yearly_target where kpi = '% Attrition' group by year) sr_percent_attr_tgt
on sr_percent_attr_tgt.year = sr_active_actual."year" ) salesrep_Active
on salesrep_Active."year" = store."year"
 left join ----- for ss_active
 (select ss_active_actual."year",count( distinct ss_active_actual.supervisor_code)as ss_active,ss_active_tgt.ss_active_tagt as ss_active_yearly_target from 
(select "year",supervisor_code   from 
(select edw_vw_os_time_dim."year",
                   max(edw_vw_os_time_dim.cal_date) as cal_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year")timedim
            left join (select distinct supervisor_code , sup_crtdate, sup_dateoff from itg_vn_dms_sales_org_dim where supervisor_code is not null ) sls_org
            on sls_org.sup_crtdate <= timedim.cal_date
            and (sls_org.sup_dateoff > timedim.cal_date or sls_org.sup_dateoff = '1900-01-01')) ss_active_actual
  left join (select year,sum(target) as ss_active_tagt from itg_vn_dms_yearly_target where kpi = 'Headcount SS' group by year) ss_active_tgt
 on ss_active_tgt.year = ss_active_actual."year"          
 group by ss_active_actual."year",ss_active_tgt.ss_active_tagt) supervisor_active
on supervisor_active."year" = store."year"
 left join ----for sr_inactive
 (select sr_inactive_actual."year",count( distinct sr_inactive_actual.salesrep_id)as sr_inactive,sr_inactive_tgt.sr_inactive_tagt as sr_resigned_yearly_target from 
(select "year",salesrep_id   from 
(select edw_vw_os_time_dim."year",
                    min(edw_vw_os_time_dim.cal_date) as start_date,
                   max(edw_vw_os_time_dim.cal_date) as end_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year")timedim
            left join itg_vn_dms_sales_org_dim  sls_org
            on sls_org.salesrep_dateoff between timedim.start_date and timedim.end_date) sr_inactive_actual
left join (select year,round(sum(target),0) as sr_inactive_tagt from itg_vn_dms_yearly_target where kpi = 'Resigned' group by year) sr_inactive_tgt
 on sr_inactive_tgt.year = sr_inactive_actual."year"        
 group by sr_inactive_actual."year",sr_inactive_tgt.sr_inactive_tagt) salesrep_inactive
on salesrep_inactive."year" = store."year"
 left join -------ss_inactive
 (select "year",count( distinct supervisor_code)as ss_inactive from 
(select "year", supervisor_code   from 
(select edw_vw_os_time_dim."year",
                    min(edw_vw_os_time_dim.cal_date) as start_date,
                   max(edw_vw_os_time_dim.cal_date) as end_date
             from edw_vw_os_time_dim
             group by edw_vw_os_time_dim."year")timedim
            left join (select distinct supervisor_code , sup_dateoff from itg_vn_dms_sales_org_dim where supervisor_code is not null ) sls_org
            on sls_org.sup_dateoff between timedim.start_date and timedim.end_date)group by "year") supervisor_inactive
on supervisor_inactive."year" = store."year")gt_sc
 left join (select mnth_id ,min(cal_date) as mnth_start_date ,max(cal_date) as mnth_end_date from edw_vw_os_time_dim  group by mnth_id)jj_cal
ON gt_sc.jj_mnth_id = jj_cal.mnth_id
left join 
(select "year"||mnth as mnth_id , ex_rt from 
(select fisc_yr_per,substring(cast(fisc_yr_per as varchar),1,4) as "year",substring(cast(fisc_yr_per as varchar),6,2) as mnth, 
ex_rt from edw_crncy_exch_rates where from_crncy = 'VND' and to_crncy = 'USD')) exch_rate
on cast(gt_sc.jj_mnth_id as numeric) = cast(exch_rate.mnth_id as numeric)) 
),
final as (
select
    jj_year::number(18,0) as jj_year,
    jj_qrtr::varchar(14) as jj_qrtr,
    jj_mnth_id::varchar(23) as jj_mnth_id,
    territory_dist::varchar(100) as territory_dist,
    dist_type::varchar(100) as dist_type,
    asm_id::varchar(200) as asm_id,
    so_value::number(38,4) as so_value,
    so_yearly_target::number(38,4) as so_yearly_target,
    ub_brand_lvl_yearly_target::number(38,4) as ub_brand_lvl_yearly_target,
    so_target::number(38,6) as so_target,
    inv_value::number(38,4) as inv_value,
    inv_value_prev::number(38,4) as inv_value_prev,
    brand::varchar(100) as brand,
    ub::number(38,0) as ub,
    ub_yearly_target::number(38,4) as ub_yearly_target,
    pc::number(38,0) as pc,
    pc_yearly_target::number(38,4) as pc_yearly_target,
    total_stores::number(38,0) as total_stores,
    total_stores_yearly_target::number(38,4) as total_stores_yearly_target,
    sr_active::number(38,0) as sr_active,
    sr_active_yearly_target::number(38,4) as sr_active_yearly_target,
    ss_active::number(38,0) as ss_active,
    ss_active_yearly_target::number(38,4) as ss_active_yearly_target,
    sr_inactive::number(38,0) as sr_inactive,
    ss_inactive::number(38,0) as ss_inactive,
    sr_resigned_yearly_target::number(35,0) as sr_resigned_yearly_target,
    ub_coverage_percent_yearly_target::number(38,4) as ub_coverage_percent_yearly_target,
    pc_sr_yearly_target::number(38,4) as pc_sr_yearly_target,
    ub_sr_yearly_target::number(38,4) as ub_sr_yearly_target,
    revenue_sr_m_yearly_target::number(38,4) as revenue_sr_m_yearly_target,
    sr_attrition_percent_yearly_target::number(38,4) as sr_attrition_percent_yearly_target,
    mnth_start_date::date as mnth_start_date,
    mnth_end_date::date as mnth_end_date,
    exchg_rate::number(14,10) as exchg_rate,
    curr_mnth_indc::varchar(1) as curr_mnth_indc,
    curr_year_indc::varchar(1) as curr_year_indc
from transformed
)
select * from final 