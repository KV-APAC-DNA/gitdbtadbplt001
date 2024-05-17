with
itg_sales_rep_so_target_fact as (
select * from dev_dna_core.snapntaitg_integration.itg_sales_rep_so_target_fact
),
v_intrm_calendar_ims as (
select * from dev_dna_core.snapntaedw_integration.v_intrm_calendar_ims
),
final as (
select 
  t1.ctry_cd, 
  t1.dstr_cd, 
  t1.crncy_cd, 
  t2.cal_day, 
  t2.jj_wk_num, 
  t1.jj_mnth_id, 
  t1.sls_rep_cd, 
  t1.sls_rep_nm, 
  t1.brand, 
  (
    t1.sls_trgt_val / (t2.num_of_days):: numeric
  ) as sls_trgt_val 
from 
  itg_sales_rep_so_target_fact t1, 
  (
    select 
      v_intrm_calendar_ims.cal_day, 
      v_intrm_calendar_ims.fisc_wk_num as jj_wk_num, 
      (
        (
          "substring"(
            (v_intrm_calendar_ims.fisc_per):: text, 1, 4) 
            || 
           "substring"(
            (v_intrm_calendar_ims.fisc_per):: text, 6, 2)
        )
      ):: integer as jj_mnth_id, 
      count(1) over(
        partition by (
          "substring"(
            (v_intrm_calendar_ims.fisc_per):: text, 1, 4) || 
            "substring"(
            (v_intrm_calendar_ims.fisc_per):: text, 6, 2)
        ) order by null rows between unbounded preceding 
        and unbounded following
      ) as num_of_days 
    from 
      v_intrm_calendar_ims 
    where 
      (v_intrm_calendar_ims.wkday <> 7)
  ) t2 
where 
  (t1.jj_mnth_id = t2.jj_mnth_id)
)
select * from final