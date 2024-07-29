--import cte
with mds_reds_market_msl_target as (
    select * from {{ ref('aspedw_integration__wks_mds_reds_market_msl_target_final') }}
),

wks_id_rpt_re as (
    select * from {{ ref('idnwks_integration__wks_id_rpt_re') }}
),

wks_id_re_msl_targets as (
    select act.jj_mnth_id , 
           act.global_product_brand , 
           case when (tgt.mdp_target <> 0 or tgt.mdp_target is not null) then
           (tgt.mdp_target/act.actuals):: numeric(38,6) 
           else 100 end as target_complaince from
   (select jj_mnth_id , global_product_brand, count(1) as actuals from
    (select distinct jj_mnth_id , distributor_code , store_code , product_code,global_product_brand 
    from wks_id_rpt_re where mdp_flag = 'Y')
    group by jj_mnth_id, global_product_brand) act
    left join mds_reds_market_msl_target tgt 
    on act.jj_mnth_id >=  tgt.start_month_id and act.jj_mnth_id <= tgt.end_month_id 
    and market = 'Indonesia'
    and upper(act.global_product_brand) = upper(tgt.brand_code)
    
),

final as (
    select jj_mnth_id::numeric(18) as jj_mnth_id,
    global_product_brand::varchar(30) as global_product_brand,
    target_complaince::numeric(38,6) as target_complaince
    from wks_id_re_msl_targets
)

--final select
select * from final