with  mds_reds_market_msl_target_final as
(
    select * from {{ ref('aspedw_integration__wks_mds_reds_market_msl_target_final') }}
),
rpt_retail_excellence_gcph as 
 (
    select * from {{ ref('myswks_integration__wks_my_rpt_re_gcph') }}
 ),
 
 final as 
 (
   select b.jj_mnth_id,
   b.global_product_brand,b.market,
   ((total_mdp_target/mds.mdp_target)*100 ):: decimal(38,6) as TARGET_COMPLAINCE 
   from 
        (select jj_mnth_id,
        global_product_brand,market,
        count(1)   as total_mdp_target  
        from 
            (select  distinct jj_mnth_id,
                distributor_code,
                 store_code, 
                 global_product_brand,market
                from rpt_retail_excellence_gcph
                where   mdp_flag = 'Y'
                ) a
        group by 1,2,3)b 
    inner join 
    (select * from mds_reds_market_msl_target_final where upper(market)='MALAYSIA'
     ) mds 
        on ( jj_mnth_id >= mds.start_month_id and jj_mnth_id <= mds.end_month_id and upper(b.global_product_brand)=upper(mds.brand_code))
    group by 1,2,3,4  
 )

select * from final