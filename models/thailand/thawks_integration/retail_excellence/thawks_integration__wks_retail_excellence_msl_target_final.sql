with  mds_reds_market_msl_target_final as
(
    select * from {{ ref('aspedw_integration__wks_mds_reds_market_msl_target_final') }}
),
rpt_retail_excellence_gcph as 
 (
    select * from {{ ref('thawks_integration__th_wks_rpt_retail_excellence_sop') }}
 ),
 
 final as 
 (
   select b.fisc_per,
   b.global_product_brand,b.market,
   round((total_mdp_target/mds.mdp_target)*100 ):: integer as TARGET_COMPLAINCE 
   from 
        (select fisc_per,
        global_product_brand,market,
        count(1)   as total_mdp_target  
        from 
            (select  distinct fisc_per,
                distributor_code,
                 store_code, 
                 global_product_brand,market
                from rpt_retail_excellence_gcph
                where   mdp_flag = 'Y'
                ) a
        group by 1,2,3)b 
    inner join 
    (select * from mds_reds_market_msl_target_final where upper(market)='THAILAND'
     ) mds 
        on ( fisc_per >= mds.start_month_id and fisc_per <= mds.end_month_id and upper(b.global_product_brand)=upper(mds.brand_code))
    group by 1,2,3,4  
 )

select * from final