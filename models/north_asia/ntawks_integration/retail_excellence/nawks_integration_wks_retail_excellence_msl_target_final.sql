with  mds_reds_market_msl_target_final as
(
    select * from {{ ref('natwks_integration_wks_mds_reds_market_msl_target_final') }}
),
rpt_retail_excellence_gcph as 
 (
    select * from {{ ref('nawks_integration__wks_kr_rpt_retail_excellence_gcph') }}
 ),
 mds_reds_market_msl_target_final as 
  (
    select * from {{ ref('natwks_integration_wks_mds_reds_market_msl_target_final') }}
  ),
 final as 
 (
   select b.fisc_per,
   b.global_product_brand,
   ((total_mdp_target/mds.mdp_target)*100 ):: decimal(38,3) as target_compliance 
   from 
        (select fisc_per,
        global_product_brand,
        count(1)   as total_mdp_target  
        from 
            (select  distinct fisc_per,
                distributor_code,
                 store_code, 
                 global_product_brand,
                from rpt_retail_excellence_gcph
                where   mdp_flag = 'Y'
                ) a
        group by 1,2)b 
    inner join 
    (select * from mds_reds_market_msl_target_final
     ) mds 
        on ( fisc_per >= mds.start_month_id and fisc_per <= mds.end_month_id and upper(b.global_product_brand)=upper(mds.brand_code))
    group by 1,2,3 
 )

select * from final
