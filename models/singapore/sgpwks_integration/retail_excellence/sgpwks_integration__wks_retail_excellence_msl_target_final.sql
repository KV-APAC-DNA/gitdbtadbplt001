with  mds_reds_market_msl_target_final as
(
    select * from {{ ref('aspedw_integration__wks_mds_reds_market_msl_target_final') }}
),
sg_rpt_retail_excellence as 
 (
    select * from {{ ref('sgpwks_integration__wks_sg_rpt_retail_excellence_sop') }}
 ),
 
 final as 
 (
   select b.fisc_per,
   b.global_product_brand,
    round((mds.mdp_target/total_mdp_target)*100 ):: integer as target_compliance 
   from 
        (select fisc_per,
        global_product_brand,
        count(1)   as total_mdp_target  
        from 
            (select  distinct fisc_per,
                distributor_code,
                 store_code, 
                 product_code,
                 global_product_brand,
                from sg_rpt_retail_excellence
                where   mdp_flag = 'Y'
                ) a
        group by 1,2)b 
    inner join 
    (select * from mds_reds_market_msl_target_final where market='Singapore'
     ) mds 
        on ( fisc_per >= mds.start_month_id and fisc_per <= mds.end_month_id and upper(b.global_product_brand)=upper(mds.brand_code))
    group by 1,2,3 
 )

select * from final
