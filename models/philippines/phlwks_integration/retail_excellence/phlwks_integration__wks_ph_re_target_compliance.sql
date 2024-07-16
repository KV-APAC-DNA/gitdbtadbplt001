with  wks_mds_reds_market_msl_target_ph as
(
    select * from {{ ref('aspedw_integration__wks_mds_reds_market_msl_target_final') }}
),

ph_rpt_retail_excellence_sop as 
 (
    select * from {{ ref('phlwks_integration__wks_ph_rpt_retail_excellence_sop') }}
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
                 global_product_brand
                from ph_rpt_retail_excellence_sop
                where   mdp_flag = 'Y'
                ) a
        group by 1,2)b 
    inner join 
    (select * from wks_mds_reds_market_msl_target_ph where UPPER(market) = 'PHILIPPINES'
     ) mds 
        on ( fisc_per >= mds.start_month_id and fisc_per <= mds.end_month_id and upper(b.global_product_brand)=upper(mds.brand_code))
    group by 1,2,3 
 )

--final select

select * from final