with  wks_mds_reds_market_msl_target_anz as
(
    select * from {{ ref('aspedw_integration__wks_mds_reds_market_msl_target_final') }}
),
wks_anz_pos_rpt_re as 
 (
    select * from {{ ref('pcfwks_integration__wks_anz_pos_rpt_re') }}
 ),
 
 final as 
 (
   select b.jj_mnth_id,
   b.global_product_brand,
   (mds.mdp_target/total_mdp_target):: numeric(38,6) as TARGET_COMPLAINCE
    --round((mds.mdp_target/total_mdp_target)*100 ):: integer as TARGET_COMPLAINCE
   from 
        (select jj_mnth_id,
        global_product_brand,
        sum(cm_universe_stores) as total_mdp_target  
        from 
            (select  distinct jj_mnth_id,
                distributor_code,
                product_code,cm_universe_stores ,
                 global_product_brand
                from wks_anz_pos_rpt_re
                where   mdp_flag = 'Y'
                ) a
        group by 1,2)b 
    inner join 
    (select * from wks_mds_reds_market_msl_target_anz where UPPER(market) in ('AUSTRALIA','NEW ZEALAND')
     ) mds 
        on ( jj_mnth_id >= mds.start_month_id and jj_mnth_id <= mds.end_month_id and upper(b.global_product_brand)=upper(mds.brand_code))
    group by 1,2,3 
 )

--final select

select * from final