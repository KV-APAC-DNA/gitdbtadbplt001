with  wks_mds_reds_market_msl_target_anz as
(
    select * from {{ ref('aspedw_integration__wks_mds_reds_market_msl_target_final') }}
),

anz_rpt_re_gcph as 
 (
    select * from {{ ref('pcfwks_integration__wks_anz_rpt_re_gcph') }}
 ),
 
 final as 
 (
   select b.jj_mnth_id,
   b.global_product_brand,
   ((total_mdp_target/mds.mdp_target)*100 ):: decimal(38,6) as target_compliance 
   from 
        (select jj_mnth_id,
        global_product_brand,
        count(1)   as total_mdp_target  
        from 
            (select  distinct jj_mnth_id,
                distributor_code,
                 store_code, 
                 global_product_brand
                from anz_rpt_re_gcph
                where   mdp_flag = 'Y'
                ) a
        group by 1,2)b 
    inner join 
    (select * from wks_mds_reds_market_msl_target_anz where UPPER(market) = 'PACIFIC'
     ) mds 
        on ( jj_mnth_id >= mds.start_month_id and jj_mnth_id <= mds.end_month_id and upper(b.global_product_brand)=upper(mds.brand_code))
    group by 1,2,3 
 )

--final select

select * from final