{{
    config(
        post_hook = "delete  from {{this}}  where sub_channel in ('TD001-DÆ°Æ¡ng Anh','TD002-Tiáº¿n ThÃ nh')"
    )
}}

with 
wks_vn_oneview_rpt as 
(
    select * from {{ ref('vnmsdl_wks__wks_vn_oneview_rpt') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_query_parameters as 
(
    select * from {{ source('vnmitg_integration', 'itg_query_parameters') }}
),

time_dim as 
(
    select "year" as jj_year,
        substring(qrtr,6,7) as jj_qrtr,
        mnth_id as jj_mnth_id,
        mnth_no as jj_mnth_no,
        mnth_wk_no as jj_mnth_wk_no,
        cal_date,
        ROW_NUMBER() OVER (PARTITION BY mnth_id ORDER BY cal_date) AS jj_mnth_day
        FROM edw_vw_os_time_dim
    where "year" >= (   
                        select  (date_part(year,current_timestamp) - cast(2 as integer)) 
                            from itg_query_parameters 
                            where country_code = 'VN' 
                        and parameter_name = 'data_load_year'
                    )
 ),


 final as (
 Select 'Sell-Out Actual'  as data_type,
             'GT' as channel,
            vw.sub_channel,
             time_dim.jj_year,
             time_dim.jj_qrtr,
             time_dim.jj_mnth_id,
            time_dim.jj_mnth_no,
            time_dim.jj_mnth_wk_no,
            time_dim.jj_mnth_day,
            vw.mapped_spk,
            vw.dstrbtr_grp_cd  ,
            vw.dstrbtr_name ,
            vw.sap_sold_to_code,
            vw.sap_matl_num,
            vw.sap_matl_name,
            vw.dstrbtr_matl_num,
            vw.dstrbtr_matl_name,
            vw.bar_code,
            vw.customer_code,
            vw.customer_name,
            nvl(vw.invoice_date,time_dim.cal_date) as invoice_date,
            vw.salesman,
            vw.salesman_name,
            vw.si_gts_val,
	        vw.si_gts_excl_dm_val,
            vw.si_nts_val,
            vw.si_mnth_tgt_by_sku,
            vw.so_net_trd_sls_loc,
            vw.so_net_trd_sls_usd,
            vw.so_mnth_tgt,
            vw.so_avg_wk_tgt,
            vw.so_mnth_tgt_by_sku,
            vw.zone_manager_id,
            vw.zone_manager_name,
            vw.zone,
            vw.province,
            vw.region,
            vw.shop_type,
            vw.mt_sub_channel,
            vw.retail_environment,
            vw.group_account,
            vw.account,
            vw.franchise,
            vw.brand,
            vw.variant,
            vw.product_group,
            vw.group_jb
 from time_dim
 left join wks_vn_oneview_rpt vw
 on cal_date = invoice_date 
where  vw.channel = 'GT' and vw.data_type='Sell-Out Actual'
 UNION ALL
  Select 'Sell-Out Actual'  as data_type,
             'MT' as channel,
            vw.sub_channel,
             time_dim.jj_year,
             time_dim.jj_qrtr,
             time_dim.jj_mnth_id,
            time_dim.jj_mnth_no,
            time_dim.jj_mnth_wk_no,
            time_dim.jj_mnth_day,
            vw.mapped_spk,
            vw.dstrbtr_grp_cd  ,
            vw.dstrbtr_name ,
            vw.sap_sold_to_code,
            vw.sap_matl_num,
            vw.sap_matl_name,
            vw.dstrbtr_matl_num,
            vw.dstrbtr_matl_name,
           vw.bar_code,
            vw.customer_code,
            vw.customer_name,
            nvl(vw.invoice_date,time_dim.cal_date) as invoice_date,
            vw.salesman,
            vw.salesman_name,
            vw.si_gts_val,
	    vw.si_gts_excl_dm_val,
            vw.si_nts_val,
            vw.si_mnth_tgt_by_sku,
            vw.so_net_trd_sls_loc,
            vw.so_net_trd_sls_usd,
            vw.so_mnth_tgt,
            vw.so_avg_wk_tgt,
            vw.so_mnth_tgt_by_sku,
            vw.zone_manager_id,
            vw.zone_manager_name,
            vw.zone,
            vw.province,
            vw.region,
            vw.shop_type,
            vw.mt_sub_channel,
            vw.retail_environment,
             vw.group_account,
           vw.account,
            vw.franchise,
            vw.brand,
            vw.variant,
            vw.product_group,
            vw.group_jb
 from time_dim
 left join wks_vn_oneview_rpt vw
 on cal_date = invoice_date 
where  vw.channel = 'MT' and vw.data_type='Sell-Out Actual'
 UNION ALL
  Select 'Sell-Out Actual'  as data_type,
             'ECOM' as channel,
            vw.sub_channel,
             time_dim.jj_year,
             time_dim.jj_qrtr,
             time_dim.jj_mnth_id,
            time_dim.jj_mnth_no,
            time_dim.jj_mnth_wk_no,
            time_dim.jj_mnth_day,
            vw.mapped_spk,
            vw.dstrbtr_grp_cd  ,
            vw.dstrbtr_name ,
            vw.sap_sold_to_code,
            vw.sap_matl_num,
            vw.sap_matl_name,
            vw.dstrbtr_matl_num,
            vw.dstrbtr_matl_name,
           vw.bar_code,
            vw.customer_code,
            vw.customer_name,
            nvl(vw.invoice_date,time_dim.cal_date) as invoice_date,
            vw.salesman,
            vw.salesman_name,
            vw.si_gts_val,
	    vw.si_gts_excl_dm_val,
            vw.si_nts_val,
            vw.si_mnth_tgt_by_sku,
            vw.so_net_trd_sls_loc,
            vw.so_net_trd_sls_usd,
            vw.so_mnth_tgt,
            vw.so_avg_wk_tgt,
            vw.so_mnth_tgt_by_sku,
            vw.zone_manager_id,
            vw.zone_manager_name,
            vw.zone,
            vw.province,
            vw.region,
            vw.shop_type,
            vw.mt_sub_channel,
            vw.retail_environment,
             vw.group_account,
           vw.account,
            vw.franchise,
            vw.brand,
            vw.variant,
            vw.product_group,
            vw.group_jb
 from time_dim
 left join wks_vn_oneview_rpt vw
 on cal_date = invoice_date 
where vw.channel = 'ECOM' and vw.data_type='Sell-Out Actual'
 UNION ALL
  Select vw.data_type,
             vw.channel,
            vw.sub_channel,
             vw.jj_year,
             vw.jj_qrtr,
             vw.jj_mnth_id,
            vw.jj_mnth_no,
            vw.jj_mnth_wk_no,
            case when data_type like '%Target' then 1 else vw.jj_mnth_day end as jj_mnth_day,
            vw.mapped_spk,
            vw.dstrbtr_grp_cd  ,
            vw.dstrbtr_name ,
            vw.sap_sold_to_code,
            vw.sap_matl_num,
            vw.sap_matl_name,
            vw.dstrbtr_matl_num,
            vw.dstrbtr_matl_name,
           vw.bar_code,
            vw.customer_code,
            vw.customer_name,
            vw.invoice_date,
            vw.salesman,
            vw.salesman_name,
            vw.si_gts_val,
	    vw.si_gts_excl_dm_val,
            vw.si_nts_val,
            vw.si_mnth_tgt_by_sku,
            vw.so_net_trd_sls_loc,
            vw.so_net_trd_sls_usd,
            vw.so_mnth_tgt,
            vw.so_avg_wk_tgt,
            vw.so_mnth_tgt_by_sku,
            vw.zone_manager_id,
            vw.zone_manager_name,
            vw.zone,
            vw.province,
            vw.region,
            vw.shop_type,
            vw.mt_sub_channel,
            vw.retail_environment,
             vw.group_account,
           vw.account,
            vw.franchise,
            vw.brand,
            vw.variant,
            vw.product_group,
            vw.group_jb
 from wks_vn_oneview_rpt vw
where  vw.data_type <>'Sell-Out Actual'
and vw.jj_mnth_id >= (select min(jj_mnth_id) from time_dim)
Union all

 Select 'Sell-Out Actual'  as data_type,
             'OTC' as channel,
            vw.sub_channel,
             vw.jj_year,
             time_dim.jj_qrtr,
             vw.jj_mnth_id,
            vw.jj_mnth_no,
            time_dim.jj_mnth_wk_no,
            time_dim.jj_mnth_day,
            vw.mapped_spk,
            vw.dstrbtr_grp_cd  ,
            vw.dstrbtr_name ,
            vw.sap_sold_to_code,
            vw.sap_matl_num,
            vw.sap_matl_name,
            vw.dstrbtr_matl_num,
            vw.dstrbtr_matl_name,
           vw.bar_code,
            vw.customer_code,
            vw.customer_name,
           vw.invoice_date,
            vw.salesman,
            vw.salesman_name,
            vw.si_gts_val,
	          vw.si_gts_excl_dm_val,
            vw.si_nts_val,
            vw.si_mnth_tgt_by_sku,
            vw.so_net_trd_sls_loc,
            vw.so_net_trd_sls_usd,
            vw.so_mnth_tgt,
            vw.so_avg_wk_tgt,
            vw.so_mnth_tgt_by_sku,
            vw.zone_manager_id,
            vw.zone_manager_name,
            vw.zone,
            vw.province,
            vw.region,
            vw.shop_type,
            vw.mt_sub_channel,
            vw.retail_environment,
             vw.group_account,
           vw.account,
            vw.franchise,
            vw.brand,
            vw.variant,
            vw.product_group,
            vw.group_jb
 from  wks_vn_oneview_rpt vw
 left join time_dim
 on cal_date = invoice_date 
where vw.channel = 'OTC' and vw.data_type='Sell-Out Actual'
and vw.jj_mnth_id >= (select min(jj_mnth_id) from time_dim)
)

select * from final