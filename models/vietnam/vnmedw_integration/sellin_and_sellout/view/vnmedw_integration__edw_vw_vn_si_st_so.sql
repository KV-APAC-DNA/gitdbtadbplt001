with wks_vn_si_st_so_details_forecast as (
select * from {{ ref('vnmwks_integration__wks_vn_si_st_so_details_forecast') }}
),
edw_vw_vn_material_dim as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_material_dim') }}
),
edw_vw_vn_customer_dim as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_customer_dim') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_vn_dms_distributor_dim as (
select * from {{ ref('vnmitg_interation__itg_vn_dms_distributor_dim') }}
),
edw_crncy_exch_rates as (
select * from {{ ref('aspedw_integration__edw_crncy_exch_rates') }}
),
final as (
select *,
    case
        when current_timestamp() BETWEEN mnth_start_date AND mnth_end_date then 'Y'
        else 'N'
    end as curr_mnth_indc,
    case
        when jj_year = extract(
            year
            from current_timestamp()
        ) then 'Y'
        else 'N'
    end curr_year_indc
from (
        select jj_year,
            jj_qrtr,
            jj_mnth_id,
            jj_mnth_no,
            jj_mnth_wk_no,
            dsr.wks_in_qrtr,
            jj_cal.mnth_start_date,
            jj_cal.mnth_end_date,
            dsr.territory_dist,
            dsr.distributor_id_report,
            dsr.dstrbtr_grp_cd,
            dstrb.dstrbtr_name,
            sap_sold_to_code,
            dsr.sap_matl_num,
            dsr.sap_matl_name,
            dsr.dstrbtr_matl_num,
            dsr.dstrbtr_matl_name,
            dsr.cust_cd,
            trade_type,
            bill_doc,
            bill_date,
            exch_rate.ex_rt as exchg_rate,
            slsmn_cd,
            slsmn_nm,
            slsmn_status,
            so_sls_qty,
            so_ret_qty,
            so_grs_trd_sls,
            so_net_trd_sls,
            so_avg_wk_tgt,
            so_mnth_tgt,
            supervisor_code,
            supervisor_name,
            dsr.asm_id,
            dsr.asm_name,
            dstrb_region,
            outlet_name,
            shop_type,
            group_hierarchy,
            top_door_group,
            top_door_flag,
            top_door_target,
            si_sls_qty,
            si_ret_qty,
            si_gts_val,
            si_nts_val,
            si_target as si_mnth_tgt,
            si_avg_wk_tgt,
            st_sls_qty,
            st_ret_qty,
            st_grs_trd_sls,
            st_net_trd_sls,
            st_target as st_mnth_tgt,
            st_avg_wk_tgt,
            PC_Target,
            PC_target_by_week,
            CE_Target,
            ce_target_by_week,
            ub_weekly_target,
            visit_call_date,
            product_visit_call_date,
            forecastso_mil,
            veocd.sap_cntry_nm as cntry_nm,
            veocd.sap_state_cd,
            veocd.sap_sls_org,
            veocd.sap_cmp_id,
            veocd.sap_cntry_cd,
            veocd.sap_cntry_nm,
            veocd.sap_addr,
            veocd.sap_region,
            veocd.sap_city,
            veocd.sap_post_cd,
            veocd.sap_chnl_cd,
            veocd.sap_chnl_desc,
            veocd.sap_sls_office_cd,
            veocd.sap_sls_office_desc,
            veocd.sap_sls_grp_cd,
            veocd.sap_sls_grp_desc,
            veocd.sap_curr_cd,
            veocd.gch_region,
            veocd.gch_cluster,
            veocd.gch_subcluster,
            veocd.gch_market,
            veocd.gch_retail_banner,
            LTRIM(veomd.sap_matl_num, '0') as sku,
            veomd.sap_mat_desc as sku_desc,
            veomd.sap_mat_type_cd,
            veomd.sap_mat_type_desc,
            veomd.sap_base_uom_cd,
            veomd.sap_prchse_uom_cd,
            veomd.sap_prod_sgmt_cd,
            veomd.sap_prod_sgmt_desc,
            veomd.sap_base_prod_cd,
            veomd.sap_base_prod_desc,
            veomd.sap_mega_brnd_cd,
            veomd.sap_mega_brnd_desc,
            veomd.sap_brnd_cd,
            veomd.sap_brnd_desc,
            veomd.sap_vrnt_cd,
            veomd.sap_vrnt_desc,
            veomd.sap_put_up_cd,
            veomd.sap_put_up_desc,
            veomd.sap_grp_frnchse_cd,
            veomd.sap_grp_frnchse_desc,
            veomd.sap_frnchse_cd,
            veomd.sap_frnchse_desc,
            veomd.sap_prod_frnchse_cd,
            veomd.sap_prod_frnchse_desc,
            veomd.sap_prod_mjr_cd,
            veomd.sap_prod_mjr_desc,
            veomd.sap_prod_mnr_cd,
            veomd.sap_prod_mnr_desc,
            veomd.sap_prod_hier_cd,
            veomd.sap_prod_hier_desc,
            veomd.gph_region as global_mat_region,
            veomd.gph_prod_frnchse as global_prod_franchise,
            veomd.gph_prod_brnd as global_prod_brand,
            veomd.gph_prod_vrnt as global_prod_variant,
            veomd.gph_prod_put_up_cd as global_prod_put_up_cd,
            veomd.gph_prod_put_up_desc as global_put_up_desc,
            veomd.gph_prod_sub_brnd as global_prod_sub_brand,
            veomd.gph_prod_needstate as global_prod_need_state,
            veomd.gph_prod_ctgry as global_prod_category,
            veomd.gph_prod_subctgry as global_prod_subcategory,
            veomd.gph_prod_sgmnt as global_prod_segment,
            veomd.gph_prod_subsgmnt as global_prod_subsegment,
            veomd.gph_prod_size as global_prod_size,
            veomd.gph_prod_size_uom as global_prod_size_uom,
            dsr.franchise local_prod_franchise,
            dsr.brand local_prod_brand,
            dsr.variant local_prod_variant,
            dsr.product_group local_prod_grp,
            dsr.group_jb as local_group_jb,
            dsr.groupmsl
        from wks_vn_si_st_so_details_forecast dsr
            LEFT JOIN (
                select cntry_key,
                    sap_matl_num,
                    sap_mat_desc,
                    ean_num,
                    sap_mat_type_cd,
                    sap_mat_type_desc,
                    sap_base_uom_cd,
                    sap_prchse_uom_cd,
                    sap_prod_sgmt_cd,
                    sap_prod_sgmt_desc,
                    sap_base_prod_cd,
                    sap_base_prod_desc,
                    sap_mega_brnd_cd,
                    sap_mega_brnd_desc,
                    sap_brnd_cd,
                    sap_brnd_desc,
                    sap_vrnt_cd,
                    sap_vrnt_desc,
                    sap_put_up_cd,
                    sap_put_up_desc,
                    sap_grp_frnchse_cd,
                    sap_grp_frnchse_desc,
                    sap_frnchse_cd,
                    sap_frnchse_desc,
                    sap_prod_frnchse_cd,
                    sap_prod_frnchse_desc,
                    sap_prod_mjr_cd,
                    sap_prod_mjr_desc,
                    sap_prod_mnr_cd,
                    sap_prod_mnr_desc,
                    sap_prod_hier_cd,
                    sap_prod_hier_desc,
                    gph_region,
                    gph_prod_frnchse,
                    gph_prod_brnd,
                    gph_prod_sub_brnd,
                    gph_prod_vrnt,
                    gph_prod_needstate,
                    gph_prod_ctgry,
                    gph_prod_subctgry,
                    gph_prod_sgmnt,
                    gph_prod_subsgmnt,
                    gph_prod_put_up_cd,
                    gph_prod_put_up_desc,
                    gph_prod_size,
                    gph_prod_size_uom,
                    launch_dt,
                    qty_shipper_pc,
                    prft_ctr,
                    shlf_life
                from edw_vw_vn_material_dim
            ) veomd ON UPPER (LTRIM (veomd.sap_matl_num, '0')) = UPPER (trim (dsr.sap_matl_num))
            LEFT JOIN (
                select sap_cust_id,
                    sap_cust_nm,
                    sap_sls_org,
                    sap_cmp_id,
                    sap_cntry_cd,
                    sap_cntry_nm,
                    sap_addr,
                    sap_region,
                    sap_state_cd,
                    sap_city,
                    sap_post_cd,
                    sap_chnl_cd,
                    sap_chnl_desc,
                    sap_sls_office_cd,
                    sap_sls_office_desc,
                    sap_sls_grp_cd,
                    sap_sls_grp_desc,
                    sap_curr_cd,
                    gch_region,
                    gch_cluster,
                    gch_subcluster,
                    gch_market,
                    gch_retail_banner
                from edw_vw_vn_customer_dim
            ) veocd ON UPPER (LTRIM (veocd.sap_cust_id, '0')) = UPPER (trim (dsr.sap_sold_to_code))
            LEFT JOIN (
                select mnth_id,
                    MIN(cal_date) as mnth_start_date,
                    MAX(cal_date) as mnth_end_date
                from edw_vw_os_time_dim
                GROUP BY mnth_id
            ) jj_cal ON dsr.jj_mnth_id = jj_cal.mnth_id
            LEFT JOIN (
                select dstrbtr_id,
                    dstrbtr_name
                from (
                        select dstrbtr_id,
                            dstrbtr_name,
                            row_number() over (
                                partition by dstrbtr_id
                                order by crtd_dttm desc
                            ) as rn
                        from itg_vn_dms_distributor_dim
                    )
                where rn = 1
            ) dstrb ON dstrb.dstrbtr_id = dsr.dstrbtr_grp_cd
            LEFT JOIN (
                select year || mnth as mnth_id,
                    ex_rt
                from (
                        select fisc_yr_per,
                            SUBSTRING(cast(fisc_yr_per as VARCHAR), 1, 4) as YEAR,
                            SUBSTRING(cast(fisc_yr_per as VARCHAR), 6, 2) as mnth,
                            ex_rt
                        from edw_crncy_exch_rates
                        where from_crncy = 'VND'
                            AND to_crncy = 'USD'
                    )
            ) exch_rate ON cast (dsr.jj_mnth_id as NUMERIC) = cast (exch_rate.mnth_id as NUMERIC)
    ) )
select * from final