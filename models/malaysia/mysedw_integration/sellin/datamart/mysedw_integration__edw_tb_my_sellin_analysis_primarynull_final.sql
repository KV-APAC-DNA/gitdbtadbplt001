{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}
--Import CTEs
with
edw_tb_my_sellin_analysis_primarynull_intermediate as (
    select * from {{ ref('mysedw_integration__edw_tb_my_sellin_analysis_primarynull_intermediate') }}
),
edw_vw_my_customer_dim as (
    select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
edw_vw_my_material_dim as (
    select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
),
itg_my_customer_dim as (
    select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
itg_my_ciw_map as (
    select * from {{ ref('mysitg_integration__itg_my_ciw_map') }}
),
itg_my_material_dim as (
    select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),

--Logical CTEs
ds_primary_null as (
        SELECT 'Primary_null' AS data_src,
                            NULL AS jj_year,
                            NULL AS jj_qtr,
                            NULL AS jj_mnth_id,
                            NULL AS jj_mnth_no,
                            veocd.sap_cntry_cd,
                            'Malaysia' AS cntry_nm,
                            veossf.acct_no,
                            imciwd.acct_desc,
                            veocd.sap_state_cd,
                            (
                                ltrim(
                                    (veocd.sap_cust_id)::text,
                                    ('0'::character varying)::text
                                )
                            )::character varying AS sold_to,
                            veocd.sap_cust_nm AS sold_to_nm,
                            veocd.sap_sls_org,
                            veocd.sap_cmp_id,
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
                            imcd.cust_nm AS loc_cust_nm,
                            imcd.dstrbtr_grp_cd,
                            imcd.dstrbtr_grp_nm,
                            imcd.ullage,
                            imcd.chnl,
                            imcd.territory,
                            imcd.retail_env,
                            imcd.rdd_ind,
                            veomd.sap_matl_num AS matl_num,
                            veomd.sap_mat_desc AS matl_desc,
                            immd.item_desc AS matl_desc2,
                            immd.item_desc2 AS matl_desc3,
                            immd.frnchse_desc,
                            immd.brnd_desc,
                            immd.vrnt_desc,
                            veomd.ean_num AS sku_ean_num,
                            veomd.gph_region AS global_mat_region,
                            veomd.gph_prod_frnchse AS global_prod_franchise,
                            veomd.gph_prod_brnd AS global_prod_brand,
                            veomd.gph_prod_vrnt AS global_prod_variant,
                            veomd.gph_prod_put_up_cd AS global_prod_put_up_cd,
                            veomd.gph_prod_put_up_desc AS global_put_up_desc,
                            immd.putup_desc AS put_up_desc,
                            veomd.sap_prod_mnr_cd,
                            veomd.sap_prod_mnr_desc,
                            veomd.sap_prod_hier_cd,
                            veomd.sap_prod_hier_desc,
                            veomd.gph_prod_sub_brnd AS global_prod_sub_brand,
                            veomd.gph_prod_needstate AS global_prod_need_state,
                            veomd.gph_prod_ctgry AS global_prod_category,
                            veomd.gph_prod_subctgry AS global_prod_subcategory,
                            veomd.gph_prod_sgmnt AS global_prod_segment,
                            veomd.gph_prod_subsgmnt AS global_prod_subsegment,
                            veomd.gph_prod_size AS global_prod_size,
                            veomd.gph_prod_size_uom AS global_prod_size_uom,
                            veomd.launch_dt AS sku_launch_dt,
                            veomd.qty_shipper_pc,
                            veomd.prft_ctr,
                            veomd.shlf_life AS shelf_life,
                            immd.npi_ind,
                            immd.npi_strt_period,
                            immd.hero_ind,
                            imciwd.ciw_ctgry,
                            imciwd.ciw_buckt1,
                            imciwd.ciw_buckt2,
                            imciwd.bravo_desc1,
                            imciwd.bravo_desc2,
                            imciwd.acct_type,
                            imciwd.acct_type1,
                            from_crncy,
                            to_crncy,
                            0 AS exch_rate,
                            0 AS base_val,
                            0 AS sls_qty,
                            0 AS ret_qty,
                            0 AS sls_less_rtn_qty,
                            0 AS gts_val,
                            0 AS ret_val,
                            0 AS gts_less_rtn_val,
                            0 AS trdng_term_val,
                            0 AS tp_val,
                            0 AS trde_prmtn_val,
                            0 AS nts_val,
                            0 AS nts_qty,
                            NULL AS po_num,
                            NULL AS sls_doc_num,
                            NULL AS sls_doc_item,
                            NULL AS sls_doc_type,
                            NULL AS bill_num,
                            NULL AS bill_item,
                            NULL AS doc_creation_dt,
                            NULL AS order_status,
                            NULL AS item_status,
                            NULL AS rejectn_st,
                            NULL AS rejectn_cd,
                            NULL AS rejectn_desc,
                            0 AS ord_qty,
                            0 AS ord_net_price,
                            0 AS ord_grs_trd_sls,
                            0 AS ord_subtotal_2,
                            0 AS ord_subtotal_3,
                            0 AS ord_subtotal_4,
                            0 AS ord_net_amt,
                            0 AS ord_est_nts,
                            0 AS missed_val,
                            0 AS bill_qty_pc,
                            0 AS bill_grs_trd_sls,
                            0 AS bill_subtotal_2,
                            0 AS bill_subtotal_3,
                            0 AS bill_subtotal_4,
                            0 AS bill_net_amt,
                            0 AS bill_est_nts,
                            0 AS bill_net_val,
                            0 AS bill_gross_val,
                            NULL AS trgt_type,
                            NULL AS trgt_val_type,
                            0 AS trgt_val,
                            0 AS accrual_val,
                            0 AS le_trgt_val_wk1,
                            0 AS le_trgt_val_wk2,
                            0 AS le_trgt_val_wk3,
                            0 AS le_trgt_val_wk4,
                            0 AS le_trgt_val_wk5,
                            0 AS curr_prd_elapsed,
                            NULL AS elapsed_flag,
                            veossf.is_curr,
                            NULL AS afgr_num,
                            NULL AS cust_dn_num,
                            NULL AS rtn_ord_num,
                            NULL AS afgr_bill_num,
                            0 AS dn_amt_exc_gst_val,
                            0 AS afgr_amt,
                            0 AS rtn_ord_amt,
                            0 AS cn_amt,
                            NULL AS afgr_status,
                            0 AS afgr_val,
                            0 AS afgr_cn_diff,
                            cur_period_sgt
                        FROM 
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                   edw_tb_my_sellin_analysis_primarynull_intermediate     
                                                ) veossf
                                                LEFT JOIN (
                                                    SELECT EDW_VW_my_CUSTOMER_DIM.sap_cust_id,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_cust_nm,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_sls_org,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_cmp_id,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_cntry_cd,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_cntry_nm,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_addr,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_region,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_state_cd,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_city,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_post_cd,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_chnl_cd,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_chnl_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_sls_office_cd,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_sls_office_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_sls_grp_cd,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_sls_grp_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_curr_cd,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_prnt_cust_key,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_prnt_cust_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_cust_chnl_key,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_cust_chnl_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_cust_sub_chnl_key,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_sub_chnl_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_go_to_mdl_key,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_go_to_mdl_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_bnr_key,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_bnr_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_bnr_frmt_key,
                                                        EDW_VW_my_CUSTOMER_DIM.sap_bnr_frmt_desc,
                                                        EDW_VW_my_CUSTOMER_DIM.retail_env,
                                                        EDW_VW_my_CUSTOMER_DIM.gch_region,
                                                        EDW_VW_my_CUSTOMER_DIM.gch_cluster,
                                                        EDW_VW_my_CUSTOMER_DIM.gch_subcluster,
                                                        EDW_VW_my_CUSTOMER_DIM.gch_market,
                                                        EDW_VW_my_CUSTOMER_DIM.gch_retail_banner
                                                    FROM EDW_VW_my_CUSTOMER_DIM
                                                    WHERE (
                                                            (EDW_VW_my_CUSTOMER_DIM.sap_cntry_cd)::text = ('MY'::character varying)::text
                                                        )
                                                ) veocd ON (
                                                    (
                                                        ltrim(
                                                            (veocd.sap_cust_id)::text,
                                                            ((0)::character varying)::text
                                                        ) = trim(veossf.cust_id)
                                                    )
                                                )
                                            )
                                            LEFT JOIN (
                                                SELECT EDW_VW_my_MATERIAL_DIM.cntry_key,
                                                    EDW_VW_my_MATERIAL_DIM.sap_matl_num,
                                                    EDW_VW_my_MATERIAL_DIM.sap_mat_desc,
                                                    EDW_VW_my_MATERIAL_DIM.ean_num,
                                                    EDW_VW_my_MATERIAL_DIM.sap_mat_type_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_mat_type_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_base_uom_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prchse_uom_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_sgmt_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_sgmt_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_base_prod_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_base_prod_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_mega_brnd_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_mega_brnd_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_brnd_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_brnd_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_vrnt_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_vrnt_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_put_up_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_put_up_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_grp_frnchse_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_grp_frnchse_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_frnchse_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_frnchse_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_frnchse_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_frnchse_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_mjr_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_mjr_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_mnr_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_mnr_desc,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_hier_cd,
                                                    EDW_VW_my_MATERIAL_DIM.sap_prod_hier_desc,
                                                    EDW_VW_my_MATERIAL_DIM.gph_region,
                                                    EDW_VW_my_MATERIAL_DIM.gph_reg_frnchse,
                                                    EDW_VW_my_MATERIAL_DIM.gph_reg_frnchse_grp,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_frnchse,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_brnd,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_sub_brnd,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_vrnt,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_needstate,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_ctgry,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_subctgry,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_sgmnt,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_subsgmnt,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_put_up_cd,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_put_up_desc,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_size,
                                                    EDW_VW_my_MATERIAL_DIM.gph_prod_size_uom,
                                                    EDW_VW_my_MATERIAL_DIM.launch_dt,
                                                    EDW_VW_my_MATERIAL_DIM.qty_shipper_pc,
                                                    EDW_VW_my_MATERIAL_DIM.prft_ctr,
                                                    EDW_VW_my_MATERIAL_DIM.shlf_life
                                                FROM EDW_VW_my_MATERIAL_DIM
                                                WHERE (
                                                        (EDW_VW_my_MATERIAL_DIM.cntry_key)::text = ('MY'::character varying)::text
                                                    )
                                            ) veomd ON (
                                                (
                                                    trim((veomd.sap_matl_num)::text) = trim(veossf.item_cd)
                                                )
                                            )
                                        )
                                        LEFT JOIN itg_my_customer_dim imcd ON (
                                            (
                                                trim((imcd.cust_id)::text) = trim(veossf.cust_id)
                                            )
                                        )
                                    )
                                    LEFT JOIN itg_my_ciw_map imciwd ON (
                                        (
                                            trim(((imciwd.acct_num)::character varying)::text) = trim((veossf.acct_no)::text)
                                        )
                                    )
                                )
                                LEFT JOIN itg_my_material_dim immd ON (
                                    (
                                        trim((immd.item_cd)::text) = trim(veossf.item_cd)
                                    )
                                )
                            )

)
    
    
select * from ds_primary_null