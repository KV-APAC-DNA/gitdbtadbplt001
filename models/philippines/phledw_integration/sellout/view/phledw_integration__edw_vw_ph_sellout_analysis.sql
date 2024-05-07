with edw_vw_ph_sellout_sales_fact as (
    select * from {{ ref('phledw_integration__edw_vw_ph_sellout_sales_fact') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_vw_ph_dstrbtr_customer_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_dstrbtr_customer_dim') }}
),
edw_vw_ph_dstrbtr_material_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_dstrbtr_material_dim') }}
),
itg_mds_ph_distributor_supervisors as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_distributor_supervisors') }}
),
itg_mds_ph_gt_customer as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_gt_customer') }}
),
edw_vw_ph_material_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
),
itg_mds_ph_lav_product as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
itg_mds_ph_pos_pricelist as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
itg_mds_ph_ref_rka_master as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_rka_master') }}
),
edw_mv_ph_customer_dim as (
    select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
edw_vw_ph_customer_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_customer_dim') }}
),
veosf as
(
    SELECT
        dstrbtr_matl.dstrbtr_grp_cd,
        dstrbtr_matl.cust_cd,
        dstrbtr_matl.sup_id,
        dstrbtr_matl.sup_nm,
        dstrbtr_matl.sls_rep_id,
        dstrbtr_matl.sls_rep_nm,
        dstrbtr_matl.sales_team,
        dstrbtr_matl.cust_nm,
        dstrbtr_matl.sap_soldto_code,
        dstrbtr_matl.sap_matl_num,
        (dstrbtr_matl.dstrbtr_matl_num)::character varying AS dstrbtr_matl_num,
        dstrbtr_matl.chnl_cd,
        dstrbtr_matl.chnl_desc,
        dstrbtr_matl.sub_chnl_cd,
        dstrbtr_matl.sub_chnl_desc,
        dstrbtr_matl.region_cd,
        dstrbtr_matl.region_nm,
        dstrbtr_matl.province_cd,
        dstrbtr_matl.province_nm,
        dstrbtr_matl.town_cd,
        dstrbtr_matl.town_nm,
        dstrbtr_matl.str_prioritization,
        dstrbtr_matl.rka_cd,
        dstrbtr_matl.rka_nm,
        dstrbtr_matl.is_npi,
        dstrbtr_matl.npi_str_period,
        dstrbtr_matl.npi_end_period,
        dstrbtr_matl.is_reg,
        dstrbtr_matl.is_promo,
        dstrbtr_matl.promo_strt_period,
        dstrbtr_matl.promo_end_period,
        dstrbtr_matl.is_mcl,
        dstrbtr_matl.is_hero,
        dstrbtr_matl.bill_date,
        dstrbtr_matl.bill_doc,
        dstrbtr_matl.slsmn_cd,
        dstrbtr_matl.sls_qty,
        dstrbtr_matl.ret_qty,
        dstrbtr_matl.uom,
        dstrbtr_matl.sls_qty_pc,
        dstrbtr_matl.ret_qty_pc,
        dstrbtr_matl.grs_trd_sls,
        dstrbtr_matl.ret_val,
        dstrbtr_matl.trd_discnt,
        dstrbtr_matl.trd_sls,
        dstrbtr_matl.net_trd_sls,
        dstrbtr_matl.jj_grs_trd_sls,
        dstrbtr_matl.jj_ret_val,
        dstrbtr_matl.jj_trd_sls,
        dstrbtr_matl.jj_net_trd_sls,
        dstrbtr_matl.sku_desc,
        dstrbtr_matl.sap_mat_type_cd,
        dstrbtr_matl.sap_mat_type_desc,
        dstrbtr_matl.sap_base_uom_cd,
        dstrbtr_matl.sap_prchse_uom_cd,
        dstrbtr_matl.sap_prod_sgmt_cd,
        dstrbtr_matl.sap_prod_sgmt_desc,
        dstrbtr_matl.sap_base_prod_cd,
        dstrbtr_matl.sap_base_prod_desc,
        dstrbtr_matl.sap_mega_brnd_cd,
        dstrbtr_matl.sap_mega_brnd_desc,
        dstrbtr_matl.sap_brnd_cd,
        dstrbtr_matl.sap_brnd_desc,
        dstrbtr_matl.sap_vrnt_cd,
        dstrbtr_matl.sap_vrnt_desc,
        dstrbtr_matl.sap_put_up_cd,
        dstrbtr_matl.sap_put_up_desc,
        dstrbtr_matl.sap_grp_frnchse_cd,
        dstrbtr_matl.sap_grp_frnchse_desc,
        dstrbtr_matl.sap_frnchse_cd,
        dstrbtr_matl.sap_frnchse_desc,
        dstrbtr_matl.sap_prod_frnchse_cd,
        dstrbtr_matl.sap_prod_frnchse_desc,
        dstrbtr_matl.sap_prod_mjr_cd,
        dstrbtr_matl.sap_prod_mjr_desc,
        dstrbtr_matl.sap_prod_mnr_cd,
        dstrbtr_matl.sap_prod_mnr_desc,
        dstrbtr_matl.sap_prod_hier_cd,
        dstrbtr_matl.sap_prod_hier_desc,
        dstrbtr_matl.global_mat_region,
        dstrbtr_matl.global_prod_franchise,
        dstrbtr_matl.global_prod_brand,
        dstrbtr_matl.global_prod_variant,
        dstrbtr_matl.global_prod_put_up_cd,
        dstrbtr_matl.global_put_up_desc,
        dstrbtr_matl.global_prod_sub_brand,
        dstrbtr_matl.global_prod_need_state,
        dstrbtr_matl.global_prod_category,
        dstrbtr_matl.global_prod_subcategory,
        dstrbtr_matl.global_prod_segment,
        dstrbtr_matl.global_prod_subsegment,
        dstrbtr_matl.global_prod_size,
        dstrbtr_matl.global_prod_size_uom
    FROM
        (
            SELECT veosf1.dstrbtr_grp_cd,
                veosf1.cust_cd,
                veosf1.sup_id,
                veosf1.sup_nm,
                veosf1.sls_rep_id,
                veosf1.sls_rep_nm,
                veosf1.sales_team,
                veosf1.cust_nm,
                veosf1.sap_soldto_code,
                veosf1.sap_matl_num,
                veosf1.dstrbtr_matl_num,
                veosf1.chnl_cd,
                veosf1.chnl_desc,
                veosf1.sub_chnl_cd,
                veosf1.sub_chnl_desc,
                veosf1.region_cd,
                veosf1.region_nm,
                veosf1.province_cd,
                veosf1.province_nm,
                veosf1.town_cd,
                veosf1.town_nm,
                veosf1.str_prioritization,
                veosf1.rka_cd,
                veosf1.rka_nm,
                veosf1.is_npi,
                veosf1.npi_str_period,
                veosf1.npi_end_period,
                veosf1.is_reg,
                veosf1.is_promo,
                veosf1.promo_strt_period,
                veosf1.promo_end_period,
                veosf1.is_mcl,
                veosf1.is_hero,
                veosf1.bill_date,
                veosf1.bill_doc,
                veosf1.slsmn_cd,
                veosf1.sls_qty,
                veosf1.ret_qty,
                veosf1.uom,
                veosf1.sls_qty_pc,
                veosf1.ret_qty_pc,
                veosf1.grs_trd_sls,
                veosf1.ret_val,
                veosf1.trd_discnt,
                veosf1.trd_sls,
                veosf1.net_trd_sls,
                veosf1.jj_grs_trd_sls,
                veosf1.jj_ret_val,
                veosf1.jj_trd_sls,
                veosf1.jj_net_trd_sls,
                veomd.sap_mat_desc AS sku_desc,
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
                veomd.gph_region AS global_mat_region,
                veomd.gph_prod_frnchse AS global_prod_franchise,
                veomd.gph_prod_brnd AS global_prod_brand,
                veomd.gph_prod_vrnt AS global_prod_variant,
                veomd.gph_prod_put_up_cd AS global_prod_put_up_cd,
                veomd.gph_prod_put_up_desc AS global_put_up_desc,
                veomd.gph_prod_sub_brnd AS global_prod_sub_brand,
                veomd.gph_prod_needstate AS global_prod_need_state,
                veomd.gph_prod_ctgry AS global_prod_category,
                veomd.gph_prod_subctgry AS global_prod_subcategory,
                veomd.gph_prod_sgmnt AS global_prod_segment,
                veomd.gph_prod_subsgmnt AS global_prod_subsegment,
                veomd.gph_prod_size AS global_prod_size,
                veomd.gph_prod_size_uom AS global_prod_size_uom
            FROM
                (
                    (
                        SELECT a.dstrbtr_grp_cd,
                            a.cust_cd,
                            esp.sup_id,
                            esp.sup_nm,
                            a.slsmn_cd AS sls_rep_id,
                            a.slsmn_nm AS sls_rep_nm,
                            esp.slsgrpnm AS sales_team,
                            veodcd2.cust_nm,
                            veodcd.sap_soldto_code,
                            veodmd.sap_matl_num,
                            upper(trim((a.dstrbtr_matl_num)::text)) AS dstrbtr_matl_num,
                            veodcd2.chnl_cd,
                            veodcd2.chnl_desc,
                            veodcd2.sub_chnl_cd,
                            veodcd2.sub_chnl_desc,
                            impgc.rep_grp2 AS region_cd,
                            impgc.rep_grp2_desc AS region_nm,
                            impgc.rep_grp3 AS province_cd,
                            impgc.rep_grp3_desc AS province_nm,
                            impgc.rep_grp5 AS town_cd,
                            impgc.rep_grp5_desc AS town_nm,
                            impgc.store_prioritization AS str_prioritization,
                            impgc.sls_dist AS rka_cd,
                            rka.rka_nm,
                            veodmd.is_npi,
                            veodmd.npi_str_period,
                            veodmd.npi_end_period,
                            veodmd.is_reg,
                            veodmd.is_promo,
                            veodmd.promo_strt_period,
                            veodmd.promo_end_period,
                            veodmd.is_mcl,
                            veodmd.is_hero,
                            a.bill_date,
                            a.bill_doc,
                            a.slsmn_cd,
                            a.sls_qty,
                            a.ret_qty,
                            a.uom,
                            a.sls_qty_pc,
                            a.ret_qty_pc,
                            a.grs_trd_sls,
                            a.ret_val,
                            a.trd_discnt,
                            a.trd_sls,
                            a.net_trd_sls,
                            a.jj_grs_trd_sls,
                            a.jj_ret_val,
                            a.jj_trd_sls,
                            a.jj_net_trd_sls
                        FROM
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    SELECT fact.cntry_cd,
                                                        fact.cntry_nm,
                                                        fact.dstrbtr_grp_cd,
                                                        fact.dstrbtr_soldto_code,
                                                        fact.cust_cd,
                                                        fact.dstrbtr_matl_num,
                                                        fact.sap_matl_num,
                                                        fact.bar_cd,
                                                        fact.bill_date,
                                                        fact.bill_doc,
                                                        fact.slsmn_cd,
                                                        fact.slsmn_nm,
                                                        fact.wh_id,
                                                        fact.doc_type,
                                                        fact.doc_type_desc,
                                                        fact.base_sls,
                                                        fact.sls_qty,
                                                        fact.ret_qty,
                                                        fact.uom,
                                                        fact.sls_qty_pc,
                                                        fact.ret_qty_pc,
                                                        fact.grs_trd_sls,
                                                        fact.ret_val,
                                                        fact.trd_discnt,
                                                        fact.trd_discnt_item_lvl,
                                                        fact.trd_discnt_bill_lvl,
                                                        fact.trd_sls,
                                                        fact.net_trd_sls,
                                                        fact.cn_reason_cd,
                                                        fact.cn_reason_desc,
                                                        fact.jj_grs_trd_sls,
                                                        fact.jj_ret_val,
                                                        fact.jj_trd_sls,
                                                        fact.jj_net_trd_sls,
                                                        cal.sales_cycle
                                                    FROM
                                                        (
                                                            edw_vw_ph_sellout_sales_fact fact
                                                            LEFT JOIN
                                                            (
                                                                SELECT edw_calendar_dim.cal_day,
                                                                    edw_calendar_dim.fisc_yr_vrnt,
                                                                    edw_calendar_dim.wkday,
                                                                    edw_calendar_dim.cal_wk,
                                                                    edw_calendar_dim.cal_mo_1,
                                                                    edw_calendar_dim.cal_mo_2,
                                                                    edw_calendar_dim.cal_qtr_1,
                                                                    edw_calendar_dim.cal_qtr_2,
                                                                    edw_calendar_dim.half_yr,
                                                                    edw_calendar_dim.cal_yr,
                                                                    edw_calendar_dim.fisc_per,
                                                                    edw_calendar_dim.pstng_per,
                                                                    edw_calendar_dim.fisc_yr,
                                                                    edw_calendar_dim.rec_mode,
                                                                    edw_calendar_dim.crt_dttm,
                                                                    edw_calendar_dim.updt_dttm,
                                                                    (
                                                                        "left"(
                                                                            ((edw_calendar_dim.fisc_per)::character varying)::text,
                                                                            4
                                                                        ) || "right"(
                                                                            ((edw_calendar_dim.fisc_per)::character varying)::text,
                                                                            2
                                                                        )
                                                                    ) AS sales_cycle
                                                                FROM edw_calendar_dim
                                                            ) cal ON ((to_date(fact.bill_date) = cal.cal_day))
                                                        )
                                                ) a
                                                LEFT JOIN
                                                (
                                                    SELECT DISTINCT edw_vw_ph_dstrbtr_customer_dim.dstrbtr_grp_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.sap_soldto_code
                                                    FROM edw_vw_ph_dstrbtr_customer_dim
                                                    WHERE (
                                                            (edw_vw_ph_dstrbtr_customer_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                        )
                                                ) veodcd ON (veodcd.dstrbtr_grp_cd)::text = (a.dstrbtr_grp_cd)::text
                                            )
                                            LEFT JOIN
                                            (
                                                SELECT
                                                    edw_vw_ph_dstrbtr_material_dim.dstrbtr_matl_num,
                                                    edw_vw_ph_dstrbtr_material_dim.dstrbtr_grp_cd,
                                                    edw_vw_ph_dstrbtr_material_dim.sap_soldto_code,
                                                    edw_vw_ph_dstrbtr_material_dim.sap_matl_num,
                                                    edw_vw_ph_dstrbtr_material_dim.is_npi,
                                                    edw_vw_ph_dstrbtr_material_dim.npi_str_period,
                                                    edw_vw_ph_dstrbtr_material_dim.npi_end_period,
                                                    edw_vw_ph_dstrbtr_material_dim.is_reg,
                                                    edw_vw_ph_dstrbtr_material_dim.is_promo,
                                                    edw_vw_ph_dstrbtr_material_dim.promo_strt_period,
                                                    edw_vw_ph_dstrbtr_material_dim.promo_end_period,
                                                    edw_vw_ph_dstrbtr_material_dim.is_mcl,
                                                    edw_vw_ph_dstrbtr_material_dim.is_hero
                                                FROM edw_vw_ph_dstrbtr_material_dim
                                                WHERE (
                                                        (edw_vw_ph_dstrbtr_material_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                    )
                                            ) veodmd ON
                                            (
                                                (
                                                    (
                                                        (veodmd.dstrbtr_grp_cd)::text = (a.dstrbtr_grp_cd)::text
                                                    )
                                                    AND (
                                                        upper(trim((veodmd.dstrbtr_matl_num)::text)) = (a.dstrbtr_matl_num)::text
                                                    )
                                                )
                                            )
                                        )
                                        LEFT JOIN
                                        (
                                            SELECT itg_mds_ph_distributor_supervisors.distcode,
                                                itg_mds_ph_distributor_supervisors.slsspid AS sup_id,
                                                itg_mds_ph_distributor_supervisors.slsspnm AS sup_nm,
                                                itg_mds_ph_distributor_supervisors.salescycle,
                                                itg_mds_ph_distributor_supervisors.slsid,
                                                itg_mds_ph_distributor_supervisors.slsgrpnm
                                            FROM itg_mds_ph_distributor_supervisors
                                            GROUP BY itg_mds_ph_distributor_supervisors.distcode,
                                                itg_mds_ph_distributor_supervisors.slsspid,
                                                itg_mds_ph_distributor_supervisors.slsspnm,
                                                itg_mds_ph_distributor_supervisors.salescycle,
                                                itg_mds_ph_distributor_supervisors.slsid,
                                                itg_mds_ph_distributor_supervisors.slsgrpnm
                                        ) esp ON
                                        (
                                            (
                                                (
                                                    ((a.dstrbtr_grp_cd)::text = (esp.distcode)::text)
                                                    AND (a.sales_cycle = (esp.salescycle)::text)
                                                )
                                                AND ((a.slsmn_cd)::text = (esp.slsid)::text)
                                            )
                                        )
                                    )
                                    LEFT JOIN
                                    (
                                        SELECT edw_vw_ph_dstrbtr_customer_dim.cntry_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.cntry_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.dstrbtr_grp_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.dstrbtr_soldto_code,
                                            edw_vw_ph_dstrbtr_customer_dim.sap_soldto_code,
                                            edw_vw_ph_dstrbtr_customer_dim.cust_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.cust_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.alt_cust_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.alt_cust_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.addr,
                                            edw_vw_ph_dstrbtr_customer_dim.area_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.area_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.state_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.state_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.region_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.region_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.prov_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.prov_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.town_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.town_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.city_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.city_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.post_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.post_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.slsmn_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.slsmn_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.chnl_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.chnl_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.sub_chnl_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.sub_chnl_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.chnl_attr1_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.chnl_attr1_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.chnl_attr2_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.chnl_attr2_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.outlet_type_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.outlet_type_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.cust_grp_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.cust_grp_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr1_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr1_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr2_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr2_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.sls_dstrct_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.sls_dstrct_nm,
                                            edw_vw_ph_dstrbtr_customer_dim.sls_office_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.sls_office_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.sls_grp_cd,
                                            edw_vw_ph_dstrbtr_customer_dim.sls_grp_desc,
                                            edw_vw_ph_dstrbtr_customer_dim.status
                                        FROM edw_vw_ph_dstrbtr_customer_dim
                                        WHERE (
                                                (edw_vw_ph_dstrbtr_customer_dim.cntry_cd)::text = ('PH'::character varying)::text
                                            )
                                    ) veodcd2 ON (((veodcd2.cust_cd)::text = (a.cust_cd)::text))
                                )
                                LEFT JOIN
                                (
                                    (
                                        SELECT itg_mds_ph_gt_customer.dstrbtr_cust_id,
                                            itg_mds_ph_gt_customer.dstrbtr_cust_nm,
                                            itg_mds_ph_gt_customer.slsmn,
                                            itg_mds_ph_gt_customer.slsmn_desc,
                                            itg_mds_ph_gt_customer.rep_grp2,
                                            itg_mds_ph_gt_customer.rep_grp2_desc,
                                            itg_mds_ph_gt_customer.rep_grp3,
                                            itg_mds_ph_gt_customer.rep_grp3_desc,
                                            itg_mds_ph_gt_customer.rep_grp4,
                                            itg_mds_ph_gt_customer.rep_grp4_desc,
                                            itg_mds_ph_gt_customer.rep_grp5,
                                            itg_mds_ph_gt_customer.rep_grp5_desc,
                                            itg_mds_ph_gt_customer.rep_grp6,
                                            itg_mds_ph_gt_customer.rep_grp6_desc,
                                            itg_mds_ph_gt_customer.status,
                                            itg_mds_ph_gt_customer.address,
                                            itg_mds_ph_gt_customer.zip,
                                            itg_mds_ph_gt_customer.slm_grp_cd,
                                            itg_mds_ph_gt_customer.frequency_visit,
                                            itg_mds_ph_gt_customer.store_prioritization,
                                            itg_mds_ph_gt_customer.latitude,
                                            itg_mds_ph_gt_customer.longitude,
                                            itg_mds_ph_gt_customer.rpt_grp9,
                                            itg_mds_ph_gt_customer.rpt_grp9_desc,
                                            itg_mds_ph_gt_customer.rpt_grp11,
                                            itg_mds_ph_gt_customer.rpt_grp11_desc,
                                            itg_mds_ph_gt_customer.sls_dist,
                                            itg_mds_ph_gt_customer.sls_dist_desc,
                                            itg_mds_ph_gt_customer.dstrbtr_grp_cd,
                                            itg_mds_ph_gt_customer.dstrbtr_grp_nm,
                                            itg_mds_ph_gt_customer.rpt_grp_15_desc,
                                            itg_mds_ph_gt_customer.last_chg_datetime,
                                            itg_mds_ph_gt_customer.effective_from,
                                            itg_mds_ph_gt_customer.effective_to,
                                            itg_mds_ph_gt_customer.active,
                                            itg_mds_ph_gt_customer.crtd_dttm,
                                            itg_mds_ph_gt_customer.updt_dttm,
                                            itg_mds_ph_gt_customer.zip_code,
                                            itg_mds_ph_gt_customer.zip_cd_name,
                                            itg_mds_ph_gt_customer.barangay_code,
                                            itg_mds_ph_gt_customer.barangay_cd_name,
                                            itg_mds_ph_gt_customer.long_lat_source
                                        FROM itg_mds_ph_gt_customer
                                        WHERE (
                                                (itg_mds_ph_gt_customer.active)::text = ('Y'::character varying)::text
                                            )
                                    ) impgc
                                    LEFT JOIN
                                    (
                                        SELECT itg_mds_ph_ref_rka_master.rka_cd,
                                            itg_mds_ph_ref_rka_master.rka_nm,
                                            itg_mds_ph_ref_rka_master.last_chg_datetime,
                                            itg_mds_ph_ref_rka_master.effective_from,
                                            itg_mds_ph_ref_rka_master.effective_to,
                                            itg_mds_ph_ref_rka_master.active,
                                            itg_mds_ph_ref_rka_master.crtd_dttm,
                                            itg_mds_ph_ref_rka_master.updt_dttm
                                        FROM itg_mds_ph_ref_rka_master
                                        WHERE (
                                                (itg_mds_ph_ref_rka_master.active)::text = ('Y'::character varying)::text
                                            )
                                    ) rka ON
                                    (
                                        (
                                            ltrim((impgc.sls_dist)::text) = ltrim((rka.rka_cd)::text)
                                        )
                                    )
                                ) ON
                                (
                                    (
                                        ltrim(
                                            (impgc.dstrbtr_cust_id)::text,
                                            ('0'::character varying)::text
                                        ) = ltrim(
                                            (a.cust_cd)::text,
                                            ('0'::character varying)::text
                                        )
                                    )
                                )
                            )
                        WHERE
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    a.sls_qty <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                                                                )
                                                                OR (
                                                                    a.ret_qty <> (((0)::numeric)::numeric(18, 0))::numeric(24, 6)
                                                                )
                                                            )
                                                            OR (
                                                                a.sls_qty_pc <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                                                            )
                                                        )
                                                        OR (
                                                            a.ret_qty_pc <> (((0)::numeric)::numeric(18, 0))::numeric(24, 6)
                                                        )
                                                    )
                                                    OR (
                                                        a.grs_trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(38, 12)
                                                    )
                                                )
                                                OR (
                                                    a.ret_val <> (((0)::numeric)::numeric(18, 0))::numeric(38, 12)
                                                )
                                            )
                                            OR (
                                                a.trd_discnt <> (((0)::numeric)::numeric(18, 0))::numeric(23, 6)
                                            )
                                        )
                                        OR (
                                            a.trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(20, 4)
                                        )
                                    )
                                    OR (
                                        a.net_trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                                    )
                                )
                                AND (
                                    (a.cntry_cd)::text = ('PH'::character varying)::text
                                )
                            )
                    ) veosf1
                    LEFT JOIN
                    (
                        SELECT edw_vw_ph_material_dim.cntry_key,
                            edw_vw_ph_material_dim.sap_matl_num,
                            edw_vw_ph_material_dim.sap_mat_desc,
                            edw_vw_ph_material_dim.ean_num,
                            edw_vw_ph_material_dim.sap_mat_type_cd,
                            edw_vw_ph_material_dim.sap_mat_type_desc,
                            edw_vw_ph_material_dim.sap_base_uom_cd,
                            edw_vw_ph_material_dim.sap_prchse_uom_cd,
                            edw_vw_ph_material_dim.sap_prod_sgmt_cd,
                            edw_vw_ph_material_dim.sap_prod_sgmt_desc,
                            edw_vw_ph_material_dim.sap_base_prod_cd,
                            edw_vw_ph_material_dim.sap_base_prod_desc,
                            edw_vw_ph_material_dim.sap_mega_brnd_cd,
                            edw_vw_ph_material_dim.sap_mega_brnd_desc,
                            edw_vw_ph_material_dim.sap_brnd_cd,
                            edw_vw_ph_material_dim.sap_brnd_desc,
                            edw_vw_ph_material_dim.sap_vrnt_cd,
                            edw_vw_ph_material_dim.sap_vrnt_desc,
                            edw_vw_ph_material_dim.sap_put_up_cd,
                            edw_vw_ph_material_dim.sap_put_up_desc,
                            edw_vw_ph_material_dim.sap_grp_frnchse_cd,
                            edw_vw_ph_material_dim.sap_grp_frnchse_desc,
                            edw_vw_ph_material_dim.sap_frnchse_cd,
                            edw_vw_ph_material_dim.sap_frnchse_desc,
                            edw_vw_ph_material_dim.sap_prod_frnchse_cd,
                            edw_vw_ph_material_dim.sap_prod_frnchse_desc,
                            edw_vw_ph_material_dim.sap_prod_mjr_cd,
                            edw_vw_ph_material_dim.sap_prod_mjr_desc,
                            edw_vw_ph_material_dim.sap_prod_mnr_cd,
                            edw_vw_ph_material_dim.sap_prod_mnr_desc,
                            edw_vw_ph_material_dim.sap_prod_hier_cd,
                            edw_vw_ph_material_dim.sap_prod_hier_desc,
                            edw_vw_ph_material_dim.gph_region,
                            edw_vw_ph_material_dim.gph_reg_frnchse,
                            edw_vw_ph_material_dim.gph_reg_frnchse_grp,
                            edw_vw_ph_material_dim.gph_prod_frnchse,
                            edw_vw_ph_material_dim.gph_prod_brnd,
                            edw_vw_ph_material_dim.gph_prod_sub_brnd,
                            edw_vw_ph_material_dim.gph_prod_vrnt,
                            edw_vw_ph_material_dim.gph_prod_needstate,
                            edw_vw_ph_material_dim.gph_prod_ctgry,
                            edw_vw_ph_material_dim.gph_prod_subctgry,
                            edw_vw_ph_material_dim.gph_prod_sgmnt,
                            edw_vw_ph_material_dim.gph_prod_subsgmnt,
                            edw_vw_ph_material_dim.gph_prod_put_up_cd,
                            edw_vw_ph_material_dim.gph_prod_put_up_desc,
                            edw_vw_ph_material_dim.gph_prod_size,
                            edw_vw_ph_material_dim.gph_prod_size_uom,
                            edw_vw_ph_material_dim.launch_dt,
                            edw_vw_ph_material_dim.qty_shipper_pc,
                            edw_vw_ph_material_dim.prft_ctr,
                            edw_vw_ph_material_dim.shlf_life
                        FROM edw_vw_ph_material_dim
                        WHERE (
                                (edw_vw_ph_material_dim.cntry_key)::text = ('PH'::character varying)::text
                            )
                    ) veomd ON
                    (
                        (
                            ltrim(
                                (veomd.sap_matl_num)::text,
                                ('0'::character varying)::text
                            ) = ltrim(
                                (veosf1.sap_matl_num)::text,
                                ('0'::character varying)::text
                            )
                        )
                    )
                )
            WHERE (
                ltrim(COALESCE(veosf1.dstrbtr_matl_num,'NA'),('0'::character varying)::text) NOT IN 
                (
                    SELECT DISTINCT ltrim((COALESCE(itg_mds_ph_lav_product.item_cd,'NA'::character varying))::text,('0'::character varying)::text) AS ltrim
                    FROM itg_mds_ph_lav_product
                    WHERE 
                        (
                            (itg_mds_ph_lav_product.active)::text = ('Y'::character varying)::text
                        )
                )
                    
                )
        ) dstrbtr_matl
    UNION ALL
    SELECT veosf1.dstrbtr_grp_cd,
        veosf1.cust_cd,
        veosf1.sup_id,
        veosf1.sup_nm,
        veosf1.sls_rep_id,
        veosf1.sls_rep_nm,
        veosf1.sales_team,
        veosf1.cust_nm,
        veosf1.sap_soldto_code,
        veosf1.sap_matl_num,
        (veosf1.dstrbtr_matl_num)::character varying AS dstrbtr_matl_num,
        veosf1.chnl_cd,
        veosf1.chnl_desc,
        veosf1.sub_chnl_cd,
        veosf1.sub_chnl_desc,
        veosf1.region_cd,
        veosf1.region_nm,
        veosf1.province_cd,
        veosf1.province_nm,
        veosf1.town_cd,
        veosf1.town_nm,
        veosf1.str_prioritization,
        veosf1.rka_cd,
        veosf1.rka_nm,
        veosf1.is_npi,
        veosf1.npi_str_period,
        veosf1.npi_end_period,
        veosf1.is_reg,
        veosf1.is_promo,
        veosf1.promo_strt_period,
        veosf1.promo_end_period,
        veosf1.is_mcl,
        (veosf1.is_hero)::character varying AS is_hero,
        veosf1.bill_date,
        veosf1.bill_doc,
        veosf1.slsmn_cd,
        veosf1.sls_qty,
        veosf1.ret_qty,
        veosf1.uom,
        veosf1.sls_qty_pc,
        veosf1.ret_qty_pc,
        veosf1.grs_trd_sls,
        veosf1.ret_val,
        veosf1.trd_discnt,
        veosf1.trd_sls,
        veosf1.net_trd_sls,
        veosf1.jj_grs_trd_sls,
        veosf1.jj_ret_val,
        veosf1.jj_trd_sls,
        veosf1.jj_net_trd_sls,
        veomd.sap_mat_desc AS sku_desc,
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
        veomd.gph_region AS global_mat_region,
        veomd.gph_prod_frnchse AS global_prod_franchise,
        veomd.gph_prod_brnd AS global_prod_brand,
        veomd.gph_prod_vrnt AS global_prod_variant,
        veomd.gph_prod_put_up_cd AS global_prod_put_up_cd,
        veomd.gph_prod_put_up_desc AS global_put_up_desc,
        veomd.gph_prod_sub_brnd AS global_prod_sub_brand,
        veomd.gph_prod_needstate AS global_prod_need_state,
        veomd.gph_prod_ctgry AS global_prod_category,
        veomd.gph_prod_subctgry AS global_prod_subcategory,
        veomd.gph_prod_sgmnt AS global_prod_segment,
        veomd.gph_prod_subsgmnt AS global_prod_subsegment,
        veomd.gph_prod_size AS global_prod_size,
        veomd.gph_prod_size_uom AS global_prod_size_uom
    FROM
        (
            (
                SELECT a.dstrbtr_grp_cd,
                    a.cust_cd,
                    esp.sup_id,
                    esp.sup_nm,
                    a.slsmn_cd AS sls_rep_id,
                    a.slsmn_nm AS sls_rep_nm,
                    esp.slsgrpnm AS sales_team,
                    veodcd2.cust_nm,
                    veodcd.sap_soldto_code,
                    a.dstrbtr_matl_num AS sap_matl_num,
                    upper(trim((a.dstrbtr_matl_num)::text)) AS dstrbtr_matl_num,
                    veodcd2.chnl_cd,
                    veodcd2.chnl_desc,
                    veodcd2.sub_chnl_cd,
                    veodcd2.sub_chnl_desc,
                    impgc.rep_grp2 AS region_cd,
                    impgc.rep_grp2_desc AS region_nm,
                    impgc.rep_grp3 AS province_cd,
                    impgc.rep_grp3_desc AS province_nm,
                    impgc.rep_grp5 AS town_cd,
                    impgc.rep_grp5_desc AS town_nm,
                    impgc.store_prioritization AS str_prioritization,
                    impgc.sls_dist AS rka_cd,
                    rka.rka_nm,
                    veodmd.is_npi,
                    veodmd.npi_str_period,
                    veodmd.npi_end_period,
                    veodmd.is_reg,
                    veodmd.is_promo,
                    veodmd.promo_strt_period,
                    veodmd.promo_end_period,
                    veodmd.is_mcl,
                    veodmd.is_hero,
                    a.bill_date,
                    a.bill_doc,
                    a.slsmn_cd,
                    a.sls_qty,
                    a.ret_qty,
                    a.uom,
                    a.sls_qty_pc,
                    a.ret_qty_pc,
                    a.grs_trd_sls,
                    a.ret_val,
                    a.trd_discnt,
                    a.trd_sls,
                    a.net_trd_sls,
                    a.jj_grs_trd_sls,
                    a.jj_ret_val,
                    a.jj_trd_sls,
                    a.jj_net_trd_sls
                FROM
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            SELECT fact.cntry_cd,
                                                fact.cntry_nm,
                                                fact.dstrbtr_grp_cd,
                                                fact.dstrbtr_soldto_code,
                                                fact.cust_cd,
                                                fact.dstrbtr_matl_num,
                                                fact.sap_matl_num,
                                                fact.bar_cd,
                                                fact.bill_date,
                                                fact.bill_doc,
                                                fact.slsmn_cd,
                                                fact.slsmn_nm,
                                                fact.wh_id,
                                                fact.doc_type,
                                                fact.doc_type_desc,
                                                fact.base_sls,
                                                fact.sls_qty,
                                                fact.ret_qty,
                                                fact.uom,
                                                fact.sls_qty_pc,
                                                fact.ret_qty_pc,
                                                fact.grs_trd_sls,
                                                fact.ret_val,
                                                fact.trd_discnt,
                                                fact.trd_discnt_item_lvl,
                                                fact.trd_discnt_bill_lvl,
                                                fact.trd_sls,
                                                fact.net_trd_sls,
                                                fact.cn_reason_cd,
                                                fact.cn_reason_desc,
                                                fact.jj_grs_trd_sls,
                                                fact.jj_ret_val,
                                                fact.jj_trd_sls,
                                                fact.jj_net_trd_sls,
                                                cal.sales_cycle
                                            FROM
                                                (
                                                    edw_vw_ph_sellout_sales_fact fact
                                                    LEFT JOIN
                                                    (
                                                        SELECT edw_calendar_dim.cal_day,
                                                            edw_calendar_dim.fisc_yr_vrnt,
                                                            edw_calendar_dim.wkday,
                                                            edw_calendar_dim.cal_wk,
                                                            edw_calendar_dim.cal_mo_1,
                                                            edw_calendar_dim.cal_mo_2,
                                                            edw_calendar_dim.cal_qtr_1,
                                                            edw_calendar_dim.cal_qtr_2,
                                                            edw_calendar_dim.half_yr,
                                                            edw_calendar_dim.cal_yr,
                                                            edw_calendar_dim.fisc_per,
                                                            edw_calendar_dim.pstng_per,
                                                            edw_calendar_dim.fisc_yr,
                                                            edw_calendar_dim.rec_mode,
                                                            edw_calendar_dim.crt_dttm,
                                                            edw_calendar_dim.updt_dttm,
                                                            (
                                                                "left"(
                                                                    ((edw_calendar_dim.fisc_per)::character varying)::text,
                                                                    4
                                                                ) + "right"(
                                                                    ((edw_calendar_dim.fisc_per)::character varying)::text,
                                                                    2
                                                                )
                                                            ) AS sales_cycle
                                                        FROM edw_calendar_dim
                                                    ) cal ON ((to_date(fact.bill_date) = cal.cal_day))
                                                )
                                        ) a
                                        LEFT JOIN
                                        (
                                            SELECT DISTINCT edw_vw_ph_dstrbtr_customer_dim.dstrbtr_grp_cd,
                                                edw_vw_ph_dstrbtr_customer_dim.sap_soldto_code
                                            FROM edw_vw_ph_dstrbtr_customer_dim
                                            WHERE (
                                                    (edw_vw_ph_dstrbtr_customer_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                )
                                        ) veodcd ON (
                                            (
                                                (veodcd.dstrbtr_grp_cd)::text = (a.dstrbtr_grp_cd)::text
                                            )
                                        )
                                    )
                                    LEFT JOIN
                                    (
                                        SELECT implp.item_cd,
                                            CASE
                                                WHEN (
                                                    (
                                                        (
                                                            (epp.status)::text = ('**'::character varying)::text
                                                        )
                                                        AND (veotd.mnth_id >= epp.launch_period)
                                                    )
                                                    AND (veotd.mnth_id <= epp.end_period)
                                                ) THEN 'Y'::character varying
                                                ELSE 'N'::character varying
                                            END AS is_npi,
                                            (epp.launch_period)::character varying(52) AS npi_str_period,
                                            (epp.end_period)::character varying(52) AS npi_end_period,
                                            CASE
                                                WHEN (
                                                    upper((implp.promo_reg_ind)::text) = ('REG'::character varying)::text
                                                ) THEN 'Y'::character varying
                                                ELSE 'N'::character varying
                                            END AS is_reg,
                                            CASE
                                                WHEN (
                                                    upper((implp.promo_reg_ind)::text) = ('PROMO'::character varying)::text
                                                ) THEN 'Y'::character varying
                                                ELSE 'N'::character varying
                                            END AS is_promo,
                                            (implp.promo_strt_period)::character varying(10) AS promo_strt_period,
                                            (implp.promo_end_period)::character varying(10) AS promo_end_period,
                                            NULL::character varying AS is_mcl,
                                            upper((implp.hero_sku_ind)::text) AS is_hero
                                        FROM
                                            (
                                                SELECT edw_vw_os_time_dim.mnth_id
                                                FROM edw_vw_os_time_dim
                                                WHERE (
                                                        edw_vw_os_time_dim.cal_date::date = to_date(
                                                            current_timestamp::timestamp without time zone
                                                        )
                                                    )
                                            ) veotd,
                                            (
                                                (
                                                    SELECT itg_mds_ph_lav_product.item_cd,
                                                        itg_mds_ph_lav_product.item_nm,
                                                        itg_mds_ph_lav_product.ims_otc_tag,
                                                        itg_mds_ph_lav_product.ims_otc_tag_nm,
                                                        itg_mds_ph_lav_product.npi_strt_period,
                                                        itg_mds_ph_lav_product.price_lst_period,
                                                        itg_mds_ph_lav_product.promo_strt_period,
                                                        itg_mds_ph_lav_product.promo_end_period,
                                                        itg_mds_ph_lav_product.promo_reg_ind,
                                                        itg_mds_ph_lav_product.promo_reg_nm,
                                                        itg_mds_ph_lav_product.hero_sku_ind,
                                                        itg_mds_ph_lav_product.hero_sku_nm,
                                                        itg_mds_ph_lav_product.rpt_grp_1,
                                                        itg_mds_ph_lav_product.rpt_grp_1_desc,
                                                        itg_mds_ph_lav_product.rpt_grp_2,
                                                        itg_mds_ph_lav_product.rpt_grp_2_desc,
                                                        itg_mds_ph_lav_product.rpt_grp_3,
                                                        itg_mds_ph_lav_product.rpt_grp_3_desc,
                                                        itg_mds_ph_lav_product.rpt_grp_4,
                                                        itg_mds_ph_lav_product.rpt_grp_4_desc,
                                                        itg_mds_ph_lav_product.rpt_grp_5,
                                                        itg_mds_ph_lav_product.rpt_grp_5_desc,
                                                        itg_mds_ph_lav_product.scard_brand_cd,
                                                        itg_mds_ph_lav_product.scard_brand_desc,
                                                        itg_mds_ph_lav_product.scard_franchise_cd,
                                                        itg_mds_ph_lav_product.scard_franchise_desc,
                                                        itg_mds_ph_lav_product.scard_put_up_cd,
                                                        itg_mds_ph_lav_product.scard_put_up_desc,
                                                        itg_mds_ph_lav_product.scard_varient_cd,
                                                        itg_mds_ph_lav_product.scard_varient_desc,
                                                        itg_mds_ph_lav_product.last_chg_datetime,
                                                        itg_mds_ph_lav_product.effective_from,
                                                        itg_mds_ph_lav_product.effective_to,
                                                        itg_mds_ph_lav_product.active,
                                                        itg_mds_ph_lav_product.crtd_dttm,
                                                        itg_mds_ph_lav_product.updt_dttm
                                                    FROM itg_mds_ph_lav_product
                                                    WHERE (
                                                            (itg_mds_ph_lav_product.active)::text = ('Y'::character varying)::text
                                                        )
                                                ) implp
                                                LEFT JOIN (
                                                    SELECT itg_mds_ph_pos_pricelist.status,
                                                        itg_mds_ph_pos_pricelist.item_cd,
                                                        min((itg_mds_ph_pos_pricelist.jj_mnth_id)::text) AS launch_period,
                                                        MIN(left(replace(add_months(to_date(JJ_MNTH_ID||'01','yyyymmdd'),11),'-',''),6)) AS END_PERIOD
                                                    FROM itg_mds_ph_pos_pricelist
                                                    WHERE (
                                                            (
                                                                (itg_mds_ph_pos_pricelist.status)::text = ('**'::character varying)::text
                                                            )
                                                            AND (
                                                                (itg_mds_ph_pos_pricelist.active)::text = ('Y'::character varying)::text
                                                            )
                                                        )
                                                    GROUP BY itg_mds_ph_pos_pricelist.status,
                                                        itg_mds_ph_pos_pricelist.item_cd
                                                ) epp ON (
                                                    (
                                                        ltrim(
                                                            (epp.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        ) = ltrim(
                                                            (implp.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        )
                                                    )
                                                )
                                            )
                                    ) veodmd ON (
                                        (
                                            ltrim((veodmd.item_cd)::text) = ltrim(
                                                (a.dstrbtr_matl_num)::text,
                                                ('0'::character varying)::text
                                            )
                                        )
                                    )
                                )
                                LEFT JOIN
                                (
                                    SELECT itg_mds_ph_distributor_supervisors.distcode,
                                        itg_mds_ph_distributor_supervisors.slsspid AS sup_id,
                                        itg_mds_ph_distributor_supervisors.slsspnm AS sup_nm,
                                        itg_mds_ph_distributor_supervisors.salescycle,
                                        itg_mds_ph_distributor_supervisors.slsid,
                                        itg_mds_ph_distributor_supervisors.slsgrpnm
                                    FROM itg_mds_ph_distributor_supervisors
                                    GROUP BY itg_mds_ph_distributor_supervisors.distcode,
                                        itg_mds_ph_distributor_supervisors.slsspid,
                                        itg_mds_ph_distributor_supervisors.slsspnm,
                                        itg_mds_ph_distributor_supervisors.salescycle,
                                        itg_mds_ph_distributor_supervisors.slsid,
                                        itg_mds_ph_distributor_supervisors.slsgrpnm
                                ) esp ON
                                (
                                    (
                                        (
                                            ((a.dstrbtr_grp_cd)::text = (esp.distcode)::text)
                                            AND (a.sales_cycle = (esp.salescycle)::text)
                                        )
                                        AND ((a.slsmn_cd)::text = (esp.slsid)::text)
                                    )
                                )
                            )
                            LEFT JOIN
                            (
                                SELECT edw_vw_ph_dstrbtr_customer_dim.cntry_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.cntry_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.dstrbtr_grp_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.dstrbtr_soldto_code,
                                    edw_vw_ph_dstrbtr_customer_dim.sap_soldto_code,
                                    edw_vw_ph_dstrbtr_customer_dim.cust_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.cust_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.alt_cust_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.alt_cust_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.addr,
                                    edw_vw_ph_dstrbtr_customer_dim.area_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.area_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.state_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.state_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.region_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.region_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.prov_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.prov_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.town_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.town_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.city_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.city_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.post_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.post_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.slsmn_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.slsmn_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.chnl_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.chnl_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.sub_chnl_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.sub_chnl_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.chnl_attr1_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.chnl_attr1_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.chnl_attr2_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.chnl_attr2_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.outlet_type_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.outlet_type_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.cust_grp_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.cust_grp_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr1_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr1_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr2_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr2_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.sls_dstrct_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.sls_dstrct_nm,
                                    edw_vw_ph_dstrbtr_customer_dim.sls_office_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.sls_office_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.sls_grp_cd,
                                    edw_vw_ph_dstrbtr_customer_dim.sls_grp_desc,
                                    edw_vw_ph_dstrbtr_customer_dim.status
                                FROM edw_vw_ph_dstrbtr_customer_dim
                                WHERE (
                                        (edw_vw_ph_dstrbtr_customer_dim.cntry_cd)::text = ('PH'::character varying)::text
                                    )
                            ) veodcd2 ON (((veodcd2.cust_cd)::text = (a.cust_cd)::text))
                        )
                        LEFT JOIN
                        (
                            (
                                SELECT itg_mds_ph_gt_customer.dstrbtr_cust_id,
                                    itg_mds_ph_gt_customer.dstrbtr_cust_nm,
                                    itg_mds_ph_gt_customer.slsmn,
                                    itg_mds_ph_gt_customer.slsmn_desc,
                                    itg_mds_ph_gt_customer.rep_grp2,
                                    itg_mds_ph_gt_customer.rep_grp2_desc,
                                    itg_mds_ph_gt_customer.rep_grp3,
                                    itg_mds_ph_gt_customer.rep_grp3_desc,
                                    itg_mds_ph_gt_customer.rep_grp4,
                                    itg_mds_ph_gt_customer.rep_grp4_desc,
                                    itg_mds_ph_gt_customer.rep_grp5,
                                    itg_mds_ph_gt_customer.rep_grp5_desc,
                                    itg_mds_ph_gt_customer.rep_grp6,
                                    itg_mds_ph_gt_customer.rep_grp6_desc,
                                    itg_mds_ph_gt_customer.status,
                                    itg_mds_ph_gt_customer.address,
                                    itg_mds_ph_gt_customer.zip,
                                    itg_mds_ph_gt_customer.slm_grp_cd,
                                    itg_mds_ph_gt_customer.frequency_visit,
                                    itg_mds_ph_gt_customer.store_prioritization,
                                    itg_mds_ph_gt_customer.latitude,
                                    itg_mds_ph_gt_customer.longitude,
                                    itg_mds_ph_gt_customer.rpt_grp9,
                                    itg_mds_ph_gt_customer.rpt_grp9_desc,
                                    itg_mds_ph_gt_customer.rpt_grp11,
                                    itg_mds_ph_gt_customer.rpt_grp11_desc,
                                    itg_mds_ph_gt_customer.sls_dist,
                                    itg_mds_ph_gt_customer.sls_dist_desc,
                                    itg_mds_ph_gt_customer.dstrbtr_grp_cd,
                                    itg_mds_ph_gt_customer.dstrbtr_grp_nm,
                                    itg_mds_ph_gt_customer.rpt_grp_15_desc,
                                    itg_mds_ph_gt_customer.last_chg_datetime,
                                    itg_mds_ph_gt_customer.effective_from,
                                    itg_mds_ph_gt_customer.effective_to,
                                    itg_mds_ph_gt_customer.active,
                                    itg_mds_ph_gt_customer.crtd_dttm,
                                    itg_mds_ph_gt_customer.updt_dttm,
                                    itg_mds_ph_gt_customer.zip_code,
                                    itg_mds_ph_gt_customer.zip_cd_name,
                                    itg_mds_ph_gt_customer.barangay_code,
                                    itg_mds_ph_gt_customer.barangay_cd_name,
                                    itg_mds_ph_gt_customer.long_lat_source
                                FROM itg_mds_ph_gt_customer
                                WHERE (
                                        (itg_mds_ph_gt_customer.active)::text = ('Y'::character varying)::text
                                    )
                            ) impgc
                            LEFT JOIN
                            (
                                SELECT itg_mds_ph_ref_rka_master.rka_cd,
                                    itg_mds_ph_ref_rka_master.rka_nm,
                                    itg_mds_ph_ref_rka_master.last_chg_datetime,
                                    itg_mds_ph_ref_rka_master.effective_from,
                                    itg_mds_ph_ref_rka_master.effective_to,
                                    itg_mds_ph_ref_rka_master.active,
                                    itg_mds_ph_ref_rka_master.crtd_dttm,
                                    itg_mds_ph_ref_rka_master.updt_dttm
                                FROM itg_mds_ph_ref_rka_master
                                WHERE (
                                        (itg_mds_ph_ref_rka_master.active)::text = ('Y'::character varying)::text
                                    )
                            ) rka ON
                            (
                                (
                                    ltrim((impgc.sls_dist)::text) = ltrim((rka.rka_cd)::text)
                                )
                            )
                        ) ON
                        (
                            (
                                ltrim(
                                    (impgc.dstrbtr_cust_id)::text,
                                    ('0'::character varying)::text
                                ) = ltrim(
                                    (a.cust_cd)::text,
                                    ('0'::character varying)::text
                                )
                            )
                        )
                    )
                WHERE
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            a.sls_qty <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                                                        )
                                                        OR (
                                                            a.ret_qty <> (((0)::numeric)::numeric(18, 0))::numeric(24, 6)
                                                        )
                                                    )
                                                    OR (
                                                        a.sls_qty_pc <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                                                    )
                                                )
                                                OR (
                                                    a.ret_qty_pc <> (((0)::numeric)::numeric(18, 0))::numeric(24, 6)
                                                )
                                            )
                                            OR (
                                                a.grs_trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(38, 12)
                                            )
                                        )
                                        OR (
                                            a.ret_val <> (((0)::numeric)::numeric(18, 0))::numeric(38, 12)
                                        )
                                    )
                                    OR (
                                        a.trd_discnt <> (((0)::numeric)::numeric(18, 0))::numeric(23, 6)
                                    )
                                )
                                OR (
                                    a.trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(20, 4)
                                )
                            )
                            OR (
                                a.net_trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                            )
                        )
                        AND (
                            (a.cntry_cd)::text = ('PH'::character varying)::text
                        )
                    )
            ) veosf1
            LEFT JOIN
            (
                SELECT edw_vw_ph_material_dim.cntry_key,
                    edw_vw_ph_material_dim.sap_matl_num,
                    edw_vw_ph_material_dim.sap_mat_desc,
                    edw_vw_ph_material_dim.ean_num,
                    edw_vw_ph_material_dim.sap_mat_type_cd,
                    edw_vw_ph_material_dim.sap_mat_type_desc,
                    edw_vw_ph_material_dim.sap_base_uom_cd,
                    edw_vw_ph_material_dim.sap_prchse_uom_cd,
                    edw_vw_ph_material_dim.sap_prod_sgmt_cd,
                    edw_vw_ph_material_dim.sap_prod_sgmt_desc,
                    edw_vw_ph_material_dim.sap_base_prod_cd,
                    edw_vw_ph_material_dim.sap_base_prod_desc,
                    edw_vw_ph_material_dim.sap_mega_brnd_cd,
                    edw_vw_ph_material_dim.sap_mega_brnd_desc,
                    edw_vw_ph_material_dim.sap_brnd_cd,
                    edw_vw_ph_material_dim.sap_brnd_desc,
                    edw_vw_ph_material_dim.sap_vrnt_cd,
                    edw_vw_ph_material_dim.sap_vrnt_desc,
                    edw_vw_ph_material_dim.sap_put_up_cd,
                    edw_vw_ph_material_dim.sap_put_up_desc,
                    edw_vw_ph_material_dim.sap_grp_frnchse_cd,
                    edw_vw_ph_material_dim.sap_grp_frnchse_desc,
                    edw_vw_ph_material_dim.sap_frnchse_cd,
                    edw_vw_ph_material_dim.sap_frnchse_desc,
                    edw_vw_ph_material_dim.sap_prod_frnchse_cd,
                    edw_vw_ph_material_dim.sap_prod_frnchse_desc,
                    edw_vw_ph_material_dim.sap_prod_mjr_cd,
                    edw_vw_ph_material_dim.sap_prod_mjr_desc,
                    edw_vw_ph_material_dim.sap_prod_mnr_cd,
                    edw_vw_ph_material_dim.sap_prod_mnr_desc,
                    edw_vw_ph_material_dim.sap_prod_hier_cd,
                    edw_vw_ph_material_dim.sap_prod_hier_desc,
                    edw_vw_ph_material_dim.gph_region,
                    edw_vw_ph_material_dim.gph_reg_frnchse,
                    edw_vw_ph_material_dim.gph_reg_frnchse_grp,
                    edw_vw_ph_material_dim.gph_prod_frnchse,
                    edw_vw_ph_material_dim.gph_prod_brnd,
                    edw_vw_ph_material_dim.gph_prod_sub_brnd,
                    edw_vw_ph_material_dim.gph_prod_vrnt,
                    edw_vw_ph_material_dim.gph_prod_needstate,
                    edw_vw_ph_material_dim.gph_prod_ctgry,
                    edw_vw_ph_material_dim.gph_prod_subctgry,
                    edw_vw_ph_material_dim.gph_prod_sgmnt,
                    edw_vw_ph_material_dim.gph_prod_subsgmnt,
                    edw_vw_ph_material_dim.gph_prod_put_up_cd,
                    edw_vw_ph_material_dim.gph_prod_put_up_desc,
                    edw_vw_ph_material_dim.gph_prod_size,
                    edw_vw_ph_material_dim.gph_prod_size_uom,
                    edw_vw_ph_material_dim.launch_dt,
                    edw_vw_ph_material_dim.qty_shipper_pc,
                    edw_vw_ph_material_dim.prft_ctr,
                    edw_vw_ph_material_dim.shlf_life
                FROM edw_vw_ph_material_dim
                WHERE (
                        (edw_vw_ph_material_dim.cntry_key)::text = ('PH'::character varying)::text
                    )
            ) veomd ON (
                (
                    ltrim(
                        (veomd.sap_matl_num)::text,
                        ('0'::character varying)::text
                    ) = ltrim(
                        veosf1.dstrbtr_matl_num,
                        ('0'::character varying)::text
                    )
                )
            )
        )
    WHERE
        (
            ltrim(COALESCE(veosf1.dstrbtr_matl_num,'NA'),('0'::character varying)::text) NOT IN 
            (
                SELECT DISTINCT ltrim(COALESCE(   dstrbtr_matl.dstrbtr_matl_num,    ('NA'::character varying)::text),('0'::character varying)::text) AS ltrim
                FROM (
                        SELECT veosf1.dstrbtr_grp_cd,
                            veosf1.cust_cd,
                            veosf1.sup_id,
                            veosf1.sup_nm,
                            veosf1.sls_rep_id,
                            veosf1.sls_rep_nm,
                            veosf1.sales_team,
                            veosf1.cust_nm,
                            veosf1.sap_soldto_code,
                            veosf1.sap_matl_num,
                            veosf1.dstrbtr_matl_num,
                            veosf1.chnl_cd,
                            veosf1.chnl_desc,
                            veosf1.sub_chnl_cd,
                            veosf1.sub_chnl_desc,
                            veosf1.region_cd,
                            veosf1.region_nm,
                            veosf1.province_cd,
                            veosf1.province_nm,
                            veosf1.town_cd,
                            veosf1.town_nm,
                            veosf1.str_prioritization,
                            veosf1.rka_cd,
                            veosf1.rka_nm,
                            veosf1.is_npi,
                            veosf1.npi_str_period,
                            veosf1.npi_end_period,
                            veosf1.is_reg,
                            veosf1.is_promo,
                            veosf1.promo_strt_period,
                            veosf1.promo_end_period,
                            veosf1.is_mcl,
                            veosf1.is_hero,
                            veosf1.bill_date,
                            veosf1.bill_doc,
                            veosf1.slsmn_cd,
                            veosf1.sls_qty,
                            veosf1.ret_qty,
                            veosf1.uom,
                            veosf1.sls_qty_pc,
                            veosf1.ret_qty_pc,
                            veosf1.grs_trd_sls,
                            veosf1.ret_val,
                            veosf1.trd_discnt,
                            veosf1.trd_sls,
                            veosf1.net_trd_sls,
                            veosf1.jj_grs_trd_sls,
                            veosf1.jj_ret_val,
                            veosf1.jj_trd_sls,
                            veosf1.jj_net_trd_sls,
                            veomd.sap_mat_desc AS sku_desc,
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
                            veomd.gph_region AS global_mat_region,
                            veomd.gph_prod_frnchse AS global_prod_franchise,
                            veomd.gph_prod_brnd AS global_prod_brand,
                            veomd.gph_prod_vrnt AS global_prod_variant,
                            veomd.gph_prod_put_up_cd AS global_prod_put_up_cd,
                            veomd.gph_prod_put_up_desc AS global_put_up_desc,
                            veomd.gph_prod_sub_brnd AS global_prod_sub_brand,
                            veomd.gph_prod_needstate AS global_prod_need_state,
                            veomd.gph_prod_ctgry AS global_prod_category,
                            veomd.gph_prod_subctgry AS global_prod_subcategory,
                            veomd.gph_prod_sgmnt AS global_prod_segment,
                            veomd.gph_prod_subsgmnt AS global_prod_subsegment,
                            veomd.gph_prod_size AS global_prod_size,
                            veomd.gph_prod_size_uom AS global_prod_size_uom
                        FROM (
                                (
                                    SELECT a.dstrbtr_grp_cd,
                                        a.cust_cd,
                                        esp.sup_id,
                                        esp.sup_nm,
                                        a.slsmn_cd AS sls_rep_id,
                                        a.slsmn_nm AS sls_rep_nm,
                                        esp.slsgrpnm AS sales_team,
                                        veodcd2.cust_nm,
                                        veodcd.sap_soldto_code,
                                        veodmd.sap_matl_num,
                                        upper(trim((a.dstrbtr_matl_num)::text)) AS dstrbtr_matl_num,
                                        veodcd2.chnl_cd,
                                        veodcd2.chnl_desc,
                                        veodcd2.sub_chnl_cd,
                                        veodcd2.sub_chnl_desc,
                                        impgc.rep_grp2 AS region_cd,
                                        impgc.rep_grp2_desc AS region_nm,
                                        impgc.rep_grp3 AS province_cd,
                                        impgc.rep_grp3_desc AS province_nm,
                                        impgc.rep_grp5 AS town_cd,
                                        impgc.rep_grp5_desc AS town_nm,
                                        impgc.store_prioritization AS str_prioritization,
                                        impgc.sls_dist AS rka_cd,
                                        rka.rka_nm,
                                        veodmd.is_npi,
                                        veodmd.npi_str_period,
                                        veodmd.npi_end_period,
                                        veodmd.is_reg,
                                        veodmd.is_promo,
                                        veodmd.promo_strt_period,
                                        veodmd.promo_end_period,
                                        veodmd.is_mcl,
                                        veodmd.is_hero,
                                        a.bill_date,
                                        a.bill_doc,
                                        a.slsmn_cd,
                                        a.sls_qty,
                                        a.ret_qty,
                                        a.uom,
                                        a.sls_qty_pc,
                                        a.ret_qty_pc,
                                        a.grs_trd_sls,
                                        a.ret_val,
                                        a.trd_discnt,
                                        a.trd_sls,
                                        a.net_trd_sls,
                                        a.jj_grs_trd_sls,
                                        a.jj_ret_val,
                                        a.jj_trd_sls,
                                        a.jj_net_trd_sls
                                    FROM (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                SELECT fact.cntry_cd,
                                                                    fact.cntry_nm,
                                                                    fact.dstrbtr_grp_cd,
                                                                    fact.dstrbtr_soldto_code,
                                                                    fact.cust_cd,
                                                                    fact.dstrbtr_matl_num,
                                                                    fact.sap_matl_num,
                                                                    fact.bar_cd,
                                                                    fact.bill_date,
                                                                    fact.bill_doc,
                                                                    fact.slsmn_cd,
                                                                    fact.slsmn_nm,
                                                                    fact.wh_id,
                                                                    fact.doc_type,
                                                                    fact.doc_type_desc,
                                                                    fact.base_sls,
                                                                    fact.sls_qty,
                                                                    fact.ret_qty,
                                                                    fact.uom,
                                                                    fact.sls_qty_pc,
                                                                    fact.ret_qty_pc,
                                                                    fact.grs_trd_sls,
                                                                    fact.ret_val,
                                                                    fact.trd_discnt,
                                                                    fact.trd_discnt_item_lvl,
                                                                    fact.trd_discnt_bill_lvl,
                                                                    fact.trd_sls,
                                                                    fact.net_trd_sls,
                                                                    fact.cn_reason_cd,
                                                                    fact.cn_reason_desc,
                                                                    fact.jj_grs_trd_sls,
                                                                    fact.jj_ret_val,
                                                                    fact.jj_trd_sls,
                                                                    fact.jj_net_trd_sls,
                                                                    cal.sales_cycle
                                                                FROM (
                                                                        edw_vw_ph_sellout_sales_fact fact
                                                                        LEFT JOIN (
                                                                            SELECT edw_calendar_dim.cal_day,
                                                                                edw_calendar_dim.fisc_yr_vrnt,
                                                                                edw_calendar_dim.wkday,
                                                                                edw_calendar_dim.cal_wk,
                                                                                edw_calendar_dim.cal_mo_1,
                                                                                edw_calendar_dim.cal_mo_2,
                                                                                edw_calendar_dim.cal_qtr_1,
                                                                                edw_calendar_dim.cal_qtr_2,
                                                                                edw_calendar_dim.half_yr,
                                                                                edw_calendar_dim.cal_yr,
                                                                                edw_calendar_dim.fisc_per,
                                                                                edw_calendar_dim.pstng_per,
                                                                                edw_calendar_dim.fisc_yr,
                                                                                edw_calendar_dim.rec_mode,
                                                                                edw_calendar_dim.crt_dttm,
                                                                                edw_calendar_dim.updt_dttm,
                                                                                (
                                                                                    "left"(
                                                                                        ((edw_calendar_dim.fisc_per)::character varying)::text,
                                                                                        4
                                                                                    ) + "right"(
                                                                                        ((edw_calendar_dim.fisc_per)::character varying)::text,
                                                                                        2
                                                                                    )
                                                                                ) AS sales_cycle
                                                                            FROM edw_calendar_dim
                                                                        ) cal ON ((to_date(fact.bill_date) = cal.cal_day))
                                                                    )
                                                            ) a
                                                            LEFT JOIN (
                                                                SELECT DISTINCT edw_vw_ph_dstrbtr_customer_dim.dstrbtr_grp_cd,
                                                                    edw_vw_ph_dstrbtr_customer_dim.sap_soldto_code
                                                                FROM edw_vw_ph_dstrbtr_customer_dim
                                                                WHERE (
                                                                        (edw_vw_ph_dstrbtr_customer_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                                    )
                                                            ) veodcd ON (
                                                                (
                                                                    (veodcd.dstrbtr_grp_cd)::text = (a.dstrbtr_grp_cd)::text
                                                                )
                                                            )
                                                        )
                                                        LEFT JOIN (
                                                            SELECT edw_vw_ph_dstrbtr_material_dim.dstrbtr_matl_num,
                                                                edw_vw_ph_dstrbtr_material_dim.dstrbtr_grp_cd,
                                                                edw_vw_ph_dstrbtr_material_dim.sap_soldto_code,
                                                                edw_vw_ph_dstrbtr_material_dim.sap_matl_num,
                                                                edw_vw_ph_dstrbtr_material_dim.is_npi,
                                                                edw_vw_ph_dstrbtr_material_dim.npi_str_period,
                                                                edw_vw_ph_dstrbtr_material_dim.npi_end_period,
                                                                edw_vw_ph_dstrbtr_material_dim.is_reg,
                                                                edw_vw_ph_dstrbtr_material_dim.is_promo,
                                                                edw_vw_ph_dstrbtr_material_dim.promo_strt_period,
                                                                edw_vw_ph_dstrbtr_material_dim.promo_end_period,
                                                                edw_vw_ph_dstrbtr_material_dim.is_mcl,
                                                                edw_vw_ph_dstrbtr_material_dim.is_hero
                                                            FROM edw_vw_ph_dstrbtr_material_dim
                                                            WHERE (
                                                                    (edw_vw_ph_dstrbtr_material_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                                )
                                                        ) veodmd ON (
                                                            (
                                                                (
                                                                    (veodmd.dstrbtr_grp_cd)::text = (a.dstrbtr_grp_cd)::text
                                                                )
                                                                AND (
                                                                    upper(trim((veodmd.dstrbtr_matl_num)::text)) = (a.dstrbtr_matl_num)::text
                                                                )
                                                            )
                                                        )
                                                    )
                                                    LEFT JOIN (
                                                        SELECT itg_mds_ph_distributor_supervisors.distcode,
                                                            itg_mds_ph_distributor_supervisors.slsspid AS sup_id,
                                                            itg_mds_ph_distributor_supervisors.slsspnm AS sup_nm,
                                                            itg_mds_ph_distributor_supervisors.salescycle,
                                                            itg_mds_ph_distributor_supervisors.slsid,
                                                            itg_mds_ph_distributor_supervisors.slsgrpnm
                                                        FROM itg_mds_ph_distributor_supervisors
                                                        GROUP BY itg_mds_ph_distributor_supervisors.distcode,
                                                            itg_mds_ph_distributor_supervisors.slsspid,
                                                            itg_mds_ph_distributor_supervisors.slsspnm,
                                                            itg_mds_ph_distributor_supervisors.salescycle,
                                                            itg_mds_ph_distributor_supervisors.slsid,
                                                            itg_mds_ph_distributor_supervisors.slsgrpnm
                                                    ) esp ON (
                                                        (
                                                            (
                                                                ((a.dstrbtr_grp_cd)::text = (esp.distcode)::text)
                                                                AND (a.sales_cycle = (esp.salescycle)::text)
                                                            )
                                                            AND ((a.slsmn_cd)::text = (esp.slsid)::text)
                                                        )
                                                    )
                                                )
                                                LEFT JOIN
                                                (
                                                    SELECT edw_vw_ph_dstrbtr_customer_dim.cntry_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.cntry_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.dstrbtr_grp_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.dstrbtr_soldto_code,
                                                        edw_vw_ph_dstrbtr_customer_dim.sap_soldto_code,
                                                        edw_vw_ph_dstrbtr_customer_dim.cust_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.cust_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.alt_cust_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.alt_cust_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.addr,
                                                        edw_vw_ph_dstrbtr_customer_dim.area_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.area_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.state_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.state_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.region_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.region_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.prov_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.prov_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.town_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.town_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.city_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.city_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.post_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.post_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.slsmn_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.slsmn_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.chnl_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.chnl_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.sub_chnl_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.sub_chnl_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.chnl_attr1_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.chnl_attr1_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.chnl_attr2_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.chnl_attr2_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.outlet_type_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.outlet_type_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.cust_grp_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.cust_grp_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr1_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr1_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr2_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.cust_grp_attr2_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.sls_dstrct_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.sls_dstrct_nm,
                                                        edw_vw_ph_dstrbtr_customer_dim.sls_office_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.sls_office_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.sls_grp_cd,
                                                        edw_vw_ph_dstrbtr_customer_dim.sls_grp_desc,
                                                        edw_vw_ph_dstrbtr_customer_dim.status
                                                    FROM edw_vw_ph_dstrbtr_customer_dim
                                                    WHERE (
                                                            (edw_vw_ph_dstrbtr_customer_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                        )
                                                ) veodcd2 ON (((veodcd2.cust_cd)::text = (a.cust_cd)::text))
                                            )
                                            LEFT JOIN
                                            (
                                                (
                                                    SELECT itg_mds_ph_gt_customer.dstrbtr_cust_id,
                                                        itg_mds_ph_gt_customer.dstrbtr_cust_nm,
                                                        itg_mds_ph_gt_customer.slsmn,
                                                        itg_mds_ph_gt_customer.slsmn_desc,
                                                        itg_mds_ph_gt_customer.rep_grp2,
                                                        itg_mds_ph_gt_customer.rep_grp2_desc,
                                                        itg_mds_ph_gt_customer.rep_grp3,
                                                        itg_mds_ph_gt_customer.rep_grp3_desc,
                                                        itg_mds_ph_gt_customer.rep_grp4,
                                                        itg_mds_ph_gt_customer.rep_grp4_desc,
                                                        itg_mds_ph_gt_customer.rep_grp5,
                                                        itg_mds_ph_gt_customer.rep_grp5_desc,
                                                        itg_mds_ph_gt_customer.rep_grp6,
                                                        itg_mds_ph_gt_customer.rep_grp6_desc,
                                                        itg_mds_ph_gt_customer.status,
                                                        itg_mds_ph_gt_customer.address,
                                                        itg_mds_ph_gt_customer.zip,
                                                        itg_mds_ph_gt_customer.slm_grp_cd,
                                                        itg_mds_ph_gt_customer.frequency_visit,
                                                        itg_mds_ph_gt_customer.store_prioritization,
                                                        itg_mds_ph_gt_customer.latitude,
                                                        itg_mds_ph_gt_customer.longitude,
                                                        itg_mds_ph_gt_customer.rpt_grp9,
                                                        itg_mds_ph_gt_customer.rpt_grp9_desc,
                                                        itg_mds_ph_gt_customer.rpt_grp11,
                                                        itg_mds_ph_gt_customer.rpt_grp11_desc,
                                                        itg_mds_ph_gt_customer.sls_dist,
                                                        itg_mds_ph_gt_customer.sls_dist_desc,
                                                        itg_mds_ph_gt_customer.dstrbtr_grp_cd,
                                                        itg_mds_ph_gt_customer.dstrbtr_grp_nm,
                                                        itg_mds_ph_gt_customer.rpt_grp_15_desc,
                                                        itg_mds_ph_gt_customer.last_chg_datetime,
                                                        itg_mds_ph_gt_customer.effective_from,
                                                        itg_mds_ph_gt_customer.effective_to,
                                                        itg_mds_ph_gt_customer.active,
                                                        itg_mds_ph_gt_customer.crtd_dttm,
                                                        itg_mds_ph_gt_customer.updt_dttm,
                                                        itg_mds_ph_gt_customer.zip_code,
                                                        itg_mds_ph_gt_customer.zip_cd_name,
                                                        itg_mds_ph_gt_customer.barangay_code,
                                                        itg_mds_ph_gt_customer.barangay_cd_name,
                                                        itg_mds_ph_gt_customer.long_lat_source
                                                    FROM itg_mds_ph_gt_customer
                                                    WHERE (
                                                            (itg_mds_ph_gt_customer.active)::text = ('Y'::character varying)::text
                                                        )
                                                ) impgc
                                                LEFT JOIN (
                                                    SELECT itg_mds_ph_ref_rka_master.rka_cd,
                                                        itg_mds_ph_ref_rka_master.rka_nm,
                                                        itg_mds_ph_ref_rka_master.last_chg_datetime,
                                                        itg_mds_ph_ref_rka_master.effective_from,
                                                        itg_mds_ph_ref_rka_master.effective_to,
                                                        itg_mds_ph_ref_rka_master.active,
                                                        itg_mds_ph_ref_rka_master.crtd_dttm,
                                                        itg_mds_ph_ref_rka_master.updt_dttm
                                                    FROM itg_mds_ph_ref_rka_master
                                                    WHERE (
                                                            (itg_mds_ph_ref_rka_master.active)::text = ('Y'::character varying)::text
                                                        )
                                                ) rka ON (
                                                    (
                                                        ltrim((impgc.sls_dist)::text) = ltrim((rka.rka_cd)::text)
                                                    )
                                                )
                                            ) ON (
                                                (
                                                    ltrim(
                                                        (impgc.dstrbtr_cust_id)::text,
                                                        ('0'::character varying)::text
                                                    ) = ltrim(
                                                        (a.cust_cd)::text,
                                                        ('0'::character varying)::text
                                                    )
                                                )
                                            )
                                        )
                                    WHERE (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            (
                                                                                a.sls_qty <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                                                                            )
                                                                            OR (
                                                                                a.ret_qty <> (((0)::numeric)::numeric(18, 0))::numeric(24, 6)
                                                                            )
                                                                        )
                                                                        OR (
                                                                            a.sls_qty_pc <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                                                                        )
                                                                    )
                                                                    OR (
                                                                        a.ret_qty_pc <> (((0)::numeric)::numeric(18, 0))::numeric(24, 6)
                                                                    )
                                                                )
                                                                OR (
                                                                    a.grs_trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(38, 12)
                                                                )
                                                            )
                                                            OR (
                                                                a.ret_val <> (((0)::numeric)::numeric(18, 0))::numeric(38, 12)
                                                            )
                                                        )
                                                        OR (
                                                            a.trd_discnt <> (((0)::numeric)::numeric(18, 0))::numeric(23, 6)
                                                        )
                                                    )
                                                    OR (
                                                        a.trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(20, 4)
                                                    )
                                                )
                                                OR (
                                                    a.net_trd_sls <> (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                                                )
                                            )
                                            AND (
                                                (a.cntry_cd)::text = ('PH'::character varying)::text
                                            )
                                        )
                                ) veosf1
                                LEFT JOIN (
                                    SELECT edw_vw_ph_material_dim.cntry_key,
                                        edw_vw_ph_material_dim.sap_matl_num,
                                        edw_vw_ph_material_dim.sap_mat_desc,
                                        edw_vw_ph_material_dim.ean_num,
                                        edw_vw_ph_material_dim.sap_mat_type_cd,
                                        edw_vw_ph_material_dim.sap_mat_type_desc,
                                        edw_vw_ph_material_dim.sap_base_uom_cd,
                                        edw_vw_ph_material_dim.sap_prchse_uom_cd,
                                        edw_vw_ph_material_dim.sap_prod_sgmt_cd,
                                        edw_vw_ph_material_dim.sap_prod_sgmt_desc,
                                        edw_vw_ph_material_dim.sap_base_prod_cd,
                                        edw_vw_ph_material_dim.sap_base_prod_desc,
                                        edw_vw_ph_material_dim.sap_mega_brnd_cd,
                                        edw_vw_ph_material_dim.sap_mega_brnd_desc,
                                        edw_vw_ph_material_dim.sap_brnd_cd,
                                        edw_vw_ph_material_dim.sap_brnd_desc,
                                        edw_vw_ph_material_dim.sap_vrnt_cd,
                                        edw_vw_ph_material_dim.sap_vrnt_desc,
                                        edw_vw_ph_material_dim.sap_put_up_cd,
                                        edw_vw_ph_material_dim.sap_put_up_desc,
                                        edw_vw_ph_material_dim.sap_grp_frnchse_cd,
                                        edw_vw_ph_material_dim.sap_grp_frnchse_desc,
                                        edw_vw_ph_material_dim.sap_frnchse_cd,
                                        edw_vw_ph_material_dim.sap_frnchse_desc,
                                        edw_vw_ph_material_dim.sap_prod_frnchse_cd,
                                        edw_vw_ph_material_dim.sap_prod_frnchse_desc,
                                        edw_vw_ph_material_dim.sap_prod_mjr_cd,
                                        edw_vw_ph_material_dim.sap_prod_mjr_desc,
                                        edw_vw_ph_material_dim.sap_prod_mnr_cd,
                                        edw_vw_ph_material_dim.sap_prod_mnr_desc,
                                        edw_vw_ph_material_dim.sap_prod_hier_cd,
                                        edw_vw_ph_material_dim.sap_prod_hier_desc,
                                        edw_vw_ph_material_dim.gph_region,
                                        edw_vw_ph_material_dim.gph_reg_frnchse,
                                        edw_vw_ph_material_dim.gph_reg_frnchse_grp,
                                        edw_vw_ph_material_dim.gph_prod_frnchse,
                                        edw_vw_ph_material_dim.gph_prod_brnd,
                                        edw_vw_ph_material_dim.gph_prod_sub_brnd,
                                        edw_vw_ph_material_dim.gph_prod_vrnt,
                                        edw_vw_ph_material_dim.gph_prod_needstate,
                                        edw_vw_ph_material_dim.gph_prod_ctgry,
                                        edw_vw_ph_material_dim.gph_prod_subctgry,
                                        edw_vw_ph_material_dim.gph_prod_sgmnt,
                                        edw_vw_ph_material_dim.gph_prod_subsgmnt,
                                        edw_vw_ph_material_dim.gph_prod_put_up_cd,
                                        edw_vw_ph_material_dim.gph_prod_put_up_desc,
                                        edw_vw_ph_material_dim.gph_prod_size,
                                        edw_vw_ph_material_dim.gph_prod_size_uom,
                                        edw_vw_ph_material_dim.launch_dt,
                                        edw_vw_ph_material_dim.qty_shipper_pc,
                                        edw_vw_ph_material_dim.prft_ctr,
                                        edw_vw_ph_material_dim.shlf_life
                                    FROM edw_vw_ph_material_dim
                                    WHERE (
                                            (edw_vw_ph_material_dim.cntry_key)::text = ('PH'::character varying)::text
                                        )
                                ) veomd ON (
                                    (
                                        ltrim(
                                            (veomd.sap_matl_num)::text,
                                            ('0'::character varying)::text
                                        ) = ltrim(
                                            (veosf1.sap_matl_num)::text,
                                            ('0'::character varying)::text
                                        )
                                    )
                                )
                            )
                        WHERE (
                                    
                                    ltrim(veosf1.dstrbtr_matl_num,('0'::character varying)::text) NOT IN 
                                    (
                                        SELECT DISTINCT ltrim((COALESCE(itg_mds_ph_lav_product.item_cd,'NA'::character varying))::text,('0'::character varying)::text) AS ltrim
                                        FROM itg_mds_ph_lav_product
                                        WHERE (
                                                (itg_mds_ph_lav_product.active)::text = ('Y'::character varying)::text
                                            )
                                    )
                                
                            )
                    ) dstrbtr_matl
            )
            
        )
),
final as
(
    SELECT
        veotd."year" AS jj_year,
        veotd.qrtr AS jj_qrtr,
        veotd.mnth_id AS jj_mnth_id,
        veotd.mnth_no AS jj_mnth_no,
        veotd.wk AS jj_wk_no,
        veotd.mnth_wk_no AS jj_mnth_wk_no,
        veocd.sap_cntry_nm AS cntry_nm,
        veosf.dstrbtr_grp_cd,
        veosf.cust_cd AS dstrbtr_cust_cd,
        veosf.cust_nm AS dstrbtr_cust_nm,
        ltrim(
            (veosf.sap_soldto_code)::text,
            ('0'::character varying)::text
        ) AS sap_soldto_code,
        eocd.cust_nm AS sap_soldto_nm,
        eocd.region,
        veosf.chnl_cd,
        veosf.chnl_desc,
        veosf.sub_chnl_cd,
        veosf.sub_chnl_desc,
        veosf.sls_rep_id,
        veosf.sls_rep_nm,
        eocd.parent_cust_cd AS parent_customer_cd,
        eocd.parent_cust_nm AS parent_customer,
        eocd.rpt_grp_6_desc AS account_grp,
        'General Trade' AS trade_type,
        eocd.rpt_grp_2_desc AS sls_grp_desc,
        veosf.region_cd,
        veosf.region_nm,
        veosf.province_cd,
        veosf.province_nm,
        veosf.town_cd,
        veosf.town_nm,
        veosf.str_prioritization,
        veosf.rka_cd,
        veosf.rka_nm,
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
        ltrim(
            (veosf.sap_matl_num)::text,
            ('0'::character varying)::text
        ) AS sku,
        veosf.sku_desc,
        veosf.sap_mat_type_cd,
        veosf.sap_mat_type_desc,
        veosf.sap_base_uom_cd,
        veosf.sap_prchse_uom_cd,
        veosf.sap_prod_sgmt_cd,
        veosf.sap_prod_sgmt_desc,
        veosf.sap_base_prod_cd,
        veosf.sap_base_prod_desc,
        veosf.sap_mega_brnd_cd,
        veosf.sap_mega_brnd_desc,
        veosf.sap_brnd_cd,
        veosf.sap_brnd_desc,
        veosf.sap_vrnt_cd,
        veosf.sap_vrnt_desc,
        veosf.sap_put_up_cd,
        veosf.sap_put_up_desc,
        veosf.sap_grp_frnchse_cd,
        veosf.sap_grp_frnchse_desc,
        veosf.sap_frnchse_cd,
        veosf.sap_frnchse_desc,
        veosf.sap_prod_frnchse_cd,
        veosf.sap_prod_frnchse_desc,
        veosf.sap_prod_mjr_cd,
        veosf.sap_prod_mjr_desc,
        veosf.sap_prod_mnr_cd,
        veosf.sap_prod_mnr_desc,
        veosf.sap_prod_hier_cd,
        veosf.sap_prod_hier_desc,
        veosf.global_mat_region,
        veosf.global_prod_franchise,
        veosf.global_prod_brand,
        veosf.global_prod_variant,
        veosf.global_prod_put_up_cd,
        veosf.global_put_up_desc,
        veosf.global_prod_sub_brand,
        veosf.global_prod_need_state,
        veosf.global_prod_category,
        veosf.global_prod_subcategory,
        veosf.global_prod_segment,
        veosf.global_prod_subsegment,
        veosf.global_prod_size,
        veosf.global_prod_size_uom,
        b.bill_date,
        veosf.bill_doc,
        veosf.sls_qty,
        veosf.ret_qty,
        veosf.uom,
        veosf.sls_qty_pc,
        veosf.ret_qty_pc,
        veosf.grs_trd_sls,
        veosf.ret_val,
        veosf.trd_discnt,
        veosf.trd_sls,
        veosf.net_trd_sls,
        veosf.jj_grs_trd_sls,
        veosf.jj_ret_val,
        (
            (
                COALESCE(
                    veosf.jj_grs_trd_sls,
                    ((0)::numeric)::numeric(18, 0)
                ) + COALESCE(veosf.jj_ret_val, ((0)::numeric)::numeric(18, 0))
            ) - COALESCE(veosf.trd_discnt, ((0)::numeric)::numeric(18, 0))
        ) AS jj_net_sales,
        veosf.jj_trd_sls,
        veosf.jj_net_trd_sls,
        veosf.is_npi,
        veosf.npi_str_period,
        veosf.npi_end_period,
        veosf.is_reg,
        veosf.is_promo,
        veosf.promo_strt_period,
        veosf.promo_end_period,
        veosf.is_mcl,
        veosf.is_hero,
        veosf.sup_id,
        veosf.sup_nm,
        veosf.sales_team
    FROM
        (
            SELECT "max"(edw_vw_ph_sellout_sales_fact.bill_date) AS bill_date
            FROM edw_vw_ph_sellout_sales_fact
        ) b,
        (
            SELECT DISTINCT edw_vw_os_time_dim."year",
                edw_vw_os_time_dim.qrtr,
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.mnth_no,
                edw_vw_os_time_dim.wk,
                edw_vw_os_time_dim.mnth_wk_no,
                edw_vw_os_time_dim.cal_date,
                edw_vw_os_time_dim.cal_date_id
            FROM edw_vw_os_time_dim
        ) veotd,
        (
            (
                veosf
                LEFT JOIN edw_mv_ph_customer_dim eocd ON (
                    (
                        (eocd.cust_id)::text = (veosf.sap_soldto_code)::text
                    )
                )
            )
            LEFT JOIN
            (
                SELECT edw_vw_ph_customer_dim.sap_cust_id,
                    edw_vw_ph_customer_dim.sap_cust_nm,
                    edw_vw_ph_customer_dim.sap_sls_org,
                    edw_vw_ph_customer_dim.sap_cmp_id,
                    edw_vw_ph_customer_dim.sap_cntry_cd,
                    edw_vw_ph_customer_dim.sap_cntry_nm,
                    edw_vw_ph_customer_dim.sap_addr,
                    edw_vw_ph_customer_dim.sap_region,
                    edw_vw_ph_customer_dim.sap_state_cd,
                    edw_vw_ph_customer_dim.sap_city,
                    edw_vw_ph_customer_dim.sap_post_cd,
                    edw_vw_ph_customer_dim.sap_chnl_cd,
                    edw_vw_ph_customer_dim.sap_chnl_desc,
                    edw_vw_ph_customer_dim.sap_sls_office_cd,
                    edw_vw_ph_customer_dim.sap_sls_office_desc,
                    edw_vw_ph_customer_dim.sap_sls_grp_cd,
                    edw_vw_ph_customer_dim.sap_sls_grp_desc,
                    edw_vw_ph_customer_dim.sap_curr_cd,
                    edw_vw_ph_customer_dim.sap_prnt_cust_key,
                    edw_vw_ph_customer_dim.sap_prnt_cust_desc,
                    edw_vw_ph_customer_dim.sap_cust_chnl_key,
                    edw_vw_ph_customer_dim.sap_cust_chnl_desc,
                    edw_vw_ph_customer_dim.sap_cust_sub_chnl_key,
                    edw_vw_ph_customer_dim.sap_sub_chnl_desc,
                    edw_vw_ph_customer_dim.sap_go_to_mdl_key,
                    edw_vw_ph_customer_dim.sap_go_to_mdl_desc,
                    edw_vw_ph_customer_dim.sap_bnr_key,
                    edw_vw_ph_customer_dim.sap_bnr_desc,
                    edw_vw_ph_customer_dim.sap_bnr_frmt_key,
                    edw_vw_ph_customer_dim.sap_bnr_frmt_desc,
                    edw_vw_ph_customer_dim.retail_env,
                    edw_vw_ph_customer_dim.gch_region,
                    edw_vw_ph_customer_dim.gch_cluster,
                    edw_vw_ph_customer_dim.gch_subcluster,
                    edw_vw_ph_customer_dim.gch_market,
                    edw_vw_ph_customer_dim.gch_retail_banner
                FROM edw_vw_ph_customer_dim
                WHERE (
                        (edw_vw_ph_customer_dim.sap_cntry_cd)::text = ('PH'::character varying)::text
                    )
            ) veocd ON (
                (
                    ltrim(
                        (veocd.sap_cust_id)::text,
                        ('0'::character varying)::text
                    ) = (veosf.sap_soldto_code)::text
                )
            )
        )
    WHERE
        (
            to_date((veotd.cal_date)::timestamp without time zone) = to_date(veosf.bill_date)
        )
)
select * from final