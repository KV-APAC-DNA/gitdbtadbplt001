with
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_ph_pos_pg_sm_sales_fact as 
(
    select * from snaposeitg_integration.itg_ph_pos_pg_sm_sales_fact
),
edw_vw_ph_pos_customer_dim as 
(
    select * from snaposeedw_integration.edw_vw_os_pos_customer_dim
    where cntry_cd='PH'
),
itg_mds_ph_ref_pos_primary_sold_to as 
(
    select * from snaposeitg_integration.itg_mds_ph_ref_pos_primary_sold_to
),
edw_vw_ph_pos_material_dim as 
(
    select * from snaposeedw_integration.edw_vw_os_pos_material_dim
    where cntry_cd='PH'
),
itg_mds_ph_pos_customers as 
(
    select * from snaposeitg_integration.itg_mds_ph_pos_customers
),
edw_vw_ph_material_dim as 
(
    select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}),
edw_mv_ph_customer_dim as 
(
    select * from snaposeedw_integration.edw_mv_ph_customer_dim
),
edw_vw_ph_customer_dim as 
(
    select * from snaposeedw_integration.edw_vw_os_customer_dim
    where sap_cntry_cd='PH'
),
itg_mds_ph_lav_product as 
(
    select * from snaposeitg_integration.itg_mds_ph_lav_product
),
itg_mds_ph_pos_pricelist as 
(
    select * from snaposeitg_integration.itg_mds_ph_pos_pricelist
),
final as
(
SELECT 
    veposf."year" AS jj_year,
    veposf.qrtr AS jj_qtr,
    veposf.jj_mnth_id,
    veposf.mnth_no AS jj_mnth_no,
    veposf.cntry_nm,
    veposf.cust_cd,
    veposf.cust_brnch_cd,
    veposf.brnch_nm AS mt_cust_brnch_nm,
    veposf.region_cd,
    veposf.region_nm,
    veposf.prov_cd,
    veposf.prov_nm,
    veposf.mncplty_cd,
    veposf.mncplty_nm,
    veposf.city_cd,
    veposf.city_nm,
    veposf.ae_nm,
    veposf.ash_no,
    veposf.ash_nm,
    veposf.pms_nm,
    veposf.item_cd,
    veposf.item_nm AS mt_item_nm,
    veposf.sold_to,
    veocd.sap_cust_nm AS sold_to_nm,
   eocd.region,
    eocd.channel_cd AS chnl_cd,
    eocd.channel_desc AS chnl_desc,
    eocd.sub_chnl_cd,
    eocd.sub_chnl_desc,
    eocd.parent_cust_cd AS parent_customer_cd,
    eocd.parent_cust_nm AS parent_customer,
    eocd.rpt_grp_6_desc AS account_grp,
    'MODERN TRADE' AS trade_type,
    eocd.rpt_grp_2_desc AS sls_grp_desc,
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
        (veomd.sap_matl_num)::text,
        ('0'::character varying)::text
    ) AS sku,
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
    veomd.gph_prod_size_uom AS global_prod_size_uom,
    CASE
        WHEN (
            upper((epmad.promo_reg_ind)::text) = ('REG'::character varying)::text
        ) THEN 'Y'::character varying
        ELSE 'N'::character varying
    END AS is_reg,
    CASE
        WHEN (
            upper((epmad.promo_reg_ind)::text) = ('PROMO'::character varying)::text
        ) THEN 'Y'::character varying
        ELSE 'N'::character varying
    END AS is_promo,
    epmad.promo_strt_period AS local_mat_promo_strt_period,
    CASE
        WHEN (
            (
                (
                    (epp2.status)::text = ('**'::character varying)::text
                )
                AND (veotd2.mnth_id >= epp2.launch_period)
            )
            AND (veotd2.mnth_id <= epp2.end_period)
        ) THEN 'Y'::character varying
        ELSE 'N'::character varying
    END AS is_npi,
    CASE
        WHEN (
            upper((epmad.hero_sku_ind)::text) = ('Y'::character varying)::text
        ) THEN 'HERO'::character varying
        ELSE 'NA'::character varying
    END AS is_hero,
    NULL AS is_mcl,
    epp2.launch_period AS local_mat_npi_strt_period,
    veposf.pos_qty,
    veposf.pos_gts,
    veposf.pos_item_prc,
    veposf.pos_tax,
    veposf.pos_nts,
    veposf.conv_factor,
    veposf.jj_qty_pc,
    veposf.jj_item_prc_per_pc,
    veposf.jj_gts,
    veposf.jj_vat_amt,
    veposf.jj_nts
FROM (
        SELECT edw_vw_os_time_dim.mnth_id
        FROM edw_vw_os_time_dim
        WHERE (
                edw_vw_os_time_dim.cal_date = current_timestamp::date
            )
    ) veotd2,
    (
        (
            (
                (
                    (
                        (
                            SELECT b."year",
                                b.qrtr,
                                b.qrtr_no,
                                b.mnth_no,
                                a.cntry_cd,
                                c.cntry_nm,
                                a.jj_mnth_id,
                                a.cust_cd,
                                a.cust_brnch_cd,
                                e.primary_soldto AS sold_to,
                                c.brnch_nm,
                                c.region_cd,
                                c.region_nm,
                                c.prov_cd,
                                c.prov_nm,
                                c.mncplty_cd,
                                c.mncplty_nm,
                                c.city_cd,
                                c.city_nm,
                                f.ae_nm,
                                f.ash_no,
                                f.ash_nm,
                                f.pms_nm,
                                a.item_cd,
                                d.item_nm,
                                d.sap_item_cd,
                                a.pos_qty,
                                a.pos_gts,
                                a.pos_item_prc,
                                a.pos_tax,
                                a.pos_nts,
                                a.conv_factor,
                                a.jj_qty_pc,
                                a.jj_item_prc_per_pc,
                                a.jj_gts,
                                a.jj_vat_amt,
                                a.jj_nts
                            FROM (
                                    SELECT DISTINCT edw_vw_os_time_dim."year",
                                        edw_vw_os_time_dim.qrtr_no,
                                        edw_vw_os_time_dim.qrtr,
                                        edw_vw_os_time_dim.mnth_id,
                                        edw_vw_os_time_dim.mnth_desc,
                                        edw_vw_os_time_dim.mnth_no
                                    FROM edw_vw_os_time_dim
                                ) b,
                                (
                                    (
                                        (
                                            (
                                                (
                                                    SELECT 'PH'::character varying AS cntry_cd,
                                                        'PHILIPPINES'::character varying AS cntry_nm,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_mnth_id,
                                                        itg_ph_pos_pg_sm_sales_fact.cust_cd,
                                                        itg_ph_pos_pg_sm_sales_fact.item_cd,
                                                        itg_ph_pos_pg_sm_sales_fact.brnch_cd AS cust_brnch_cd,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_qty,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_gts,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_item_prc,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_tax,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_nts,
                                                        itg_ph_pos_pg_sm_sales_fact.conv_factor,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_qty_pc,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_item_prc_per_pc,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_gts,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_vat_amt,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_nts
                                                    FROM itg_ph_pos_pg_sm_sales_fact
                                                ) a
                                                LEFT JOIN (
                                                    SELECT edw_vw_ph_pos_customer_dim.cntry_cd,
                                                        edw_vw_ph_pos_customer_dim.cntry_nm,
                                                        edw_vw_ph_pos_customer_dim.cust_cd,
                                                        edw_vw_ph_pos_customer_dim.cust_nm,
                                                        edw_vw_ph_pos_customer_dim.sold_to,
                                                        edw_vw_ph_pos_customer_dim.brnch_cd,
                                                        edw_vw_ph_pos_customer_dim.brnch_nm,
                                                        edw_vw_ph_pos_customer_dim.brnch_frmt,
                                                        edw_vw_ph_pos_customer_dim.brnch_typ,
                                                        edw_vw_ph_pos_customer_dim.dept_cd,
                                                        edw_vw_ph_pos_customer_dim.dept_nm,
                                                        edw_vw_ph_pos_customer_dim.address1,
                                                        edw_vw_ph_pos_customer_dim.address2,
                                                        edw_vw_ph_pos_customer_dim.region_cd,
                                                        edw_vw_ph_pos_customer_dim.region_nm,
                                                        edw_vw_ph_pos_customer_dim.prov_cd,
                                                        edw_vw_ph_pos_customer_dim.prov_nm,
                                                        edw_vw_ph_pos_customer_dim.city_cd,
                                                        edw_vw_ph_pos_customer_dim.city_nm,
                                                        edw_vw_ph_pos_customer_dim.mncplty_cd,
                                                        edw_vw_ph_pos_customer_dim.mncplty_nm
                                                    FROM edw_vw_ph_pos_customer_dim
                                                    WHERE (
                                                            (edw_vw_ph_pos_customer_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                        )
                                                ) c ON (
                                                    (
                                                        ((c.brnch_cd)::text = (a.cust_brnch_cd)::text)
                                                        AND ((c.cust_cd)::text = (a.cust_cd)::text)
                                                    )
                                                )
                                            )
                                            LEFT JOIN (
                                                SELECT itg_mds_ph_ref_pos_primary_sold_to.cust_cd,
                                                    itg_mds_ph_ref_pos_primary_sold_to.primary_soldto,
                                                    itg_mds_ph_ref_pos_primary_sold_to.last_chg_datetime,
                                                    itg_mds_ph_ref_pos_primary_sold_to.effective_from,
                                                    itg_mds_ph_ref_pos_primary_sold_to.effective_to,
                                                    itg_mds_ph_ref_pos_primary_sold_to.active,
                                                    itg_mds_ph_ref_pos_primary_sold_to.crtd_dttm,
                                                    itg_mds_ph_ref_pos_primary_sold_to.updt_dttm
                                                FROM itg_mds_ph_ref_pos_primary_sold_to
                                                WHERE (
                                                        (itg_mds_ph_ref_pos_primary_sold_to.active)::text = ('Y'::character varying)::text
                                                    )
                                            ) e ON (((e.cust_cd)::text = (a.cust_cd)::text))
                                        )
                                        LEFT JOIN (
                                            SELECT edw_vw_ph_pos_material_dim.cntry_cd,
                                                edw_vw_ph_pos_material_dim.cntry_nm,
                                                edw_vw_ph_pos_material_dim.jj_mnth_id,
                                                edw_vw_ph_pos_material_dim.cust_cd,
                                                edw_vw_ph_pos_material_dim.item_cd,
                                                edw_vw_ph_pos_material_dim.item_nm,
                                                edw_vw_ph_pos_material_dim.sap_item_cd,
                                                edw_vw_ph_pos_material_dim.bar_cd,
                                                edw_vw_ph_pos_material_dim.cust_sku_grp,
                                                edw_vw_ph_pos_material_dim.cust_conv_factor,
                                                edw_vw_ph_pos_material_dim.cust_item_prc,
                                                edw_vw_ph_pos_material_dim.lst_period,
                                                edw_vw_ph_pos_material_dim.early_bk_period,
                                                edw_vw_ph_pos_material_dim.eff_str_date,
                                                edw_vw_ph_pos_material_dim.eff_end_date
                                            FROM edw_vw_ph_pos_material_dim
                                            WHERE (
                                                    (edw_vw_ph_pos_material_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                )
                                        ) d ON (
                                            (
                                                (
                                                    (
                                                        ltrim(
                                                            (d.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        ) = ltrim(
                                                            (a.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        )
                                                    )
                                                    AND ((d.jj_mnth_id)::text = (a.jj_mnth_id)::text)
                                                )
                                                AND ((d.cust_cd)::text = (a.cust_cd)::text)
                                            )
                                        )
                                    )
                                    LEFT JOIN (
                                        SELECT itg_mds_ph_pos_customers.cust_cd,
                                            itg_mds_ph_pos_customers.brnch_cd,
                                            itg_mds_ph_pos_customers.ae_nm,
                                            itg_mds_ph_pos_customers.ash_no,
                                            itg_mds_ph_pos_customers.ash_nm,
                                            itg_mds_ph_pos_customers.pms_nm
                                        FROM itg_mds_ph_pos_customers
                                        WHERE (
                                                (
                                                    (
                                                        (itg_mds_ph_pos_customers.cust_cd)::text = ('MDC'::character varying)::text
                                                    )
                                                    OR (
                                                        (itg_mds_ph_pos_customers.cust_cd)::text = ('WAT'::character varying)::text
                                                    )
                                                )
                                                AND (
                                                    (itg_mds_ph_pos_customers.active)::text = ('Y'::character varying)::text
                                                )
                                            )
                                    ) f ON (
                                        (
                                            ((f.cust_cd)::text = (a.cust_cd)::text)
                                            AND ((f.brnch_cd)::text = (a.cust_brnch_cd)::text)
                                        )
                                    )
                                )
                            WHERE (b.mnth_id = (a.jj_mnth_id)::text)
                        ) veposf
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
                                upper(
                                    ltrim(
                                        (veomd.sap_matl_num)::text,
                                        ((0)::character varying)::text
                                    )
                                ) = ltrim(
                                    (veposf.sap_item_cd)::text,
                                    ('0'::character varying)::text
                                )
                            )
                        )
                    )
                    LEFT JOIN edw_mv_ph_customer_dim eocd ON (
                        (
                            upper(trim((eocd.cust_id)::text)) = upper(trim((veposf.sold_to)::text))
                        )
                    )
                )
                LEFT JOIN (
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
                        upper(
                            ltrim(
                                (veocd.sap_cust_id)::text,
                                ('0'::character varying)::text
                            )
                        ) = upper(trim((veposf.sold_to)::text))
                    )
                )
            )
            LEFT JOIN (
                SELECT itg_mds_ph_lav_product.item_cd,
                    itg_mds_ph_lav_product.promo_reg_ind,
                    itg_mds_ph_lav_product.hero_sku_ind,
                    itg_mds_ph_lav_product.promo_strt_period,
                    itg_mds_ph_lav_product.npi_strt_period
                FROM itg_mds_ph_lav_product
                WHERE (
                        (itg_mds_ph_lav_product.active)::text = ('Y'::character varying)::text
                    )
            ) epmad ON (
                (
                    upper(trim((epmad.item_cd)::text)) = ltrim(
                        (veposf.sap_item_cd)::text,
                        ('0'::character varying)::text
                    )
                )
            )
        )
        LEFT JOIN (
            SELECT itg_mds_ph_pos_pricelist.status,
                itg_mds_ph_pos_pricelist.item_cd,
                min((itg_mds_ph_pos_pricelist.jj_mnth_id)::text) AS launch_period,
                min(
                    to_char(
                        add_months(
                            (
                                (
                                    concat(
                                        (itg_mds_ph_pos_pricelist.jj_mnth_id)::text,
                                        ('01'::character varying)::text
                                    )
                                )::date
                            )::timestamp without time zone,
                            (11)::bigint
                        ),
                        ('YYYYMM'::character varying)::text
                    )
                ) AS end_period
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
        ) epp2 ON (
            (
                upper(trim((epp2.item_cd)::text)) = ltrim(
                    (veposf.sap_item_cd)::text,
                    ('0'::character varying)::text
                )
            )
        )
    )
)
select * from final