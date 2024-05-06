with edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_ph_pos_sales_fact as (
    select * from {{ ref('phledw_integration__edw_vw_ph_pos_sales_fact') }}
),
edw_vw_ph_pos_customer_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_pos_customer_dim') }}
),
itg_mds_ph_ref_pos_primary_sold_to as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_pos_primary_sold_to') }}
),
edw_vw_ph_pos_material_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_pos_material_dim') }}
),
edw_vw_ph_material_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
),
edw_vw_ph_customer_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_customer_dim') }}
),
edw_mv_ph_customer_dim as (
    select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
edw_vw_ph_sellin_sales_fact as (
    select * from {{ ref('phledw_integration__edw_vw_ph_sellin_sales_fact') }}
),
itg_ph_as_watsons_inventory as (
    select * from {{ ref('phlitg_integration__itg_ph_as_watsons_inventory') }}
),
itg_mds_ph_pos_pricelist as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
itg_mds_ph_pos_product as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_product') }}
),
edw_vw_ph_pos_inventory as (
    select * from {{ ref('phledw_integration__edw_vw_ph_pos_inventory') }}
),
veposf as
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
    FROM
        (
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
                    SELECT edw_vw_ph_pos_sales_fact.cntry_cd,
                        edw_vw_ph_pos_sales_fact.cntry_nm,
                        edw_vw_ph_pos_sales_fact.pos_dt,
                        edw_vw_ph_pos_sales_fact.jj_yr_week_no,
                        edw_vw_ph_pos_sales_fact.jj_mnth_id,
                        edw_vw_ph_pos_sales_fact.cust_cd,
                        edw_vw_ph_pos_sales_fact.item_cd,
                        edw_vw_ph_pos_sales_fact.item_desc,
                        edw_vw_ph_pos_sales_fact.sap_matl_num,
                        edw_vw_ph_pos_sales_fact.bar_cd,
                        edw_vw_ph_pos_sales_fact.master_code,
                        edw_vw_ph_pos_sales_fact.cust_brnch_cd,
                        edw_vw_ph_pos_sales_fact.pos_qty,
                        edw_vw_ph_pos_sales_fact.pos_gts,
                        edw_vw_ph_pos_sales_fact.pos_item_prc,
                        edw_vw_ph_pos_sales_fact.pos_tax,
                        edw_vw_ph_pos_sales_fact.pos_nts,
                        edw_vw_ph_pos_sales_fact.conv_factor,
                        edw_vw_ph_pos_sales_fact.jj_qty_pc,
                        edw_vw_ph_pos_sales_fact.jj_item_prc_per_pc,
                        edw_vw_ph_pos_sales_fact.jj_gts,
                        edw_vw_ph_pos_sales_fact.jj_vat_amt,
                        edw_vw_ph_pos_sales_fact.jj_nts,
                        edw_vw_ph_pos_sales_fact.dept_cd
                    FROM edw_vw_ph_pos_sales_fact
                    WHERE
                        (
                            (edw_vw_ph_pos_sales_fact.cntry_cd)::TEXT = ('PH'::CHARACTER VARYING)::TEXT
                        )
                ) a
                LEFT JOIN
                (
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
                            (edw_vw_ph_pos_customer_dim.cntry_cd)::TEXT = ('PH'::CHARACTER VARYING)::TEXT
                        )
                ) c ON
                (
                    (
                        ((c.brnch_cd)::TEXT = (a.cust_brnch_cd)::TEXT)
                        AND ((c.cust_cd)::TEXT = (a.cust_cd)::TEXT)
                    )
                )
                LEFT JOIN
                (
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
                            (itg_mds_ph_ref_pos_primary_sold_to.active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT
                        )
                ) e ON (((e.cust_cd)::TEXT = (a.cust_cd)::TEXT))
            )
            LEFT JOIN
            (
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
                        (edw_vw_ph_pos_material_dim.cntry_cd)::TEXT = ('PH'::CHARACTER VARYING)::TEXT
                    )
            ) d ON
            (
                (
                    (
                        (
                            ltrim(
                                (d.item_cd)::TEXT,
                                ('0'::CHARACTER VARYING)::TEXT
                            ) = ltrim(
                                (a.item_cd)::TEXT,
                                ('0'::CHARACTER VARYING)::TEXT
                            )
                        )
                        AND ((d.jj_mnth_id)::TEXT = (a.jj_mnth_id)::TEXT)
                    )
                    AND ((d.cust_cd)::TEXT = (a.cust_cd)::TEXT)
                )
            )
        )
    WHERE (b.mnth_id = (a.jj_mnth_id)::TEXT)
),
veomd as
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
    WHERE
    (
        (edw_vw_ph_material_dim.cntry_key)::TEXT = ('PH'::CHARACTER VARYING)::TEXT
    )
),
veocd as
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
            (edw_vw_ph_customer_dim.sap_cntry_cd)::TEXT = ('PH'::CHARACTER VARYING)::TEXT
        )
),
veossf as
(
    SELECT veotd.mnth_id AS jj_mnth_id,
        veotd.mnth_no AS jj_mnth_no,
        veotd."year" AS jj_year,
        veotd.qrtr AS jj_qtr,
        eocd1.dstrbtr_grp_cd,
        ltrim(
            (veossf1.cust_id)::TEXT,
            ('0'::CHARACTER VARYING)::TEXT
        ) AS cust_id,
        veossf1.item_cd,
        sum(veossf1.sls_qty) AS sls_qty,
        sum(veossf1.ret_qty) AS ret_qty,
        sum(veossf1.sls_less_rtn_qty) AS sls_less_rtn_qty,
        sum(veossf1.gts_val) AS gts_val,
        sum(veossf1.ret_val) AS ret_val,
        sum(veossf1.gts_less_rtn_val) AS gts_less_rtn_val,
        sum(veossf1.nts_qty) AS nts_qty,
        sum(veossf1.nts_val) AS nts_val,
        sum(veossf1.tp_val) AS tp_val
    FROM
        (
            SELECT a."year",
                a.qrtr_no,
                a.qrtr,
                a.mnth_id,
                a.mnth_desc,
                a.mnth_no,
                a.mnth_shrt,
                a.mnth_long
            FROM (
                    SELECT DISTINCT t."year",
                        t.qrtr_no,
                        t.qrtr,
                        t.mnth_id,
                        t.mnth_desc,
                        t.mnth_no,
                        t.mnth_shrt,
                        t.mnth_long
                    FROM edw_vw_os_time_dim t
                ) a
        ) veotd,
        (
            edw_vw_ph_sellin_sales_fact veossf1
            LEFT JOIN
            (
                SELECT DISTINCT edw_mv_ph_customer_dim.cust_id,
                    edw_mv_ph_customer_dim.dstrbtr_grp_cd
                FROM edw_mv_ph_customer_dim
                WHERE
                    (
                        edw_mv_ph_customer_dim.cust_id IN
                        (
                            SELECT DISTINCT edw_mv_ph_customer_dim.cust_id
                            FROM edw_mv_ph_customer_dim
                            WHERE (
                                    edw_mv_ph_customer_dim.parent_cust_cd IN (
                                        SELECT DISTINCT edw_mv_ph_customer_dim.parent_cust_cd
                                        FROM edw_mv_ph_customer_dim
                                        WHERE (
                                                edw_mv_ph_customer_dim.cust_id IN (
                                                    SELECT itg_mds_ph_ref_pos_primary_sold_to.primary_soldto
                                                    FROM itg_mds_ph_ref_pos_primary_sold_to
                                                    WHERE (
                                                            (itg_mds_ph_ref_pos_primary_sold_to.active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT
                                                        )
                                                )
                                            )
                                    )
                                )
                        )
                    )
            ) eocd1 ON
            (
                (
                    upper(trim((eocd1.cust_id)::TEXT)) = upper(
                        ltrim(
                            (veossf1.cust_id)::TEXT,
                            ('0'::CHARACTER VARYING)::TEXT
                        )
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
                                        veossf1.sls_qty <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 5)
                                    )
                                    OR (
                                        veossf1.ret_qty <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 5)
                                    )
                                )
                                OR (
                                    veossf1.gts_val <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 5)
                                )
                            )
                            OR (
                                veossf1.ret_val <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 5)
                            )
                        )
                        OR (
                            veossf1.nts_val <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 5)
                        )
                    )
                    OR (
                        veossf1.tp_val <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 5)
                    )
                )
                AND (
                    (veossf1.cntry_nm)::TEXT = ('PH'::CHARACTER VARYING)::TEXT
                )
            )
            AND (
                trim((veossf1.jj_mnth_id)::TEXT) = (
                    (trunc((veotd.mnth_id)::NUMERIC(18, 0)))::CHARACTER VARYING
                )::TEXT
            )
        )
    GROUP BY veotd.mnth_id,
        veotd.mnth_no,
        veotd."year",
        veotd.qrtr,
        eocd1.dstrbtr_grp_cd,
        veossf1.cust_id,
        veossf1.item_cd
),
pos as
(
    SELECT
        veposf."year" AS jj_year,
        (veposf.qrtr)::CHARACTER VARYING AS jj_qtr,
        veposf.jj_mnth_id,
        veposf.mnth_no AS jj_mnth_no,
        veposf.cntry_nm,
        veposf.cust_cd,
        veposf.cust_brnch_cd,
        veposf.brnch_nm AS mt_cust_brnch_nm,
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
        'MODERN TRADE'::CHARACTER VARYING AS trade_type,
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
        (
            ltrim(
                (veomd.sap_matl_num)::TEXT,
                ('0'::CHARACTER VARYING)::TEXT
            )
        )::CHARACTER VARYING AS sku,
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
        veposf.jj_nts,
        0 AS local_mat_listpriceunit,
        0 AS si_sls_qty,
        0 AS si_ret_qty,
        0 AS si_sls_less_rtn_qty,
        0 AS si_gts_val,
        0 AS si_ret_val,
        0 AS si_gts_less_rtn_val,
        0 AS si_nts_qty,
        0 AS si_nts_val,
        0 AS si_tp_val
    FROM veposf
    LEFT JOIN veomd ON
    upper(ltrim((veomd.sap_matl_num)::TEXT,((0)::CHARACTER VARYING)::TEXT)) = trim((veposf.sap_item_cd)::TEXT)
    LEFT JOIN edw_mv_ph_customer_dim eocd ON
    upper(trim((eocd.cust_id)::TEXT)) = upper(trim((veposf.sold_to)::TEXT))
    LEFT JOIN veocd ON
    (upper(ltrim((veocd.sap_cust_id)::TEXT,('0'::CHARACTER VARYING)::TEXT)) = upper(trim((veposf.sold_to)::TEXT)))

    UNION ALL
    SELECT
        veossf.jj_year,
        (veossf.jj_qtr)::CHARACTER VARYING AS jj_qtr,
        (veossf.jj_mnth_id)::CHARACTER VARYING AS jj_mnth_id,
        veossf.jj_mnth_no,
        veocd.sap_cntry_nm AS cntry_nm,
        NULL::CHARACTER VARYING AS mt_cust_cd,
        NULL::CHARACTER VARYING AS mt_cust_brnch_cd,
        NULL::CHARACTER VARYING AS mt_cust_brnch_nm,
        NULL::CHARACTER VARYING AS mt_item_cd,
        NULL::CHARACTER VARYING AS mt_item_nm,
        (veossf.cust_id)::CHARACTER VARYING AS sold_to,
        veocd.sap_cust_nm AS sold_to_nm,
        eocd.region,
        eocd.channel_cd AS chnl_cd,
        eocd.channel_desc AS chnl_desc,
        eocd.sub_chnl_cd,
        eocd.sub_chnl_desc,
        eocd.parent_cust_cd AS parent_customer_cd,
        eocd.parent_cust_nm AS parent_customer,
        eocd.rpt_grp_6_desc AS account_grp,
        'GENERAL TRADE'::CHARACTER VARYING AS trade_type,
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
        (
            ltrim(
                (veomd.sap_matl_num)::TEXT,
                ('0'::CHARACTER VARYING)::TEXT
            )
        )::CHARACTER VARYING AS sku,
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
        0 AS pos_qty,
        0 AS pos_gts,
        0 AS pos_item_prc,
        0 AS pos_tax,
        0 AS pos_nts,
        0 AS conv_factor,
        0 AS jj_qty_pc,
        0 AS jj_item_prc_per_pc,
        0 AS jj_gts,
        0 AS jj_vat_amt,
        0 AS jj_nts,
        epp.lst_price_unit AS local_mat_listpriceunit,
        veossf.sls_qty AS si_sls_qty,
        veossf.ret_qty AS si_ret_qty,
        veossf.sls_less_rtn_qty AS si_sls_less_rtn_qty,
        veossf.gts_val AS si_gts_val,
        veossf.ret_val AS si_ret_val,
        veossf.gts_less_rtn_val AS si_gts_less_rtn_val,
        veossf.nts_qty AS si_nts_qty,
        veossf.nts_val AS si_nts_val,
        veossf.tp_val AS si_tp_val
    FROM
        (
            SELECT edw_mv_ph_customer_dim.co_cd,
                edw_mv_ph_customer_dim.sls_org,
                edw_mv_ph_customer_dim.dstrbtr_chnl,
                edw_mv_ph_customer_dim.cust_id,
                edw_mv_ph_customer_dim.cust_nm,
                edw_mv_ph_customer_dim.address,
                edw_mv_ph_customer_dim.city,
                edw_mv_ph_customer_dim.delplant,
                edw_mv_ph_customer_dim.region,
                edw_mv_ph_customer_dim.sdistrict,
                edw_mv_ph_customer_dim.soffice,
                edw_mv_ph_customer_dim.soffice_desc,
                edw_mv_ph_customer_dim.sgroup,
                edw_mv_ph_customer_dim.sgroup_desc,
                edw_mv_ph_customer_dim.phone_no,
                edw_mv_ph_customer_dim.fax_no,
                edw_mv_ph_customer_dim.parent_cust_cd,
                edw_mv_ph_customer_dim.parent_cust_nm,
                edw_mv_ph_customer_dim.dstrbtr_grp_cd,
                edw_mv_ph_customer_dim.dstrbtr_grp_nm,
                edw_mv_ph_customer_dim.rpt_grp_1_desc,
                edw_mv_ph_customer_dim.rpt_grp_2_desc,
                edw_mv_ph_customer_dim.rpt_grp_3_desc,
                edw_mv_ph_customer_dim.rpt_grp_4_desc,
                edw_mv_ph_customer_dim.rpt_grp_5_desc,
                edw_mv_ph_customer_dim.rpt_grp_6_desc,
                edw_mv_ph_customer_dim.rpt_grp_7_desc,
                edw_mv_ph_customer_dim.channel_cd,
                edw_mv_ph_customer_dim.channel_desc,
                edw_mv_ph_customer_dim.sub_chnl_cd,
                edw_mv_ph_customer_dim.sub_chnl_desc,
                edw_mv_ph_customer_dim.region_cd,
                edw_mv_ph_customer_dim.region_nm,
                edw_mv_ph_customer_dim.province_cd,
                edw_mv_ph_customer_dim.province_nm,
                edw_mv_ph_customer_dim.mun_cd,
                edw_mv_ph_customer_dim.mun_nm,
                edw_mv_ph_customer_dim.rpt_grp_9_desc,
                edw_mv_ph_customer_dim.rpt_grp_11_desc,
                edw_mv_ph_customer_dim.rpt_grp_12_desc
            FROM edw_mv_ph_customer_dim
            WHERE
                (
                    edw_mv_ph_customer_dim.cust_id IN (
                        SELECT DISTINCT edw_mv_ph_customer_dim.cust_id
                        FROM edw_mv_ph_customer_dim
                        WHERE (
                                edw_mv_ph_customer_dim.parent_cust_cd IN (
                                    SELECT DISTINCT edw_mv_ph_customer_dim.parent_cust_cd
                                    FROM edw_mv_ph_customer_dim
                                    WHERE (
                                            edw_mv_ph_customer_dim.cust_id IN (
                                                SELECT itg_mds_ph_ref_pos_primary_sold_to.primary_soldto
                                                FROM itg_mds_ph_ref_pos_primary_sold_to
                                                WHERE (
                                                        (itg_mds_ph_ref_pos_primary_sold_to.active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT
                                                    )
                                            )
                                        )
                                )
                            )
                    )
                )
        ) eocd,
        veossf
        LEFT JOIN  veomd ON
        upper(ltrim((veomd.sap_matl_num)::TEXT,('0'::CHARACTER VARYING)::TEXT)) = upper(ltrim((veossf.item_cd)::TEXT,('0'::CHARACTER VARYING)::TEXT))
        LEFT JOIN  veocd ON
        upper(ltrim((veocd.sap_cust_id)::TEXT,('0'::CHARACTER VARYING)::TEXT)) = upper(ltrim(veossf.cust_id, ('0'::CHARACTER VARYING)::TEXT))
        LEFT JOIN
        (
            SELECT itg_mds_ph_pos_pricelist.code,
                itg_mds_ph_pos_pricelist.item_cd,
                itg_mds_ph_pos_pricelist.item_desc,
                itg_mds_ph_pos_pricelist.jj_mnth_id,
                itg_mds_ph_pos_pricelist.consumer_bar_cd,
                itg_mds_ph_pos_pricelist.shippers_bar_cd,
                itg_mds_ph_pos_pricelist.dz_per_case,
                itg_mds_ph_pos_pricelist.lst_price_case,
                itg_mds_ph_pos_pricelist.lst_price_dz,
                itg_mds_ph_pos_pricelist.lst_price_unit,
                itg_mds_ph_pos_pricelist.srp,
                itg_mds_ph_pos_pricelist.STATUS,
                itg_mds_ph_pos_pricelist.status_nm,
                itg_mds_ph_pos_pricelist.last_chg_datetime,
                itg_mds_ph_pos_pricelist.effective_from,
                itg_mds_ph_pos_pricelist.effective_to,
                itg_mds_ph_pos_pricelist.active,
                itg_mds_ph_pos_pricelist.crtd_dttm,
                itg_mds_ph_pos_pricelist.updt_dttm
            FROM itg_mds_ph_pos_pricelist
            WHERE (
                    (itg_mds_ph_pos_pricelist.active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT
                )
        ) epp ON
        (
            (
                (
                    upper(trim((epp.item_cd)::TEXT)) = upper(trim((veossf.item_cd)::TEXT))
                )
                AND (
                    trim((epp.jj_mnth_id)::TEXT) = trim(veossf.jj_mnth_id)
                )
            )
        )
    WHERE
        (
            upper(trim((eocd.cust_id)::TEXT)) = upper(ltrim(veossf.cust_id, ('0'::CHARACTER VARYING)::TEXT))
        )
),
cust as
(
    SELECT DISTINCT edw_mv_ph_customer_dim.cust_id,
        edw_mv_ph_customer_dim.dstrbtr_grp_cd
    FROM edw_mv_ph_customer_dim
    WHERE
        (
            edw_mv_ph_customer_dim.cust_id IN
            (
                SELECT DISTINCT edw_mv_ph_customer_dim.cust_id
                FROM edw_mv_ph_customer_dim
                WHERE
                    (
                        edw_mv_ph_customer_dim.parent_cust_cd IN (
                            SELECT DISTINCT edw_mv_ph_customer_dim.parent_cust_cd
                            FROM edw_mv_ph_customer_dim
                            WHERE
                                (
                                    edw_mv_ph_customer_dim.cust_id IN
                                    (
                                        SELECT itg_mds_ph_ref_pos_primary_sold_to.primary_soldto
                                        FROM itg_mds_ph_ref_pos_primary_sold_to
                                        WHERE
                                            (
                                                (
                                                    (itg_mds_ph_ref_pos_primary_sold_to.active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT
                                                )
                                                AND (
                                                    (itg_mds_ph_ref_pos_primary_sold_to.cust_cd)::TEXT = ('WAT'::CHARACTER VARYING)::TEXT
                                                )
                                            )
                                    )
                                )
                        )
                    )
            )
        )
),
set_1 as
(
    SELECT
        pos.jj_year,
        pos.jj_qtr AS jj_qrtr,
        pos.jj_mnth_id,
        pos.jj_mnth_no,
        COALESCE(pos.cntry_nm, 'Philippines'::CHARACTER VARYING) AS cntry_nm,
        COALESCE(pos.sold_to, 'NA'::CHARACTER VARYING) AS dstrbtr_grp_cd,
        pos.sold_to,
        pos.sku AS sku_cd,
        pos.sku_desc AS sku_description,
        NULL AS inv_item_cd,
        NULL AS inv_item_desc,
        pos.global_prod_franchise,
        pos.global_prod_brand,
        pos.global_prod_sub_brand,
        pos.global_prod_variant,
        pos.global_prod_segment,
        pos.global_prod_subsegment,
        pos.global_prod_category,
        pos.global_prod_subcategory,
        pos.global_put_up_desc,
        pos.si_sls_qty,
        pos.si_gts_val,
        pos.jj_qty_pc,
        pos.jj_gts,
        0 AS end_stock_qty
    FROM pos,cust
    WHERE
        (
            ((cust.cust_id)::TEXT = (pos.sold_to)::TEXT)
            AND
            (
                NOT 
                    (
                    (COALESCE(pos.sku, '#'::CHARACTER VARYING))::TEXT IN
                    (
                        SELECT
                        DISTINCT COALESCE(ltrim((mat.sap_item_cd)::TEXT,('0'::CHARACTER VARYING)::TEXT),('#'::CHARACTER VARYING)::TEXT) AS sap_matl_num
                        FROM
                            (
                                itg_ph_as_watsons_inventory inv
                                LEFT JOIN
                                (
                                    SELECT itg_mds_ph_pos_product.code,
                                        itg_mds_ph_pos_product.mnth_id,
                                        itg_mds_ph_pos_product.item_cd,
                                        itg_mds_ph_pos_product.bar_cd,
                                        itg_mds_ph_pos_product.item_nm,
                                        itg_mds_ph_pos_product.sap_item_cd,
                                        itg_mds_ph_pos_product.sap_item_desc,
                                        itg_mds_ph_pos_product.parent_cust_cd,
                                        itg_mds_ph_pos_product.parent_cust_nm,
                                        itg_mds_ph_pos_product.jnj_item_desc,
                                        itg_mds_ph_pos_product.jnj_matl_cse_barcode,
                                        itg_mds_ph_pos_product.jnj_matl_pc_barcode,
                                        itg_mds_ph_pos_product.early_bk_period,
                                        itg_mds_ph_pos_product.cust_conv_factor,
                                        itg_mds_ph_pos_product.cust_item_prc,
                                        itg_mds_ph_pos_product.jnj_matl_shipper_barcode,
                                        itg_mds_ph_pos_product.jnj_matl_consumer_barcode,
                                        itg_mds_ph_pos_product.jnj_pc_per_cust_unit,
                                        itg_mds_ph_pos_product.computed_price_per_unit,
                                        itg_mds_ph_pos_product.jj_price_per_unit,
                                        itg_mds_ph_pos_product.cust_sku_grp,
                                        itg_mds_ph_pos_product.uom,
                                        itg_mds_ph_pos_product.jnj_pc_per_cse,
                                        itg_mds_ph_pos_product.lst_period,
                                        itg_mds_ph_pos_product.cust_cd,
                                        itg_mds_ph_pos_product.cust_cd2,
                                        itg_mds_ph_pos_product.last_chg_datetime,
                                        itg_mds_ph_pos_product.effective_from,
                                        itg_mds_ph_pos_product.effective_to,
                                        itg_mds_ph_pos_product.active,
                                        itg_mds_ph_pos_product.crtd_dttm,
                                        itg_mds_ph_pos_product.updt_dttm
                                    FROM itg_mds_ph_pos_product
                                    WHERE (
                                            (
                                                upper((itg_mds_ph_pos_product.cust_cd)::TEXT) = ('WAT'::CHARACTER VARYING)::TEXT
                                            )
                                            AND (
                                                (itg_mds_ph_pos_product.active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT
                                            )
                                        )
                                ) mat ON
                                (
                                    (
                                        (
                                            ((inv.year)::TEXT || (inv.mnth_no)::TEXT) = (mat.mnth_id)::TEXT
                                        )
                                        AND ((inv.item_cd)::TEXT = (mat.item_cd)::TEXT)
                                    )
                                )
                            )
                        WHERE
                            (
                                upper((inv.group_name)::TEXT) = ('HEALTH AND FITNESS'::CHARACTER VARYING)::TEXT
                            )
                    )
                )
            )
        )
),
set_2 as
(
    SELECT
        (inv.year)::INTEGER AS jj_year,
        (inv.qrtr)::CHARACTER VARYING AS jj_qrtr,
        (inv.mnth_id)::CHARACTER VARYING AS jj_mnth_id,
        inv.month_no AS jj_mnth_no,
        'Philippines' AS cntry_nm,
        inv.cust_cd AS dstrbtr_grp_cd,
        inv.cust_cd AS sold_to,
        (inv.sap_matl_num)::CHARACTER VARYING AS sku_cd,
        veomd.sap_mat_desc AS sku_description,
        inv.item_cd AS inv_item_cd,
        inv.item_desc AS inv_item_desc,
        veomd.gph_prod_frnchse AS global_prod_franchise,
        veomd.gph_prod_brnd AS global_prod_brand,
        veomd.gph_prod_sub_brnd AS global_prod_sub_brand,
        veomd.gph_prod_vrnt AS global_prod_variant,
        veomd.gph_prod_sgmnt AS global_prod_segment,
        veomd.gph_prod_subsgmnt AS global_prod_subsegment,
        veomd.gph_prod_ctgry AS global_prod_category,
        veomd.gph_prod_subctgry AS global_prod_subcategory,
        veomd.gph_prod_put_up_desc AS global_put_up_desc,
        0 AS si_sls_qty,
        0 AS si_gts_val,
        0 AS jj_qty_pc,
        0 AS jj_gts,
        inv.end_stock_qty
    FROM
        (
            (
                SELECT vppi."year" as year,
                    vppi.mnth_no,
                    vppi.inv_week,
                    vppi.inv_date,
                    vppi.cust_cd,
                    vppi.item_cd,
                    vppi.item_desc,
                    vppi.sap_matl_num,
                    vppi.end_stock_qty,
                    vppi.end_stock_val,
                    td.qrtr,
                    td.mnth_no AS month_no,
                    td.mnth_id
                FROM edw_vw_ph_pos_inventory vppi,
                (
                    SELECT DISTINCT edw_vw_os_time_dim."year" AS jj_year,
                        edw_vw_os_time_dim.qrtr_no,
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.mnth_desc,
                        edw_vw_os_time_dim.mnth_no,
                        edw_vw_os_time_dim.wk
                    FROM edw_vw_os_time_dim
                ) td
                WHERE
                (
                        (
                            ltrim(
                                (vppi.inv_week)::TEXT,
                                ('0'::CHARACTER VARYING)::TEXT
                            ) = ((td.wk)::CHARACTER VARYING)::TEXT
                        )
                        AND (
                            (vppi."year")::TEXT = ((td.jj_year)::CHARACTER VARYING)::TEXT
                        )
                )
            ) inv
            LEFT JOIN
            veomd ON
            ltrim(inv.sap_matl_num, ('0'::CHARACTER VARYING)::TEXT) = upper(ltrim((veomd.sap_matl_num)::TEXT,((0)::CHARACTER VARYING)::TEXT))
        )
),
transformed as
(
    select * from set_1
    union all
    select * from set_2
),
final as
(
    select
        jj_year,
        jj_qrtr,
        jj_mnth_id,
        jj_mnth_no,
        cntry_nm,
        dstrbtr_grp_cd,
        sold_to,
        sku_cd,
        sku_description,
        inv_item_cd,
        inv_item_desc,
        global_prod_franchise,
        global_prod_brand,
        global_prod_sub_brand,
        global_prod_variant,
        global_prod_segment,
        global_prod_subsegment,
        global_prod_category,
        global_prod_subcategory,
        global_put_up_desc,
        si_sls_qty,
        si_gts_val,
        jj_qty_pc,
        jj_gts,
        end_stock_qty
    from transformed
)
select * from final