--Import CTEs
with edw_vw_my_curr_dim as (
    select * from {{ ref('mysedw_integration__edw_vw_my_curr_dim') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_my_curr_prev_sellin_fact as (
    select * from {{ ref('mysedw_integration__edw_vw_my_curr_prev_sellin_fact') }}
),
edw_vw_my_customer_dim as (
    select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
edw_vw_my_material_dim as (
    select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
),
edw_vw_my_orders_fact as (
    select * from {{ ref('mysedw_integration__edw_vw_my_orders_fact') }}
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
itg_my_trgts as (
    select * from {{ ref('mysitg_integration__itg_my_trgts') }}
),
itg_my_le_trgt as (
    select * from {{ ref('mysitg_integration__itg_my_le_trgt') }}
),
itg_my_accruals as (
    select * from {{ ref('mysitg_integration__itg_my_accruals') }}
),
itg_my_afgr as (
    select * from {{ ref('mysitg_integration__itg_my_afgr') }}
),

--Logical CTEs
ds_primary as (
    SELECT 'Primary' AS data_src,
                            veossf.jj_year,
                            (veossf.jj_qtr)::character varying AS jj_qtr,
                            (veossf.jj_mnth_id)::character varying AS jj_mnth_id,
                            veossf.jj_mnth_no,
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
                            veocurd.from_ccy AS from_crncy,
                            veocurd.to_ccy AS to_crncy,
                            veocurd.exch_rate,
                            ((veossf.base_val * veocurd.exch_rate))::numeric(20, 4) AS base_val,
                            (veossf.sls_qty)::numeric(20, 4) AS sls_qty,
                            (veossf.ret_qty)::numeric(20, 4) AS ret_qty,
                            (veossf.sls_less_rtn_qty)::numeric(20, 4) AS sls_less_rtn_qty,
                            ((veossf.gts_val * veocurd.exch_rate))::numeric(20, 4) AS gts_val,
                            ((veossf.ret_val * veocurd.exch_rate))::numeric(20, 4) AS ret_val,
                            ((veossf.gts_less_rtn_val * veocurd.exch_rate))::numeric(20, 4) AS gts_less_rtn_val,
                            ((veossf.trdng_term_val * veocurd.exch_rate))::numeric(20, 4) AS trdng_term_val,
                            ((veossf.tp_val * veocurd.exch_rate))::numeric(20, 4) AS tp_val,
                            ((veossf.trde_prmtn_val * veocurd.exch_rate))::numeric(20, 4) AS trde_prmtn_val,
                            ((veossf.nts_val * veocurd.exch_rate))::numeric(20, 4) AS nts_val,
                            (veossf.nts_qty)::numeric(20, 4) AS nts_qty,
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
                            CASE
                                WHEN (
                                    (veossf.jj_mnth_id = curr_mnth.mnth_id)
                                    OR (
                                        veossf.jj_mnth_id = "substring"(
                                            replace(
                                                (
                                                    (
                                                        dateadd(
                                                            month,
                                                            (- (12)::bigint),
                                                            to_date(
                                                                (
                                                                    concat(
                                                                        (curr_mnth.mnth_id)::text,
                                                                        ('01'::character varying)::text
                                                                    )
                                                                ),
                                                                'yyyymmdd'
                                                            )
                                                        ) -- )
                                                    )::character varying
                                                )::text,
                                                ('-'::character varying)::text,
                                                (''::character varying)::text
                                            ),
                                            0,
                                            7
                                        )
                                    )
                                ) THEN curr_details.elapsed_time
                                ELSE ((1)::numeric)::numeric(18, 0)
                            END AS curr_prd_elapsed,
                            CASE
                                WHEN (
                                    (veossf.jj_mnth_id = curr_mnth.mnth_id)
                                    OR (
                                        veossf.jj_mnth_id = "substring"(
                                            replace(
                                                (
                                                    (
                                                        dateadd(
                                                            month,
                                                            (- (12)::bigint),
                                                            to_date(
                                                                (
                                                                    concat(
                                                                        (curr_mnth.mnth_id)::text,
                                                                        ('01'::character varying)::text
                                                                    )
                                                                ),
                                                                'yyyymmdd'
                                                            )
                                                        )
                                                    )::character varying
                                                )::text,
                                                ('-'::character varying)::text,
                                                (''::character varying)::text
                                            ),
                                            0,
                                            7
                                        )
                                    )
                                ) THEN 'Y'::character varying
                                ELSE 'N'::character varying
                            END AS elapsed_flag,
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
                            (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
                        FROM (
                                SELECT d.cntry_key,
                                    d.cntry_nm,
                                    d.rate_type,
                                    d.from_ccy,
                                    d.to_ccy,
                                    d.valid_date,
                                    d.jj_year,
                                    d.start_period,
                                    CASE
                                        WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                                        ELSE d.end_mnth_id
                                    END AS end_period,
                                    d.exch_rate
                                FROM (
                                        SELECT a.cntry_key,
                                            a.cntry_nm,
                                            a.rate_type,
                                            a.from_ccy,
                                            a.to_ccy,
                                            a.valid_date,
                                            a.jj_year,
                                            min((a.jj_mnth_id)::text) AS start_period,
                                            "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                                            a.exch_rate
                                        FROM EDW_VW_my_CURR_DIM a
                                        WHERE (
                                                (a.cntry_key)::text = ('MY'::character varying)::text
                                            )
                                        GROUP BY a.cntry_key,
                                            a.cntry_nm,
                                            a.rate_type,
                                            a.from_ccy,
                                            a.to_ccy,
                                            a.valid_date,
                                            a.jj_year,
                                            a.exch_rate
                                    ) d,
                                    (
                                        SELECT "max"((a.jj_mnth_id)::text) AS max_period
                                        FROM EDW_VW_my_CURR_DIM a
                                        WHERE (
                                                (a.cntry_key)::text = ('MY'::character varying)::text
                                            )
                                    ) b
                            ) veocurd,
                            (
                                SELECT a.mnth_id,
                                    a.cal_date_id
                                FROM edw_vw_os_time_dim a
                                WHERE (
                                        a.cal_date_id = replace(
                                            current_timestamp()::date,
                                            ('-'::character varying)::text,
                                            (''::character varying)::text
                                        )
                                    )
                            ) cur_period_sgt,
                            (
                                SELECT a.mnth_id
                                FROM edw_vw_os_time_dim a
                                WHERE (
                                        replace(
                                            current_timestamp()::date,
                                            ('-'::character varying)::text,
                                            (''::character varying)::text
                                        ) = a.cal_date_id
                                    )
                            ) curr_mnth,
                            (
                                SELECT replace(
                                        current_timestamp()::date,
                                        ('-'::character varying)::text,
                                        (''::character varying)::text
                                    ) AS curr_dt,
                                    a.mnth_id,
                                    a.dow,
                                    a.rank_no,
                                    a.max_day,
                                    ((a.rank_no / a.max_day))::numeric(20, 6) AS elapsed_time
                                FROM (
                                        SELECT derived_table1.mnth_id,
                                            derived_table1.cal_date_id,
                                            derived_table1.dow,
                                            ((derived_table1.rank_no)::numeric)::numeric(18, 0) AS rank_no,
                                            (
                                                (
                                                    "max"(derived_table1.rank_no) OVER(
                                                        PARTITION BY derived_table1.mnth_id
                                                        order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                    )
                                                )::numeric
                                            )::numeric(18, 0) AS max_day
                                        FROM (
                                                SELECT edw_vw_os_time_dim.mnth_id,
                                                    edw_vw_os_time_dim.cal_date_id,
                                                    dayofweek ((edw_vw_os_time_dim.cal_date_id)::date)
                                                    AS dow,
                                                    dense_rank() OVER(
                                                        PARTITION BY edw_vw_os_time_dim.mnth_id
                                                        ORDER BY edw_vw_os_time_dim.cal_date_id
                                                    ) AS rank_no
                                                FROM edw_vw_os_time_dim
                                                WHERE (
                                                        (
                                                            dayofweek (edw_vw_os_time_dim.cal_date_id::date)
                                                            <> (0)::double precision
                                                        )
                                                        AND (
                                                            dayofweek (edw_vw_os_time_dim.cal_date_id::date)
                                                            <> (6)::double precision
                                                        )
                                                    )
                                            ) derived_table1
                                    ) a
                                WHERE CASE
                                        WHEN (
                                            (
                                                dayofweek (
                                                    (
                                                        current_timestamp()
                                                    )::date
                                                ) 
                                                <> (0)::double precision
                                            )
                                            AND (
                                                dayofweek (
                                                    (
                                                        current_timestamp() 
                                                    )::date
                                                )
                                                <> (6)::double precision
                                            )
                                        ) THEN (
                                            a.cal_date_id = replace(
                                                current_timestamp()::date,
                                                ('-'::character varying)::text,
                                                (''::character varying)::text
                                            )
                                        )
                                        WHEN (
                                            dayofweek (
                                                (
                                                    current_timestamp() 
                                                )::date
                                            )
                                            = (0)::double precision
                                        ) THEN (
                                            a.cal_date_id = replace(
                                                (
                                                    (
                                                        dateadd(
                                                            'day',
                                                            (- (2)::bigint),
                                                            current_timestamp()::date
                                                        ) 
                                                    )::character varying
                                                )::text,
                                                ('-'::character varying)::text,
                                                (''::character varying)::text
                                            )
                                        )
                                        WHEN (
                                            dayofweek (
                                                (
                                                    current_timestamp()
                                                )::date
                                            )
                                            = (6)::double precision
                                        ) THEN (
                                            a.cal_date_id = replace(
                                                (
                                                    (
                                                        dateadd(
                                                            'day',
                                                            (- (1)::bigint),
                                                            current_timestamp()::date
                                                        )
                                                    )::character varying
                                                )::text,
                                                ('-'::character varying)::text,
                                                (''::character varying)::text
                                            )
                                        )
                                        ELSE NULL::boolean
                                    END
                            ) curr_details,
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    SELECT veotd.jj_year,
                                                        veotd.jj_qtr,
                                                        veotd.jj_mnth_id,
                                                        veotd.jj_mnth_no,
                                                        veossf1.cntry_nm,
                                                        ltrim(
                                                            (veossf1.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        ) AS item_cd,
                                                        ltrim(
                                                            (veossf1.cust_id)::text,
                                                            ('0'::character varying)::text
                                                        ) AS cust_id,
                                                        veossf1.acct_no,
                                                        sum(veossf1.base_val) AS base_val,
                                                        sum(veossf1.sls_qty) AS sls_qty,
                                                        sum(veossf1.ret_qty) AS ret_qty,
                                                        sum(veossf1.sls_less_rtn_qty) AS sls_less_rtn_qty,
                                                        sum(veossf1.gts_val) AS gts_val,
                                                        sum(veossf1.ret_val) AS ret_val,
                                                        sum(veossf1.gts_less_rtn_val) AS gts_less_rtn_val,
                                                        sum(veossf1.trdng_term_val) AS trdng_term_val,
                                                        sum(veossf1.tp_val) AS tp_val,
                                                        sum(veossf1.trde_prmtn_val) AS trde_prmtn_val,
                                                        sum(veossf1.nts_val) AS nts_val,
                                                        sum(veossf1.nts_qty) AS nts_qty,
                                                        veossf1.is_curr
                                                    FROM edw_vw_my_curr_prev_sellin_fact veossf1,
                                                        (
                                                            SELECT a."year" AS jj_year,
                                                                a.qrtr_no,
                                                                a.qrtr AS jj_qtr,
                                                                a.mnth_id AS jj_mnth_id,
                                                                a.mnth_desc,
                                                                a.mnth_no AS jj_mnth_no,
                                                                a.mnth_shrt,
                                                                a.mnth_long
                                                            FROM edw_vw_os_time_dim a
                                                            GROUP BY a."year",
                                                                a.qrtr_no,
                                                                a.qrtr,
                                                                a.mnth_id,
                                                                a.mnth_desc,
                                                                a.mnth_no,
                                                                a.mnth_shrt,
                                                                a.mnth_long
                                                        ) veotd
                                                    WHERE (
                                                            (
                                                                (veossf1.cntry_nm)::text = ('MY'::character varying)::text
                                                            )
                                                            AND (
                                                                trim((veossf1.jj_mnth_id)::text) = (
                                                                    (
                                                                        (veotd.jj_mnth_id)::numeric(18, 0)
                                                                    )::character varying
                                                                )::text
                                                            )
                                                        )
                                                    GROUP BY veotd.jj_year,
                                                        veotd.jj_qtr,
                                                        veotd.jj_mnth_id,
                                                        veotd.jj_mnth_no,
                                                        veossf1.cntry_nm,
                                                        veossf1.item_cd,
                                                        veossf1.cust_id,
                                                        veossf1.acct_no,
                                                        veossf1.is_curr
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
                        WHERE (
                                (
                                    (veossf.jj_mnth_id >= veocurd.start_period)
                                    AND (veossf.jj_mnth_id <= veocurd.end_period)
                                )
                                AND (veossf.jj_mnth_id <= curr_mnth.mnth_id)
                        )
),
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
                            veocurd.from_ccy AS from_crncy,
                            veocurd.to_ccy AS to_crncy,
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
                            (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
                        FROM (
                                SELECT a.mnth_id,
                                    a.cal_date_id
                                FROM edw_vw_os_time_dim a
                                WHERE (
                                        a.cal_date_id = replace(
                                            current_timestamp(),
                                            ('-'::character varying)::text,
                                            (''::character varying)::text
                                        )
                                    )
                            ) as cur_period_sgt,
                            (
                                SELECT d.cntry_key,
                                    d.cntry_nm,
                                    d.rate_type,
                                    d.from_ccy,
                                    d.to_ccy,
                                    d.valid_date,
                                    d.jj_year,
                                    d.start_period,
                                    CASE
                                        WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                                        ELSE d.end_mnth_id
                                    END AS end_period,
                                    d.exch_rate
                                FROM (
                                        SELECT a.cntry_key,
                                            a.cntry_nm,
                                            a.rate_type,
                                            a.from_ccy,
                                            a.to_ccy,
                                            a.valid_date,
                                            a.jj_year,
                                            min((a.jj_mnth_id)::text) AS start_period,
                                            "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                                            a.exch_rate
                                        FROM EDW_VW_my_CURR_DIM a
                                        WHERE (
                                                (a.cntry_key)::text = ('MY'::character varying)::text
                                            )
                                        GROUP BY a.cntry_key,
                                            a.cntry_nm,
                                            a.rate_type,
                                            a.from_ccy,
                                            a.to_ccy,
                                            a.valid_date,
                                            a.jj_year,
                                            a.exch_rate
                                    ) d,
                                    (
                                        SELECT "max"((a.jj_mnth_id)::text) AS max_period
                                        FROM EDW_VW_my_CURR_DIM a
                                        WHERE (
                                                (a.cntry_key)::text = ('MY'::character varying)::text
                                            )
                                    ) b
                            ) veocurd,
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    SELECT veotd.jj_year,
                                                        veotd.jj_qtr,
                                                        veotd.jj_mnth_id,
                                                        veotd.jj_mnth_no,
                                                        veossf1.cntry_nm,
                                                        ltrim(
                                                            (veossf1.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        ) AS item_cd,
                                                        ltrim(
                                                            (veossf1.cust_id)::text,
                                                            ('0'::character varying)::text
                                                        ) AS cust_id,
                                                        veossf1.acct_no,
                                                        sum(veossf1.base_val) AS base_val,
                                                        sum(veossf1.sls_qty) AS sls_qty,
                                                        sum(veossf1.ret_qty) AS ret_qty,
                                                        sum(veossf1.sls_less_rtn_qty) AS sls_less_rtn_qty,
                                                        sum(veossf1.gts_val) AS gts_val,
                                                        sum(veossf1.ret_val) AS ret_val,
                                                        sum(veossf1.gts_less_rtn_val) AS gts_less_rtn_val,
                                                        sum(veossf1.trdng_term_val) AS trdng_term_val,
                                                        sum(veossf1.tp_val) AS tp_val,
                                                        sum(veossf1.trde_prmtn_val) AS trde_prmtn_val,
                                                        sum(veossf1.nts_val) AS nts_val,
                                                        sum(veossf1.nts_qty) AS nts_qty,
                                                        veossf1.is_curr
                                                    FROM edw_vw_my_curr_prev_sellin_fact veossf1,
                                                        (
                                                            SELECT a."year" AS jj_year,
                                                                a.qrtr_no,
                                                                a.qrtr AS jj_qtr,
                                                                a.mnth_id AS jj_mnth_id,
                                                                a.mnth_desc,
                                                                a.mnth_no AS jj_mnth_no,
                                                                a.mnth_shrt,
                                                                a.mnth_long
                                                            FROM edw_vw_os_time_dim a
                                                            GROUP BY a."year",
                                                                a.qrtr_no,
                                                                a.qrtr,
                                                                a.mnth_id,
                                                                a.mnth_desc,
                                                                a.mnth_no,
                                                                a.mnth_shrt,
                                                                a.mnth_long
                                                        ) veotd
                                                    WHERE (
                                                            (
                                                                (veossf1.cntry_nm)::text = ('MY'::character varying)::text
                                                            )
                                                            AND (
                                                                trim((veossf1.jj_mnth_id)::text) = (
                                                                    (
                                                                        (veotd.jj_mnth_id)::numeric(18, 0)
                                                                    )::character varying
                                                                )::text
                                                            )
                                                        )
                                                    GROUP BY veotd.jj_year,
                                                        veotd.jj_qtr,
                                                        veotd.jj_mnth_id,
                                                        veotd.jj_mnth_no,
                                                        veossf1.cntry_nm,
                                                        veossf1.item_cd,
                                                        veossf1.cust_id,
                                                        veossf1.acct_no,
                                                        veossf1.is_curr
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
                        WHERE (
                                (veossf.jj_mnth_id >= veocurd.start_period)
                                AND (veossf.jj_mnth_id <= veocurd.end_period)
                            )
),
ds_targets as (
    SELECT 'TARGETS' AS data_src,
                        veotd.jj_year,
                        (veotd.jj_qtr)::character varying AS jj_qtr,
                        imt.jj_mnth_id,
                        veotd.jj_mnth_no,
                        veocd.sap_cntry_cd,
                        'Malaysia' AS cntry_nm,
                        NULL AS acct_no,
                        NULL AS acct_desc,
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
                        NULL AS matl_num,
                        NULL AS matl_desc,
                        NULL AS matl_desc2,
                        NULL AS matl_desc3,
                        NULL AS frnchse_desc,
                        NULL AS brnd_desc,
                        NULL AS vrnt_desc,
                        NULL AS sku_ean_num,
                        NULL AS global_mat_region,
                        CASE
                            WHEN (
                                (imt.sub_segment)::text = ('ALL'::character varying)::text
                            ) THEN veomd.gph_prod_frnchse
                            ELSE veomd1.gph_prod_frnchse
                        END AS global_prod_franchise,
                        CASE
                            WHEN (
                                (imt.sub_segment)::text = ('ALL'::character varying)::text
                            ) THEN veomd.gph_prod_brnd
                            ELSE veomd1.gph_prod_brnd
                        END AS global_prod_brand,
                        NULL AS global_prod_variant,
                        NULL AS global_prod_put_up_cd,
                        NULL AS global_put_up_desc,
                        NULL AS put_up_desc,
                        NULL AS sap_prod_mnr_cd,
                        NULL AS sap_prod_mnr_desc,
                        NULL AS sap_prod_hier_cd,
                        NULL AS sap_prod_hier_desc,
                        NULL AS global_prod_sub_brand,
                        NULL AS global_prod_need_state,
                        NULL AS global_prod_category,
                        NULL AS global_prod_subcategory,
                        NULL AS global_prod_segment,
                        veomd1.gph_prod_subsgmnt AS global_prod_subsegment,
                        NULL AS global_prod_size,
                        NULL AS global_prod_size_uom,
                        NULL AS sku_launch_dt,
                        NULL AS qty_shipper_pc,
                        NULL AS prft_ctr,
                        NULL AS shelf_life,
                        NULL AS npi_ind,
                        NULL AS npi_strt_period,
                        NULL AS hero_ind,
                        NULL AS ciw_ctgry,
                        NULL AS ciw_buckt1,
                        NULL AS ciw_buckt2,
                        NULL AS bravo_desc1,
                        NULL AS bravo_desc2,
                        NULL AS acct_type,
                        NULL AS acct_type1,
                        NULL AS from_crncy,
                        veocurd.to_ccy AS to_crncy,
                        veocurd.exch_rate,
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
                        imt.trgt_type,
                        imt.trgt_val_type,
                        (imt.trgt_val * veocurd.exch_rate) AS trgt_val,
                        0 AS accrual_val,
                        0 AS le_trgt_val_wk1,
                        0 AS le_trgt_val_wk2,
                        0 AS le_trgt_val_wk3,
                        0 AS le_trgt_val_wk4,
                        0 AS le_trgt_val_wk5,
                        0 AS curr_prd_elapsed,
                        NULL AS elapsed_flag,
                        'Y' AS is_curr,
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
                        (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
                    FROM (
                            SELECT a.mnth_id,
                                a.cal_date_id
                            FROM edw_vw_os_time_dim a
                            WHERE (
                                    a.cal_date_id = replace(
                                        current_timestamp(),
                                        ('-'::character varying)::text,
                                        (''::character varying)::text
                                    )
                                )
                        ) cur_period_sgt,
                        (
                            SELECT a."year" AS jj_year,
                                a.qrtr_no,
                                a.qrtr AS jj_qtr,
                                a.mnth_id AS jj_mnth_id,
                                a.mnth_desc,
                                a.mnth_no AS jj_mnth_no,
                                a.mnth_shrt,
                                a.mnth_long
                            FROM edw_vw_os_time_dim a
                            GROUP BY a."year",
                                a.qrtr_no,
                                a.qrtr,
                                a.mnth_id,
                                a.mnth_desc,
                                a.mnth_no,
                                a.mnth_shrt,
                                a.mnth_long
                        ) veotd,
                        (
                            SELECT d.cntry_key,
                                d.cntry_nm,
                                d.rate_type,
                                d.from_ccy,
                                d.to_ccy,
                                d.valid_date,
                                d.jj_year,
                                d.start_period,
                                CASE
                                    WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                                    ELSE d.end_mnth_id
                                END AS end_period,
                                d.exch_rate
                            FROM (
                                    SELECT a.cntry_key,
                                        a.cntry_nm,
                                        a.rate_type,
                                        a.from_ccy,
                                        a.to_ccy,
                                        a.valid_date,
                                        a.jj_year,
                                        min((a.jj_mnth_id)::text) AS start_period,
                                        "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                                        a.exch_rate
                                    FROM EDW_VW_my_CURR_DIM a
                                    WHERE (
                                            (a.cntry_key)::text = ('MY'::character varying)::text
                                        )
                                    GROUP BY a.cntry_key,
                                        a.cntry_nm,
                                        a.rate_type,
                                        a.from_ccy,
                                        a.to_ccy,
                                        a.valid_date,
                                        a.jj_year,
                                        a.exch_rate
                                ) d,
                                (
                                    SELECT "max"((a.jj_mnth_id)::text) AS max_period
                                    FROM EDW_VW_my_CURR_DIM a
                                    WHERE (
                                            (a.cntry_key)::text = ('MY'::character varying)::text
                                        )
                                ) b
                        ) veocurd,
                        (
                            (
                                (
                                    (
                                        itg_my_trgts imt
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
                                                ) = trim((imt.cust_id)::text)
                                            )
                                        )
                                    )
                                    LEFT JOIN itg_my_customer_dim imcd ON (
                                        (
                                            trim((imcd.cust_id)::text) = trim((imt.cust_id)::text)
                                        )
                                    )
                                )
                                LEFT JOIN (
                                    SELECT DISTINCT EDW_VW_my_MATERIAL_DIM.gph_prod_frnchse,
                                        EDW_VW_my_MATERIAL_DIM.gph_prod_brnd
                                    FROM EDW_VW_my_MATERIAL_DIM
                                    WHERE (
                                            (EDW_VW_my_MATERIAL_DIM.cntry_key)::text = ('MY'::character varying)::text
                                        )
                                ) veomd ON (
                                    CASE
                                        WHEN (
                                            (imt.sub_segment)::text = ('ALL'::character varying)::text
                                        ) THEN (
                                            trim(upper((veomd.gph_prod_brnd)::text)) = trim(upper((imt.brnd_desc)::text))
                                        )
                                        ELSE NULL::boolean
                                    END
                                )
                            )
                            LEFT JOIN (
                                SELECT DISTINCT EDW_VW_my_MATERIAL_DIM.gph_prod_frnchse,
                                    EDW_VW_my_MATERIAL_DIM.gph_prod_brnd,
                                    EDW_VW_my_MATERIAL_DIM.gph_prod_subsgmnt
                                FROM EDW_VW_my_MATERIAL_DIM
                                WHERE (
                                        (EDW_VW_my_MATERIAL_DIM.cntry_key)::text = ('MY'::character varying)::text
                                    )
                            ) veomd1 ON (
                                CASE
                                    WHEN (
                                        (imt.sub_segment)::text <> ('ALL'::character varying)::text
                                    ) THEN (
                                        (
                                            trim(upper((veomd1.gph_prod_subsgmnt)::text)) = trim(upper((imt.sub_segment)::text))
                                        )
                                        AND (
                                            trim(upper((veomd1.gph_prod_brnd)::text)) = trim(upper((imt.brnd_desc)::text))
                                        )
                                    )
                                    ELSE NULL::boolean
                                END
                            )
                        )
                    WHERE (
                            (
                                (veotd.jj_mnth_id = (imt.jj_mnth_id)::text)
                                AND ((imt.jj_mnth_id)::text >= veocurd.start_period)
                            )
                            AND ((imt.jj_mnth_id)::text <= veocurd.end_period)
                        )
),
ds_targets_null as (
    SELECT 'Targets_null' AS data_src,
                    NULL AS jj_year,
                    NULL AS jj_qtr,
                    NULL AS jj_mnth_id,
                    NULL AS jj_mnth_no,
                    veocd.sap_cntry_cd,
                    'Malaysia' AS cntry_nm,
                    NULL AS acct_no,
                    NULL AS acct_desc,
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
                    NULL AS matl_num,
                    NULL AS matl_desc,
                    NULL AS matl_desc2,
                    NULL AS matl_desc3,
                    NULL AS frnchse_desc,
                    NULL AS brnd_desc,
                    NULL AS vrnt_desc,
                    NULL AS sku_ean_num,
                    NULL AS global_mat_region,
                    CASE
                        WHEN (
                            (imt.sub_segment)::text = ('ALL'::character varying)::text
                        ) THEN veomd.gph_prod_frnchse
                        ELSE veomd1.gph_prod_frnchse
                    END AS global_prod_franchise,
                    CASE
                        WHEN (
                            (imt.sub_segment)::text = ('ALL'::character varying)::text
                        ) THEN veomd.gph_prod_brnd
                        ELSE veomd1.gph_prod_brnd
                    END AS global_prod_brand,
                    NULL AS global_prod_variant,
                    NULL AS global_prod_put_up_cd,
                    NULL AS global_put_up_desc,
                    NULL AS put_up_desc,
                    NULL AS sap_prod_mnr_cd,
                    NULL AS sap_prod_mnr_desc,
                    NULL AS sap_prod_hier_cd,
                    NULL AS sap_prod_hier_desc,
                    NULL AS global_prod_sub_brand,
                    NULL AS global_prod_need_state,
                    NULL AS global_prod_category,
                    NULL AS global_prod_subcategory,
                    NULL AS global_prod_segment,
                    veomd1.gph_prod_subsgmnt AS global_prod_subsegment,
                    NULL AS global_prod_size,
                    NULL AS global_prod_size_uom,
                    NULL AS sku_launch_dt,
                    NULL AS qty_shipper_pc,
                    NULL AS prft_ctr,
                    NULL AS shelf_life,
                    NULL AS npi_ind,
                    NULL AS npi_strt_period,
                    NULL AS hero_ind,
                    NULL AS ciw_ctgry,
                    NULL AS ciw_buckt1,
                    NULL AS ciw_buckt2,
                    NULL AS bravo_desc1,
                    NULL AS bravo_desc2,
                    NULL AS acct_type,
                    NULL AS acct_type1,
                    veocurd.from_ccy AS from_crncy,
                    veocurd.to_ccy AS to_crncy,
                    NULL AS exch_rate,
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
                    imt.trgt_type,
                    imt.trgt_val_type,
                    0 AS trgt_val,
                    0 AS accrual_val,
                    0 AS le_trgt_val_wk1,
                    0 AS le_trgt_val_wk2,
                    0 AS le_trgt_val_wk3,
                    0 AS le_trgt_val_wk4,
                    0 AS le_trgt_val_wk5,
                    0 AS curr_prd_elapsed,
                    NULL AS elapsed_flag,
                    'Y' AS is_curr,
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
                    (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
                FROM (
                        SELECT a.mnth_id,
                            a.cal_date_id
                        FROM edw_vw_os_time_dim a
                        WHERE (
                                a.cal_date_id = replace(
                                    current_timestamp(),
                                    ('-'::character varying)::text,
                                    (''::character varying)::text
                                )
                            )
                    ) cur_period_sgt,
                    (
                        SELECT a."year" AS jj_year,
                            a.qrtr_no,
                            a.qrtr AS jj_qtr,
                            a.mnth_id AS jj_mnth_id,
                            a.mnth_desc,
                            a.mnth_no AS jj_mnth_no,
                            a.mnth_shrt,
                            a.mnth_long
                        FROM edw_vw_os_time_dim a
                        GROUP BY a."year",
                            a.qrtr_no,
                            a.qrtr,
                            a.mnth_id,
                            a.mnth_desc,
                            a.mnth_no,
                            a.mnth_shrt,
                            a.mnth_long
                    ) veotd,
                    (
                        SELECT d.cntry_key,
                            d.cntry_nm,
                            d.rate_type,
                            d.from_ccy,
                            d.to_ccy,
                            d.valid_date,
                            d.jj_year,
                            d.start_period,
                            CASE
                                WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                                ELSE d.end_mnth_id
                            END AS end_period,
                            d.exch_rate
                        FROM (
                                SELECT a.cntry_key,
                                    a.cntry_nm,
                                    a.rate_type,
                                    a.from_ccy,
                                    a.to_ccy,
                                    a.valid_date,
                                    a.jj_year,
                                    min((a.jj_mnth_id)::text) AS start_period,
                                    "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                                    a.exch_rate
                                FROM EDW_VW_my_CURR_DIM a
                                WHERE (
                                        (a.cntry_key)::text = ('MY'::character varying)::text
                                    )
                                GROUP BY a.cntry_key,
                                    a.cntry_nm,
                                    a.rate_type,
                                    a.from_ccy,
                                    a.to_ccy,
                                    a.valid_date,
                                    a.jj_year,
                                    a.exch_rate
                            ) d,
                            (
                                SELECT "max"((a.jj_mnth_id)::text) AS max_period
                                FROM EDW_VW_my_CURR_DIM a
                                WHERE (
                                        (a.cntry_key)::text = ('MY'::character varying)::text
                                    )
                            ) b
                    ) veocurd,
                    (
                        (
                            (
                                (
                                    (
                                        SELECT itg_my_trgts.cust_id,
                                            itg_my_trgts.cust_nm,
                                            itg_my_trgts.brnd_desc,
                                            itg_my_trgts.sub_segment,
                                            itg_my_trgts.jj_year,
                                            itg_my_trgts.jj_mnth_id,
                                            itg_my_trgts.trgt_type,
                                            itg_my_trgts.trgt_val_type,
                                            itg_my_trgts.trgt_val,
                                            itg_my_trgts.cdl_dttm,
                                            itg_my_trgts.crtd_dttm,
                                            itg_my_trgts.updt_dttm
                                        FROM itg_my_trgts
                                        WHERE (
                                                (itg_my_trgts.trgt_type)::text = ('BUF'::character varying)::text
                                            )
                                    ) imt
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
                                            ) = trim((imt.cust_id)::text)
                                        )
                                    )
                                )
                                LEFT JOIN itg_my_customer_dim imcd ON (
                                    (
                                        trim((imcd.cust_id)::text) = trim((imt.cust_id)::text)
                                    )
                                )
                            )
                            LEFT JOIN (
                                SELECT DISTINCT EDW_VW_my_MATERIAL_DIM.gph_prod_frnchse,
                                    EDW_VW_my_MATERIAL_DIM.gph_prod_brnd
                                FROM EDW_VW_my_MATERIAL_DIM
                                WHERE (
                                        (EDW_VW_my_MATERIAL_DIM.cntry_key)::text = ('MY'::character varying)::text
                                    )
                            ) veomd ON (
                                CASE
                                    WHEN (
                                        (imt.sub_segment)::text = ('ALL'::character varying)::text
                                    ) THEN (
                                        trim(upper((veomd.gph_prod_brnd)::text)) = trim(upper((imt.brnd_desc)::text))
                                    )
                                    ELSE NULL::boolean
                                END
                            )
                        )
                        LEFT JOIN (
                            SELECT DISTINCT EDW_VW_my_MATERIAL_DIM.gph_prod_frnchse,
                                EDW_VW_my_MATERIAL_DIM.gph_prod_brnd,
                                EDW_VW_my_MATERIAL_DIM.gph_prod_subsgmnt
                            FROM EDW_VW_my_MATERIAL_DIM
                            WHERE (
                                    (EDW_VW_my_MATERIAL_DIM.cntry_key)::text = ('MY'::character varying)::text
                                )
                        ) veomd1 ON (
                            CASE
                                WHEN (
                                    (imt.sub_segment)::text <> ('ALL'::character varying)::text
                                ) THEN (
                                    (
                                        trim(upper((veomd1.gph_prod_subsgmnt)::text)) = trim(upper((imt.sub_segment)::text))
                                    )
                                    AND (
                                        trim(upper((veomd1.gph_prod_brnd)::text)) = trim(upper((imt.brnd_desc)::text))
                                    )
                                )
                                ELSE NULL::boolean
                            END
                        )
                    )
                WHERE (
                        (
                            (veotd.jj_mnth_id = (imt.jj_mnth_id)::text)
                            AND ((imt.jj_mnth_id)::text >= veocurd.start_period)
                        )
                        AND ((imt.jj_mnth_id)::text <= veocurd.end_period)
                    )
),
ds_le_targets as (
    SELECT 'LE_Targets' AS data_src,
                veotd.jj_year,
                (veotd.jj_qtr)::character varying AS jj_qtr,
                imt.jj_mnth_id,
                veotd.jj_mnth_no,
                veocd.sap_cntry_cd,
                'Malaysia' AS cntry_nm,
                NULL AS acct_no,
                NULL AS acct_desc,
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
                NULL AS matl_num,
                NULL AS matl_desc,
                NULL AS matl_desc2,
                NULL AS matl_desc3,
                NULL AS frnchse_desc,
                NULL AS brnd_desc,
                NULL AS vrnt_desc,
                NULL AS sku_ean_num,
                NULL AS global_mat_region,
                NULL AS global_prod_franchise,
                NULL AS global_prod_brand,
                NULL AS global_prod_variant,
                NULL AS global_prod_put_up_cd,
                NULL AS global_put_up_desc,
                NULL AS put_up_desc,
                NULL AS sap_prod_mnr_cd,
                NULL AS sap_prod_mnr_desc,
                NULL AS sap_prod_hier_cd,
                NULL AS sap_prod_hier_desc,
                NULL AS global_prod_sub_brand,
                NULL AS global_prod_need_state,
                NULL AS global_prod_category,
                NULL AS global_prod_subcategory,
                NULL AS global_prod_segment,
                NULL AS global_prod_subsegment,
                NULL AS global_prod_size,
                NULL AS global_prod_size_uom,
                NULL AS sku_launch_dt,
                NULL AS qty_shipper_pc,
                NULL AS prft_ctr,
                NULL AS shelf_life,
                NULL AS npi_ind,
                NULL AS npi_strt_period,
                NULL AS hero_ind,
                NULL AS ciw_ctgry,
                NULL AS ciw_buckt1,
                NULL AS ciw_buckt2,
                NULL AS bravo_desc1,
                NULL AS bravo_desc2,
                NULL AS acct_type,
                NULL AS acct_type1,
                NULL AS from_crncy,
                veocurd.to_ccy AS to_crncy,
                veocurd.exch_rate,
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
                imt.trgt_type,
                imt.trgt_val_type,
                0 AS trgt_val,
                0 AS accrual_val,
                (imt.wk1 * veocurd.exch_rate) AS le_trgt_val_wk1,
                (imt.wk2 * veocurd.exch_rate) AS le_trgt_val_wk2,
                (imt.wk3 * veocurd.exch_rate) AS le_trgt_val_wk3,
                (imt.wk4 * veocurd.exch_rate) AS le_trgt_val_wk4,
                (imt.wk5 * veocurd.exch_rate) AS le_trgt_val_wk5,
                0 AS curr_prd_elapsed,
                NULL AS elapsed_flag,
                'Y' AS is_curr,
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
                (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
            FROM (
                    SELECT a.mnth_id,
                        a.cal_date_id
                    FROM edw_vw_os_time_dim a
                    WHERE (
                            a.cal_date_id = replace(
                                current_timestamp(),
                                ('-'::character varying)::text,
                                (''::character varying)::text
                            )
                        )
                ) cur_period_sgt,
                (
                    SELECT a."year" AS jj_year,
                        a.qrtr_no,
                        a.qrtr AS jj_qtr,
                        a.mnth_id AS jj_mnth_id,
                        a.mnth_desc,
                        a.mnth_no AS jj_mnth_no,
                        a.mnth_shrt,
                        a.mnth_long
                    FROM edw_vw_os_time_dim a
                    GROUP BY a."year",
                        a.qrtr_no,
                        a.qrtr,
                        a.mnth_id,
                        a.mnth_desc,
                        a.mnth_no,
                        a.mnth_shrt,
                        a.mnth_long
                ) veotd,
                (
                    SELECT d.cntry_key,
                        d.cntry_nm,
                        d.rate_type,
                        d.from_ccy,
                        d.to_ccy,
                        d.valid_date,
                        d.jj_year,
                        d.start_period,
                        CASE
                            WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                            ELSE d.end_mnth_id
                        END AS end_period,
                        d.exch_rate
                    FROM (
                            SELECT a.cntry_key,
                                a.cntry_nm,
                                a.rate_type,
                                a.from_ccy,
                                a.to_ccy,
                                a.valid_date,
                                a.jj_year,
                                min((a.jj_mnth_id)::text) AS start_period,
                                "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                                a.exch_rate
                            FROM EDW_VW_my_CURR_DIM a
                            WHERE (
                                    (a.cntry_key)::text = ('MY'::character varying)::text
                                )
                            GROUP BY a.cntry_key,
                                a.cntry_nm,
                                a.rate_type,
                                a.from_ccy,
                                a.to_ccy,
                                a.valid_date,
                                a.jj_year,
                                a.exch_rate
                        ) d,
                        (
                            SELECT "max"((a.jj_mnth_id)::text) AS max_period
                            FROM EDW_VW_my_CURR_DIM a
                            WHERE (
                                    (a.cntry_key)::text = ('MY'::character varying)::text
                                )
                        ) b
                ) veocurd,
                (
                    (
                        itg_my_le_trgt imt
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
                                ) = trim((imt.cust_id)::text)
                            )
                        )
                    )
                    LEFT JOIN itg_my_customer_dim imcd ON (
                        (
                            trim((imcd.cust_id)::text) = trim((imt.cust_id)::text)
                        )
                    )
                )
            WHERE (
                    (
                        (veotd.jj_mnth_id = (imt.jj_mnth_id)::text)
                        AND ((imt.jj_mnth_id)::text >= veocurd.start_period)
                    )
                    AND ((imt.jj_mnth_id)::text <= veocurd.end_period)
                )
),
ds_accruals as (
    SELECT 'Accruals' AS data_src,
            veotd.jj_year,
            (veotd.jj_qtr)::character varying AS jj_qtr,
            imt.jj_mnth_id,
            veotd.jj_mnth_no,
            veocd.sap_cntry_cd,
            'Malaysia' AS cntry_nm,
            NULL AS acct_no,
            NULL AS acct_desc,
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
            NULL AS matl_num,
            NULL AS matl_desc,
            NULL AS matl_desc2,
            NULL AS matl_desc3,
            NULL AS frnchse_desc,
            NULL AS brnd_desc,
            NULL AS vrnt_desc,
            NULL AS sku_ean_num,
            NULL AS global_mat_region,
            NULL AS global_prod_franchise,
            NULL AS global_prod_brand,
            NULL AS global_prod_variant,
            NULL AS global_prod_put_up_cd,
            NULL AS global_put_up_desc,
            NULL AS put_up_desc,
            NULL AS sap_prod_mnr_cd,
            NULL AS sap_prod_mnr_desc,
            NULL AS sap_prod_hier_cd,
            NULL AS sap_prod_hier_desc,
            NULL AS global_prod_sub_brand,
            NULL AS global_prod_need_state,
            NULL AS global_prod_category,
            NULL AS global_prod_subcategory,
            NULL AS global_prod_segment,
            NULL AS global_prod_subsegment,
            NULL AS global_prod_size,
            NULL AS global_prod_size_uom,
            NULL AS sku_launch_dt,
            NULL AS qty_shipper_pc,
            NULL AS prft_ctr,
            NULL AS shelf_life,
            NULL AS npi_ind,
            NULL AS npi_strt_period,
            NULL AS hero_ind,
            NULL AS ciw_ctgry,
            NULL AS ciw_buckt1,
            NULL AS ciw_buckt2,
            NULL AS bravo_desc1,
            NULL AS bravo_desc2,
            NULL AS acct_type,
            NULL AS acct_type1,
            NULL AS from_crncy,
            veocurd.to_ccy AS to_crncy,
            veocurd.exch_rate,
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
            (imt.accrual_val * veocurd.exch_rate) AS accrual_val,
            0 AS le_trgt_val_wk1,
            0 AS le_trgt_val_wk2,
            0 AS le_trgt_val_wk3,
            0 AS le_trgt_val_wk4,
            0 AS le_trgt_val_wk5,
            0 AS curr_prd_elapsed,
            NULL AS elapsed_flag,
            'Y' AS is_curr,
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
            (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
        FROM (
                SELECT a.mnth_id,
                    a.cal_date_id
                FROM edw_vw_os_time_dim a
                WHERE (
                        a.cal_date_id = replace(
                            current_timestamp(),
                            ('-'::character varying)::text,
                            (''::character varying)::text
                        )
                    )
            ) cur_period_sgt,
            (
                SELECT a."year" AS jj_year,
                    a.qrtr_no,
                    a.qrtr AS jj_qtr,
                    a.mnth_id AS jj_mnth_id,
                    a.mnth_desc,
                    a.mnth_no AS jj_mnth_no,
                    a.mnth_shrt,
                    a.mnth_long
                FROM edw_vw_os_time_dim a
                GROUP BY a."year",
                    a.qrtr_no,
                    a.qrtr,
                    a.mnth_id,
                    a.mnth_desc,
                    a.mnth_no,
                    a.mnth_shrt,
                    a.mnth_long
            ) veotd,
            (
                SELECT d.cntry_key,
                    d.cntry_nm,
                    d.rate_type,
                    d.from_ccy,
                    d.to_ccy,
                    d.valid_date,
                    d.jj_year,
                    d.start_period,
                    CASE
                        WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                        ELSE d.end_mnth_id
                    END AS end_period,
                    d.exch_rate
                FROM (
                        SELECT a.cntry_key,
                            a.cntry_nm,
                            a.rate_type,
                            a.from_ccy,
                            a.to_ccy,
                            a.valid_date,
                            a.jj_year,
                            min((a.jj_mnth_id)::text) AS start_period,
                            "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                            a.exch_rate
                        FROM EDW_VW_my_CURR_DIM a
                        WHERE (
                                (a.cntry_key)::text = ('MY'::character varying)::text
                            )
                        GROUP BY a.cntry_key,
                            a.cntry_nm,
                            a.rate_type,
                            a.from_ccy,
                            a.to_ccy,
                            a.valid_date,
                            a.jj_year,
                            a.exch_rate
                    ) d,
                    (
                        SELECT "max"((a.jj_mnth_id)::text) AS max_period
                        FROM EDW_VW_my_CURR_DIM a
                        WHERE (
                                (a.cntry_key)::text = ('MY'::character varying)::text
                            )
                    ) b
            ) veocurd,
            (
                (
                    itg_my_accruals imt
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
                            ) = trim((imt.cust_id)::text)
                        )
                    )
                )
                LEFT JOIN itg_my_customer_dim imcd ON (
                    (
                        trim((imcd.cust_id)::text) = trim((imt.cust_id)::text)
                    )
                )
            )
        WHERE (
                (
                    (veotd.jj_mnth_id = (imt.jj_mnth_id)::text)
                    AND ((imt.jj_mnth_id)::text >= veocurd.start_period)
                )
                AND ((imt.jj_mnth_id)::text <= veocurd.end_period)
            )
),
ds_orders as (
    SELECT 'Orders' AS data_src,
        myof.jj_year,
        (myof.jj_qtr)::character varying AS jj_qtr,
        (myof.jj_mnth_id)::character varying AS jj_mnth_id,
        myof.jj_mnth_no,
        veocd.sap_cntry_cd,
        'Malaysia' AS cntry_nm,
        NULL AS acct_no,
        NULL AS acct_desc,
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
        NULL AS ciw_ctgry,
        NULL AS ciw_buckt1,
        NULL AS ciw_buckt2,
        NULL AS bravo_desc1,
        NULL AS bravo_desc2,
        NULL AS acct_type,
        NULL AS acct_type1,
        veocurd.from_ccy AS from_crncy,
        veocurd.to_ccy AS to_crncy,
        veocurd.exch_rate,
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
        (myof.po_num)::character varying AS po_num,
        (myof.sls_doc_num)::character varying AS sls_doc_num,
        (myof.sls_doc_item)::character varying AS sls_doc_item,
        myof.sls_doc_type,
        (myof.bill_num)::character varying AS bill_num,
        (myof.bill_item)::character varying AS bill_item,
        myof.doc_creation_dt,
        myof.order_status,
        myof.item_status,
        myof.rejectn_st,
        myof.rejectn_cd,
        (myof.rejectn_desc)::character varying AS rejectn_desc,
        myof.ord_qty,
        (myof.ord_net_price * veocurd.exch_rate) AS ord_net_price,
        (myof.ord_grs_trd_sls * veocurd.exch_rate) AS ord_grs_trd_sls,
        (myof.ord_subtotal_2 * veocurd.exch_rate) AS ord_subtotal_2,
        (myof.ord_subtotal_3 * veocurd.exch_rate) AS ord_subtotal_3,
        (myof.ord_subtotal_4 * veocurd.exch_rate) AS ord_subtotal_4,
        (myof.ord_net_amt * veocurd.exch_rate) AS ord_net_amt,
        (myof.ord_est_nts * veocurd.exch_rate) AS ord_est_nts,
        (myof.missed_val * veocurd.exch_rate) AS missed_val,
        myof.bill_qty_pc,
        (myof.bill_grs_trd_sls * veocurd.exch_rate) AS bill_grs_trd_sls,
        (myof.bill_subtotal_2 * veocurd.exch_rate) AS bill_subtotal_2,
        (myof.bill_subtotal_3 * veocurd.exch_rate) AS bill_subtotal_3,
        (myof.bill_subtotal_4 * veocurd.exch_rate) AS bill_subtotal_4,
        (myof.bill_net_amt * veocurd.exch_rate) AS bill_net_amt,
        (myof.bill_est_nts * veocurd.exch_rate) AS bill_est_nts,
        (myof.bill_net_val * veocurd.exch_rate) AS bill_net_val,
        (myof.bill_gross_val * veocurd.exch_rate) AS bill_gross_val,
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
        'Y' AS is_curr,
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
        (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
    FROM (
            SELECT d.cntry_key,
                d.cntry_nm,
                d.rate_type,
                d.from_ccy,
                d.to_ccy,
                d.valid_date,
                d.jj_year,
                d.start_period,
                CASE
                    WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                    ELSE d.end_mnth_id
                END AS end_period,
                d.exch_rate
            FROM (
                    SELECT a.cntry_key,
                        a.cntry_nm,
                        a.rate_type,
                        a.from_ccy,
                        a.to_ccy,
                        a.valid_date,
                        a.jj_year,
                        min((a.jj_mnth_id)::text) AS start_period,
                        "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                        a.exch_rate
                    FROM EDW_VW_my_CURR_DIM a
                    WHERE (
                            (a.cntry_key)::text = ('MY'::character varying)::text
                        )
                    GROUP BY a.cntry_key,
                        a.cntry_nm,
                        a.rate_type,
                        a.from_ccy,
                        a.to_ccy,
                        a.valid_date,
                        a.jj_year,
                        a.exch_rate
                ) d,
                (
                    SELECT "max"((a.jj_mnth_id)::text) AS max_period
                    FROM EDW_VW_my_CURR_DIM a
                    WHERE (
                            (a.cntry_key)::text = ('MY'::character varying)::text
                        )
                ) b
        ) veocurd,
        (
            SELECT a.mnth_id,
                a.cal_date_id
            FROM edw_vw_os_time_dim a
            WHERE (
                    a.cal_date_id = replace(
                        current_timestamp(),
                        ('-'::character varying)::text,
                        (''::character varying)::text
                    )
                )
        ) cur_period_sgt,
        (
            (
                (
                    (
                        (
                            SELECT veotd.jj_year,
                                veotd.jj_qtr,
                                veotd.jj_mnth_id,
                                veotd.jj_mnth_no,
                                evmof.po_num,
                                evmof.sls_doc_num,
                                evmof.sls_doc_item,
                                evmof.sls_doc_type,
                                evmof.bill_num,
                                evmof.bill_item,
                                evmof.doc_creation_dt,
                                evmof.sold_to,
                                evmof.matl_num,
                                evmof.rejectn_st,
                                evmof.rejectn_cd,
                                evmof.rejectn_desc,
                                sum(evmof.ord_qty) AS ord_qty,
                                sum(evmof.missed_val) AS missed_val,
                                evmof.rdd_ind,
                                evmof.order_status,
                                evmof.item_status,
                                sum(evmof.ord_net_price) AS ord_net_price,
                                sum(evmof.ord_grs_trd_sls) AS ord_grs_trd_sls,
                                sum(evmof.ord_subtotal_2) AS ord_subtotal_2,
                                sum(evmof.ord_subtotal_3) AS ord_subtotal_3,
                                sum(evmof.ord_subtotal_4) AS ord_subtotal_4,
                                sum(evmof.ord_net_amt) AS ord_net_amt,
                                sum(evmof.ord_est_nts) AS ord_est_nts,
                                sum(evmof.bill_qty_pc) AS bill_qty_pc,
                                sum(evmof.bill_grs_trd_sls) AS bill_grs_trd_sls,
                                sum(evmof.bill_subtotal_2) AS bill_subtotal_2,
                                sum(evmof.bill_subtotal_3) AS bill_subtotal_3,
                                sum(evmof.bill_subtotal_4) AS bill_subtotal_4,
                                sum(evmof.bill_net_amt) AS bill_net_amt,
                                sum(evmof.bill_est_nts) AS bill_est_nts,
                                sum(evmof.bill_net_val) AS bill_net_val,
                                sum(evmof.bill_gross_val) AS bill_gross_val
                            FROM (
                                    SELECT a.doc_dt,
                                        a.po_num,
                                        a.sls_doc_num,
                                        a.sls_doc_item,
                                        a.sls_doc_type,
                                        a.bill_dt,
                                        a.bill_num,
                                        a.bill_item,
                                        a.doc_creation_dt,
                                        a.sold_to,
                                        a.matl_num,
                                        a.req_delvry_dt,
                                        b.rdd_ind,
                                        (a.ord_subtotal_2 - a.bill_subtotal_2) AS missed_val,
                                        CASE
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NULL)
                                                                    AND (a.bill_num IS NULL)
                                                                )
                                                                AND (a.bill_qty_pc IS NULL)
                                                            )
                                                            AND (a.bill_grs_trd_sls IS NULL)
                                                        )
                                                        AND (a.bill_net_val IS NULL)
                                                    )
                                                    AND (a.bill_subtotal_2 IS NULL)
                                                )
                                                AND ((a.ord_subtotal_2 - a.bill_subtotal_2) IS NULL)
                                            ) THEN CASE
                                                WHEN (
                                                    (
                                                        (
                                                            upper((b.rdd_ind)::text) = ('YES'::character varying)::text
                                                        )
                                                        AND (
                                                            "substring"(
                                                                add_months(
                                                                    to_date(current_timestamp()),
                                                                    (1)::bigint
                                                                ),
                                                                6,
                                                                2
                                                            ) = "substring"(
                                                                ((a.req_delvry_dt)::character varying)::text,
                                                                6,
                                                                2
                                                            )
                                                        )
                                                    )
                                                    AND (
                                                        current_timestamp()::date <= a.req_delvry_dt
                                                    )
                                                ) THEN 'Forward Month Order'::character varying
                                                ELSE 'Open Order'::character varying
                                            END
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NULL)
                                                                    AND (a.bill_num IS NOT NULL)
                                                                )
                                                                AND (a.bill_qty_pc = a.ord_qty)
                                                            )
                                                            AND (a.bill_grs_trd_sls = a.ord_grs_trd_sls)
                                                        )
                                                        AND (a.bill_net_val = a.ord_net_amt)
                                                    )
                                                    AND (a.bill_subtotal_2 = a.ord_subtotal_2)
                                                )
                                                AND (
                                                    (a.ord_subtotal_2 - a.bill_subtotal_2) = ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN 'Order Completed'::character varying
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NOT NULL)
                                                                    AND (a.bill_num IS NOT NULL)
                                                                )
                                                                AND (a.bill_qty_pc < a.ord_qty)
                                                            )
                                                            AND (a.bill_grs_trd_sls < a.ord_grs_trd_sls)
                                                        )
                                                        AND (a.bill_net_val < a.ord_net_amt)
                                                    )
                                                    AND (a.bill_subtotal_2 < a.ord_subtotal_2)
                                                )
                                                AND (
                                                    (a.ord_subtotal_2 - a.bill_subtotal_2) > ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN 'Order Completed'::character varying
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NOT NULL)
                                                                    AND (a.bill_num IS NOT NULL)
                                                                )
                                                                AND (a.bill_qty_pc IS NULL)
                                                            )
                                                            AND (a.bill_grs_trd_sls IS NULL)
                                                        )
                                                        AND (a.bill_net_val IS NULL)
                                                    )
                                                    AND (a.bill_subtotal_2 IS NULL)
                                                )
                                                AND (
                                                    (a.ord_subtotal_2 - a.bill_subtotal_2) > ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN 'Order Completed'::character varying
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NOT NULL)
                                                                    AND (a.bill_num IS NULL)
                                                                )
                                                                AND (a.bill_qty_pc IS NULL)
                                                            )
                                                            AND (a.bill_grs_trd_sls IS NULL)
                                                        )
                                                        AND (a.bill_net_val IS NULL)
                                                    )
                                                    AND (a.bill_subtotal_2 IS NULL)
                                                )
                                                AND (
                                                    (a.ord_subtotal_2 - a.bill_subtotal_2) > ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN 'Order Dropped'::character varying
                                            ELSE 'NA'::character varying
                                        END AS order_status,
                                        CASE
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NULL)
                                                                    AND (a.bill_num IS NULL)
                                                                )
                                                                AND (a.bill_qty_pc IS NULL)
                                                            )
                                                            AND (a.bill_grs_trd_sls IS NULL)
                                                        )
                                                        AND (a.bill_net_val IS NULL)
                                                    )
                                                    AND (a.bill_subtotal_2 IS NULL)
                                                )
                                                AND ((a.ord_subtotal_2 - a.bill_subtotal_2) IS NULL)
                                            ) THEN 'NULL'::character varying
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NULL)
                                                                    AND (a.bill_num IS NOT NULL)
                                                                )
                                                                AND (a.bill_qty_pc = a.ord_qty)
                                                            )
                                                            AND (a.bill_grs_trd_sls = a.ord_grs_trd_sls)
                                                        )
                                                        AND (a.bill_net_val = a.ord_net_amt)
                                                    )
                                                    AND (a.bill_subtotal_2 = a.ord_subtotal_2)
                                                )
                                                AND (
                                                    (a.ord_subtotal_2 - a.bill_subtotal_2) = ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN 'Item Fulfilled'::character varying
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NOT NULL)
                                                                    AND (a.bill_num IS NOT NULL)
                                                                )
                                                                AND (a.bill_qty_pc < a.ord_qty)
                                                            )
                                                            AND (a.bill_grs_trd_sls < a.ord_grs_trd_sls)
                                                        )
                                                        AND (a.bill_net_val < a.ord_net_amt)
                                                    )
                                                    AND (a.bill_subtotal_2 < a.ord_subtotal_2)
                                                )
                                                AND (
                                                    (a.ord_subtotal_2 - a.bill_subtotal_2) > ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN 'Item Partially Fulfilled'::character varying
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NOT NULL)
                                                                    AND (a.bill_num IS NOT NULL)
                                                                )
                                                                AND (a.bill_qty_pc IS NULL)
                                                            )
                                                            AND (a.bill_grs_trd_sls IS NULL)
                                                        )
                                                        AND (a.bill_net_val IS NULL)
                                                    )
                                                    AND (a.bill_subtotal_2 IS NULL)
                                                )
                                                AND (
                                                    (a.ord_subtotal_2 - a.bill_subtotal_2) > ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN 'Item Dropped'::character varying
                                            WHEN (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (a.rejectn_cd IS NOT NULL)
                                                                    AND (a.bill_num IS NULL)
                                                                )
                                                                AND (a.bill_qty_pc IS NULL)
                                                            )
                                                            AND (a.bill_grs_trd_sls IS NULL)
                                                        )
                                                        AND (a.bill_net_val IS NULL)
                                                    )
                                                    AND (a.bill_subtotal_2 IS NULL)
                                                )
                                                AND (
                                                    (a.ord_subtotal_2 - a.bill_subtotal_2) > ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN 'Item Dropped'::character varying
                                            ELSE 'NA'::character varying
                                        END AS item_status,
                                        a.rejectn_st,
                                        a.rejectn_desc,
                                        a.rejectn_cd,
                                        a.ord_qty,
                                        a.ord_net_price,
                                        a.ord_grs_trd_sls,
                                        a.ord_subtotal_2,
                                        a.ord_subtotal_3,
                                        a.ord_subtotal_4,
                                        a.ord_net_amt,
                                        a.ord_est_nts,
                                        a.bill_qty_pc,
                                        a.bill_grs_trd_sls,
                                        a.bill_subtotal_2,
                                        a.bill_subtotal_3,
                                        a.bill_subtotal_4,
                                        a.bill_net_amt,
                                        a.bill_est_nts,
                                        a.bill_net_val,
                                        a.bill_gross_val
                                    FROM (
                                            SELECT replace(
                                                    current_timestamp(),
                                                    ('-'::character varying)::text,
                                                    (''::character varying)::text
                                                ) AS curr_dt,
                                                a.mnth_id,
                                                a.mnth_wk_no,
                                                a.last_wk_no
                                            FROM (
                                                    SELECT derived_table2.mnth_id,
                                                        derived_table2.cal_date_id,
                                                        derived_table2.mnth_wk_no,
                                                        derived_table2.last_wk_no
                                                    FROM (
                                                            SELECT edw_vw_os_time_dim.mnth_id,
                                                                edw_vw_os_time_dim.cal_date_id,
                                                                edw_vw_os_time_dim.mnth_wk_no,
                                                                "max"(edw_vw_os_time_dim.mnth_wk_no) OVER(
                                                                    PARTITION BY edw_vw_os_time_dim.mnth_id
                                                                    order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                ) AS last_wk_no
                                                            FROM edw_vw_os_time_dim
                                                        ) derived_table2
                                                ) a
                                            WHERE (
                                                    a.cal_date_id = replace(
                                                        current_timestamp(),
                                                        ('-'::character varying)::text,
                                                        (''::character varying)::text
                                                    )
                                                )
                                        ) veotd,
                                        (
                                            EDW_VW_MY_ORDERS_FACT a
                                            LEFT JOIN itg_my_customer_dim b ON (((b.cust_id)::text = a.sold_to))
                                        )
                                ) evmof,
                                (
                                    SELECT a."year" AS jj_year,
                                        a.qrtr_no,
                                        a.qrtr AS jj_qtr,
                                        a.mnth_id AS jj_mnth_id,
                                        a.mnth_desc,
                                        a.mnth_no AS jj_mnth_no,
                                        a.mnth_shrt,
                                        a.mnth_long,
                                        a.cal_date
                                    FROM edw_vw_os_time_dim a
                                    GROUP BY a."year",
                                        a.qrtr_no,
                                        a.qrtr,
                                        a.mnth_id,
                                        a.mnth_desc,
                                        a.mnth_no,
                                        a.mnth_shrt,
                                        a.mnth_long,
                                        a.cal_date
                                ) veotd
                            WHERE (veotd.cal_date = evmof.doc_dt)
                            GROUP BY veotd.jj_year,
                                veotd.jj_qtr,
                                veotd.jj_mnth_id,
                                veotd.jj_mnth_no,
                                evmof.po_num,
                                evmof.sls_doc_num,
                                evmof.sls_doc_item,
                                evmof.sls_doc_type,
                                evmof.bill_num,
                                evmof.bill_item,
                                evmof.doc_creation_dt,
                                evmof.sold_to,
                                evmof.matl_num,
                                evmof.rejectn_st,
                                evmof.rejectn_cd,
                                evmof.rejectn_desc,
                                evmof.rdd_ind,
                                evmof.order_status,
                                evmof.item_status
                        ) as myof
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
                                    ('0'::character varying)::text
                                ) = ltrim(myof.sold_to, ('0'::character varying)::text)
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
                            ltrim(
                                (veomd.sap_matl_num)::text,
                                ('0'::character varying)::text
                            ) = ltrim(myof.matl_num, ('0'::character varying)::text)
                        )
                    )
                )
                LEFT JOIN itg_my_customer_dim imcd ON (
                    (
                        trim((imcd.cust_id)::text) = ltrim(myof.sold_to, ('0'::character varying)::text)
                    )
                )
            )
            LEFT JOIN itg_my_material_dim immd ON (
                (
                    trim((immd.item_cd)::text) = ltrim(myof.matl_num, ('0'::character varying)::text)
                )
            )
        )
    WHERE (
            (myof.jj_mnth_id >= veocurd.start_period)
            AND (myof.jj_mnth_id <= veocurd.end_period)
        )
),
ds_afgr as (
    SELECT 'AFGR' AS data_src,
    ima.jj_year,
    (ima.jj_qtr)::character varying AS jj_qtr,
    (ima.jj_mnth_id)::character varying AS jj_mnth_id,
    ima.jj_mnth_no,
    veocd.sap_cntry_cd,
    'Malaysia' AS cntry_nm,
    NULL AS acct_no,
    NULL AS acct_desc,
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
    NULL AS matl_num,
    NULL AS matl_desc,
    NULL AS matl_desc2,
    NULL AS matl_desc3,
    NULL AS frnchse_desc,
    NULL AS brnd_desc,
    NULL AS vrnt_desc,
    NULL AS sku_ean_num,
    NULL AS global_mat_region,
    NULL AS global_prod_franchise,
    NULL AS global_prod_brand,
    NULL AS global_prod_variant,
    NULL AS global_prod_put_up_cd,
    NULL AS global_put_up_desc,
    NULL AS put_up_desc,
    NULL AS sap_prod_mnr_cd,
    NULL AS sap_prod_mnr_desc,
    NULL AS sap_prod_hier_cd,
    NULL AS sap_prod_hier_desc,
    NULL AS global_prod_sub_brand,
    NULL AS global_prod_need_state,
    NULL AS global_prod_category,
    NULL AS global_prod_subcategory,
    NULL AS global_prod_segment,
    NULL AS global_prod_subsegment,
    NULL AS global_prod_size,
    NULL AS global_prod_size_uom,
    NULL AS sku_launch_dt,
    NULL AS qty_shipper_pc,
    NULL AS prft_ctr,
    NULL AS shelf_life,
    NULL AS npi_ind,
    NULL AS npi_strt_period,
    NULL AS hero_ind,
    NULL AS ciw_ctgry,
    NULL AS ciw_buckt1,
    NULL AS ciw_buckt2,
    NULL AS bravo_desc1,
    NULL AS bravo_desc2,
    NULL AS acct_type,
    NULL AS acct_type1,
    veocurd.from_ccy AS from_crncy,
    veocurd.to_ccy AS to_crncy,
    veocurd.exch_rate,
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
    ima.afgr_num AS po_num,
    (vmof.sls_doc_num)::character varying AS sls_doc_num,
    NULL AS sls_doc_item,
    NULL AS sls_doc_type,
    (vmof.bill_num)::character varying AS bill_num,
    NULL AS bill_item,
    vmof.doc_creation_dt,
    NULL AS order_status,
    NULL AS item_status,
    vmof.rejectn_st,
    vmof.rejectn_cd,
    (vmof.rejectn_desc)::character varying AS rejectn_desc,
    vmof.ord_qty,
    (vmof.ord_net_price * veocurd.exch_rate) AS ord_net_price,
    (vmof.ord_grs_trd_sls * veocurd.exch_rate) AS ord_grs_trd_sls,
    (vmof.ord_subtotal_2 * veocurd.exch_rate) AS ord_subtotal_2,
    (vmof.ord_subtotal_3 * veocurd.exch_rate) AS ord_subtotal_3,
    (vmof.ord_subtotal_4 * veocurd.exch_rate) AS ord_subtotal_4,
    (vmof.ord_net_amt * veocurd.exch_rate) AS ord_net_amt,
    (vmof.ord_est_nts * veocurd.exch_rate) AS ord_est_nts,
    0 AS missed_val,
    vmof.bill_qty_pc,
    (vmof.bill_grs_trd_sls * veocurd.exch_rate) AS bill_grs_trd_sls,
    (vmof.bill_subtotal_2 * veocurd.exch_rate) AS bill_subtotal_2,
    (vmof.bill_subtotal_3 * veocurd.exch_rate) AS bill_subtotal_3,
    (vmof.bill_subtotal_4 * veocurd.exch_rate) AS bill_subtotal_4,
    (vmof.bill_net_amt * veocurd.exch_rate) AS bill_net_amt,
    (vmof.bill_est_nts * veocurd.exch_rate) AS bill_est_nts,
    (vmof.bill_net_val * veocurd.exch_rate) AS bill_net_val,
    (vmof.bill_gross_val * veocurd.exch_rate) AS bill_gross_val,
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
    'Y' AS is_curr,
    ima.afgr_num,
    ima.cust_dn_num,
    ima.rtn_ord_num,
    ima.bill_num AS afgr_bill_num,
    (ima.dn_amt_exc_gst_val * veocurd.exch_rate) AS dn_amt_exc_gst_val,
    (ima.afgr_amt * veocurd.exch_rate) AS afgr_amt,
    (ima.rtn_ord_amt * veocurd.exch_rate) AS rtn_ord_amt,
    (ima.cn_amt * veocurd.exch_rate) AS cn_amt,
    ima.afgr_status,
    (ima.afgr_val * veocurd.exch_rate) AS afgr_val,
    (ima.afgr_cn_diff * veocurd.exch_rate) AS afgr_cn_diff,
    (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
    FROM (
        SELECT d.cntry_key,
            d.cntry_nm,
            d.rate_type,
            d.from_ccy,
            d.to_ccy,
            d.valid_date,
            d.jj_year,
            d.start_period,
            CASE
                WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                ELSE d.end_mnth_id
            END AS end_period,
            d.exch_rate
        FROM (
                SELECT a.cntry_key,
                    a.cntry_nm,
                    a.rate_type,
                    a.from_ccy,
                    a.to_ccy,
                    a.valid_date,
                    a.jj_year,
                    min((a.jj_mnth_id)::text) AS start_period,
                    "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                    a.exch_rate
                FROM EDW_VW_my_CURR_DIM a
                WHERE (
                        (a.cntry_key)::text = ('MY'::character varying)::text
                    )
                GROUP BY a.cntry_key,
                    a.cntry_nm,
                    a.rate_type,
                    a.from_ccy,
                    a.to_ccy,
                    a.valid_date,
                    a.jj_year,
                    a.exch_rate
            ) d,
            (
                SELECT "max"((a.jj_mnth_id)::text) AS max_period
                FROM EDW_VW_my_CURR_DIM a
                WHERE (
                        (a.cntry_key)::text = ('MY'::character varying)::text
                    )
            ) b
    ) veocurd,
    (
        SELECT a.mnth_id,
            a.cal_date_id
        FROM edw_vw_os_time_dim a
        WHERE (
                a.cal_date_id = replace(
                    current_timestamp(),
                    ('-'::character varying)::text,
                    (''::character varying)::text
                )
            )
    ) cur_period_sgt,
    (
        (
            (
                (
                    SELECT veotd.jj_year,
                        veotd.jj_qtr,
                        veotd.jj_mnth_id,
                        veotd.jj_mnth_no,
                        a.cust_id,
                        a.afgr_num,
                        a.cust_dn_num,
                        a.rtn_ord_num,
                        a.bill_num,
                        a.afgr_status,
                        sum(a.dn_amt_exc_gst_val) AS dn_amt_exc_gst_val,
                        sum(a.afgr_amt) AS afgr_amt,
                        sum(a.rtn_ord_amt) AS rtn_ord_amt,
                        sum(a.cn_amt) AS cn_amt,
                        sum(a.afgr_val) AS afgr_val,
                        sum(a.afgr_cn_diff) AS afgr_cn_diff
                    FROM itg_my_afgr a,
                        (
                            SELECT edw_vw_os_time_dim."year" AS jj_year,
                                edw_vw_os_time_dim.qrtr_no,
                                edw_vw_os_time_dim.qrtr AS jj_qtr,
                                edw_vw_os_time_dim.mnth_id AS jj_mnth_id,
                                edw_vw_os_time_dim.mnth_no AS jj_mnth_no,
                                edw_vw_os_time_dim.cal_date
                            FROM edw_vw_os_time_dim
                            GROUP BY edw_vw_os_time_dim."year",
                                edw_vw_os_time_dim.qrtr_no,
                                edw_vw_os_time_dim.qrtr,
                                edw_vw_os_time_dim.mnth_id,
                                edw_vw_os_time_dim.mnth_no,
                                edw_vw_os_time_dim.cal_date
                        ) veotd
                    WHERE (veotd.cal_date = a.doc_dt)
                    GROUP BY veotd.jj_year,
                        veotd.jj_qtr,
                        veotd.jj_mnth_id,
                        veotd.jj_mnth_no,
                        a.cust_id,
                        a.afgr_num,
                        a.cust_dn_num,
                        a.rtn_ord_num,
                        a.bill_num,
                        a.afgr_status
                ) ima
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
                        ) = trim((ima.cust_id)::text)
                    )
                )
            )
            LEFT JOIN itg_my_customer_dim imcd ON (
                (
                    trim((imcd.cust_id)::text) = trim((ima.cust_id)::text)
                )
            )
        )
        LEFT JOIN EDW_VW_MY_ORDERS_FACT vmof ON (
            (
                (vmof.sls_doc_num = (ima.rtn_ord_num)::text)
                AND (vmof.bill_num = (ima.bill_num)::text)
            )
        )
    )
    WHERE (
        (ima.jj_mnth_id >= veocurd.start_period)
        AND (ima.jj_mnth_id <= veocurd.end_period)
    )
),
transformed as (
    select * from ds_primary
    union all
    select * from ds_primary_null
    union all
    select * from ds_targets
    union all
    select * from ds_targets_null
    union all
    select * from ds_le_targets
    union all
    select * from ds_accruals
    union all
    select * from ds_orders
    union all
    select * from ds_afgr
),

--Final CTE
final as (
    select 
        data_src::varchar(12) as data_src,
        jj_year::numeric(18,0) as jj_year,
        jj_qtr::varchar(14) as jj_qtr,
        jj_mnth_id::varchar(23) as jj_mnth_id,
        jj_mnth_no::numeric(18,0) as jj_mnth_no,
        sap_cntry_cd::varchar(3) as sap_cntry_cd,
        cntry_nm::varchar(8) as cntry_nm,
        acct_no::varchar(10) as acct_no,
        acct_desc::varchar(100) as acct_desc,
        sap_state_cd::varchar(150) as sap_state_cd,
        sold_to::varchar(10) as sold_to,
        sold_to_nm::varchar(100) as sold_to_nm,
        sap_sls_org::varchar(4) as sap_sls_org,
        sap_cmp_id::varchar(6) as sap_cmp_id,
        sap_addr::varchar(150) as sap_addr,
        sap_region::varchar(150) as sap_region,
        sap_city::varchar(150) as sap_city,
        sap_post_cd::varchar(150) as sap_post_cd,
        sap_chnl_cd::varchar(2) as sap_chnl_cd,
        sap_chnl_desc::varchar(20) as sap_chnl_desc,
        sap_sls_office_cd::varchar(4) as sap_sls_office_cd,
        sap_sls_office_desc::varchar(40) as sap_sls_office_desc,
        sap_sls_grp_cd::varchar(3) as sap_sls_grp_cd,
        sap_sls_grp_desc::varchar(40) as sap_sls_grp_desc,
        sap_curr_cd::varchar(5) as sap_curr_cd,
        gch_region::varchar(50) as gch_region,
        gch_cluster::varchar(50) as gch_cluster,
        gch_subcluster::varchar(50) as gch_subcluster,
        gch_market::varchar(50) as gch_market,
        gch_retail_banner::varchar(50) as gch_retail_banner,
        loc_cust_nm::varchar(100) as loc_cust_nm,
        dstrbtr_grp_cd::varchar(20) as dstrbtr_grp_cd,
        dstrbtr_grp_nm::varchar(100) as dstrbtr_grp_nm,
        ullage::numeric(20,4) as ullage,
        chnl::varchar(20) as chnl,
        territory::varchar(20) as territory,
        retail_env::varchar(20) as retail_env,
        rdd_ind::varchar(10) as rdd_ind,
        matl_num::varchar(40) as matl_num,
        matl_desc::varchar(100) as matl_desc,
        matl_desc2::varchar(200) as matl_desc2,
        matl_desc3::varchar(200) as matl_desc3,
        frnchse_desc::varchar(100) as frnchse_desc,
        brnd_desc::varchar(100) as brnd_desc,
        vrnt_desc::varchar(100) as vrnt_desc,
        sku_ean_num::varchar(100) as sku_ean_num,
        global_mat_region::varchar(50) as global_mat_region,
        global_prod_franchise::varchar(30) as global_prod_franchise,
        global_prod_brand::varchar(30) as global_prod_brand,
        global_prod_variant::varchar(100) as global_prod_variant,
        global_prod_put_up_cd::varchar(10) as global_prod_put_up_cd,
        global_put_up_desc::varchar(100) as global_put_up_desc,
        put_up_desc::varchar(40) as put_up_desc,
        sap_prod_mnr_cd::varchar(18) as sap_prod_mnr_cd,
        sap_prod_mnr_desc::varchar(100) as sap_prod_mnr_desc,
        sap_prod_hier_cd::varchar(18) as sap_prod_hier_cd,
        sap_prod_hier_desc::varchar(100) as sap_prod_hier_desc,
        global_prod_sub_brand::varchar(100) as global_prod_sub_brand,
        global_prod_need_state::varchar(50) as global_prod_need_state,
        global_prod_category::varchar(50) as global_prod_category,
        global_prod_subcategory::varchar(50) as global_prod_subcategory,
        global_prod_segment::varchar(50) as global_prod_segment,
        global_prod_subsegment::varchar(100) as global_prod_subsegment,
        global_prod_size::varchar(20) as global_prod_size,
        global_prod_size_uom::varchar(20) as global_prod_size_uom,
        sku_launch_dt::timestamp without time zone as sku_launch_dt,
        qty_shipper_pc::varchar(100) as qty_shipper_pc,
        prft_ctr::varchar(100) as prft_ctr,
        shelf_life::varchar(100) as shelf_life,
        npi_ind::varchar(10) as npi_ind,
        npi_strt_period::varchar(10) as npi_strt_period,
        hero_ind::varchar(10) as hero_ind,
        ciw_ctgry::varchar(100) as ciw_ctgry,
        ciw_buckt1::varchar(100) as ciw_buckt1,
        ciw_buckt2::varchar(100) as ciw_buckt2,
        bravo_desc1::varchar(100) as bravo_desc1,
        bravo_desc2::varchar(100) as bravo_desc2,
        acct_type::varchar(20) as acct_type,
        acct_type1::varchar(20) as acct_type1,
        from_crncy::varchar(5) as from_crncy,
        to_crncy::varchar(5) as to_crncy,
        exch_rate::numeric(15,5) as exch_rate,
        base_val::numeric(20,4) as base_val,
        sls_qty::numeric(20,4) as sls_qty,
        ret_qty::numeric(20,4) as ret_qty,
        sls_less_rtn_qty::numeric(20,4) as sls_less_rtn_qty,
        gts_val::numeric(20,4) as gts_val,
        ret_val::numeric(20,4) as ret_val,
        gts_less_rtn_val::numeric(20,4) as gts_less_rtn_val,
        trdng_term_val::numeric(20,4) as trdng_term_val,
        tp_val::numeric(20,4) as tp_val,
        trde_prmtn_val::numeric(20,4) as trde_prmtn_val,
        nts_val::numeric(20,4) as nts_val,
        nts_qty::numeric(20,4) as nts_qty,
        po_num::varchar(30) as po_num,
        sls_doc_num::varchar(50) as sls_doc_num,
        sls_doc_item::varchar(50) as sls_doc_item,
        sls_doc_type::varchar(30) as sls_doc_type,
        bill_num::varchar(50) as bill_num,
        bill_item::varchar(50) as bill_item,
        doc_creation_dt::date as doc_creation_dt,
        order_status::varchar(19) as order_status,
        item_status::varchar(24) as item_status,
        rejectn_st::varchar(20) as rejectn_st,
        rejectn_cd::varchar(30) as rejectn_cd,
        rejectn_desc::varchar as rejectn_desc,
        ord_qty::numeric(38,4) as ord_qty,
        ord_net_price::numeric(38,13) as ord_net_price,
        ord_grs_trd_sls::numeric(38,13) as ord_grs_trd_sls,
        ord_subtotal_2::numeric(38,13) as ord_subtotal_2,
        ord_subtotal_3::numeric(38,13) as ord_subtotal_3,
        ord_subtotal_4::numeric(38,13) as ord_subtotal_4,
        ord_net_amt::numeric(38,13) as ord_net_amt,
        ord_est_nts::numeric(38,13) as ord_est_nts,
        missed_val::numeric(38,13) as missed_val,
        bill_qty_pc::numeric(38,4) as bill_qty_pc,
        bill_grs_trd_sls::numeric(38,13) as bill_grs_trd_sls,
        bill_subtotal_2::numeric(38,13) as bill_subtotal_2,
        bill_subtotal_3::numeric(38,13) as bill_subtotal_3,
        bill_subtotal_4::numeric(38,13) as bill_subtotal_4,
        bill_net_amt::numeric(38,13) as bill_net_amt,
        bill_est_nts::numeric(38,13) as bill_est_nts,
        bill_net_val::numeric(38,13) as bill_net_val,
        bill_gross_val::numeric(38,13) as bill_gross_val,
        trgt_type::varchar(20) as trgt_type,
        trgt_val_type::varchar(20) as trgt_val_type,
        trgt_val::numeric(36,11) as trgt_val,
        accrual_val::numeric(36,11) as accrual_val,
        le_trgt_val_wk1::numeric(36,11) as le_trgt_val_wk1,
        le_trgt_val_wk2::numeric(36,11) as le_trgt_val_wk2,
        le_trgt_val_wk3::numeric(36,11) as le_trgt_val_wk3,
        le_trgt_val_wk4::numeric(36,11) as le_trgt_val_wk4,
        le_trgt_val_wk5::numeric(36,11) as le_trgt_val_wk5,
        curr_prd_elapsed::numeric(20,6) as curr_prd_elapsed,
        elapsed_flag::varchar(1) as elapsed_flag,
        is_curr::varchar(1) as is_curr,
        afgr_num::varchar(30) as afgr_num,
        cust_dn_num::varchar(100) as cust_dn_num,
        rtn_ord_num::varchar(30) as rtn_ord_num,
        afgr_bill_num::varchar(30) as afgr_bill_num,
        dn_amt_exc_gst_val::numeric(38,9) as dn_amt_exc_gst_val,
        afgr_amt::numeric(38,9) as afgr_amt,
        rtn_ord_amt::numeric(38,9) as rtn_ord_amt,
        cn_amt::numeric(38,9) as cn_amt,
        afgr_status::varchar(30) as afgr_status,
        afgr_val::numeric(38,9) as afgr_val,
        afgr_cn_diff::numeric(38,9) as afgr_cn_diff,
        cur_period_sgt::varchar(23) as cur_period_sgt
    from transformed
) 
select * from final