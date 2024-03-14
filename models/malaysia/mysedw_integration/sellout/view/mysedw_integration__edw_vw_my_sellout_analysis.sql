with edw_vw_my_sellout_sales_fact as (
  select * from {{ ref('mysedw_integration__edw_vw_my_sellout_sales_fact') }}
),
edw_vw_my_dstrbtr_customer_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_dstrbtr_customer_dim') }}
),
edw_vw_my_curr_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_curr_dim') }}
),
edw_vw_os_time_dim as (
  select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_my_customer_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
edw_vw_my_material_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
),
edw_vw_my_listprice as (
  select * from {{ ref('mysedw_integration__edw_vw_my_listprice') }}
),
edw_vw_my_sellout_inventory_fact as (
  select * from {{ ref('mysedw_integration__edw_vw_my_sellout_inventory_fact') }}
),
edw_vw_my_billing_fact as (
  select * from {{ ref('mysedw_integration__edw_vw_my_billing_fact') }} 
),
itg_my_gt_outlet_exclusion as(
  select * from {{ source('mysitg_integration', 'itg_my_gt_outlet_exclusion') }}
),
itg_my_trgts as (
  select * from {{ ref('mysitg_integration__itg_my_trgts') }}
  ),
itg_my_customer_dim as (
  select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
itg_my_sellout_sales_fact as (
  select * from {{ ref('mysitg_integration__itg_my_sellout_sales_fact') }}
),
itg_my_dstrbtrr_dim as (
  select * from {{ ref('mysitg_integration__itg_my_dstrbtrr_dim') }}
),
itg_my_material_dim as (
  select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
 ),
itg_my_gt_outlet_exclusion as (
  select * from {{ source('mysitg_integration', 'itg_my_gt_outlet_exclusion') }}
),
itg_my_in_transit as (
  select * from {{ ref('mysitg_integration__itg_my_in_transit') }}
),
gt_sellout as (
    SELECT 'GT Sellout' AS data_src,
            veosf."year",
            veosf.qrtr_no,
            veosf.mnth_id,
            veosf.mnth_no,
            veosf.mnth_nm,
            (
                "substring"(
                    "replace"(
                        ((ym.bill_date)::character varying)::text,
                        ('-'::character varying)::text,
                        (''::character varying)::text
                    ),
                    0,
                    5
                )
            )::character varying AS max_yearmo,
            'Malaysia' AS cntry_nm,
            veosf.dstrbtr_grp_cd,
            imcd.dstrbtr_grp_nm,
            veosf.cust_cd AS dstrbtr_cust_cd,
            veosf.cust_nm AS dstrbtr_cust_nm,
            (
                ltrim(
                    (imdd.cust_id)::text,
                    ('0'::character varying)::text
                )
            )::character varying AS sap_soldto_code,
            imdd.cust_nm AS sap_soldto_nm,
            imdd.lvl1 AS dstrbtr_lvl1,
            imdd.lvl2 AS dstrbtr_lvl2,
            imdd.lvl3 AS dstrbtr_lvl3,
            imdd.lvl4 AS dstrbtr_lvl4,
            imdd.lvl5 AS dstrbtr_lvl5,
            veosf.region_nm,
            veosf.town_nm,
            veosf.slsmn_cd,
            veosf.chnl_desc,
            veosf.sub_chnl_desc,
            veosf.chnl_attr1_desc,
            veosf.chnl_attr2_desc,
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
            veosf.dstrbtr_matl_num,
            veosf.bar_cd,
            (
                ltrim(
                    (veomd.sap_matl_num)::text,
                    ('0'::character varying)::text
                )
            )::character varying AS sku,
            immd.frnchse_desc,
            immd.brnd_desc,
            immd.vrnt_desc,
            immd.putup_desc,
            immd.item_desc2,
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
            veocurd.from_ccy,
            veocurd.to_ccy,
            veocurd.exch_rate,
            veosf.wh_id,
            veosf.doc_type,
            veosf.doc_type_desc,
            veosf.bill_date,
            veosf.bill_doc,
            (veosf.base_sls * veocurd.exch_rate) AS base_sls,
            veosf.sls_qty,
            veosf.ret_qty,
            veosf.uom,
            veosf.sls_qty_pc,
            veosf.ret_qty_pc,
            0 AS in_transit_qty,
            lp.rate AS mat_list_price,
            (veosf.grs_trd_sls * veocurd.exch_rate) AS grs_trd_sls,
            (veosf.ret_val * veocurd.exch_rate) AS ret_val,
            (veosf.trd_discnt * veocurd.exch_rate) AS trd_discnt,
            (veosf.trd_sls * veocurd.exch_rate) AS trd_sls,
            (veosf.net_trd_sls * veocurd.exch_rate) AS net_trd_sls,
            (veosf.jj_grs_trd_sls * veocurd.exch_rate) AS jj_grs_trd_sls,
            (veosf.jj_ret_val * veocurd.exch_rate) AS jj_ret_val,
            (veosf.jj_trd_sls * veocurd.exch_rate) AS jj_trd_sls,
            (veosf.jj_net_trd_sls * veocurd.exch_rate) AS jj_net_trd_sls,
            0 AS in_transit_val,
            0 AS trgt_val,
            NULL AS inv_dt,
            NULL AS warehse_cd,
            0 AS end_stock_qty,
            0 AS end_stock_val_raw,
            0 AS end_stock_val,
            immd.npi_ind AS is_npi,
            immd.npi_strt_period AS npi_str_period,
            NULL AS npi_end_period,
            NULL AS is_reg,
            immd.promo_reg_ind AS is_promo,
            NULL AS promo_strt_period,
            NULL AS promo_end_period,
            NULL AS is_mcl,
            immd.hero_ind AS is_hero,
            (veosf.contribution * veocurd.exch_rate) AS contribution
        FROM (
                SELECT "max"(EDW_VW_MY_SELLOUT_SALES_FACT.bill_date) AS bill_date
                FROM EDW_VW_MY_SELLOUT_SALES_FACT
                WHERE (
                        (EDW_VW_MY_SELLOUT_SALES_FACT.cntry_cd)::text = ('MY'::character varying)::text
                    )
            ) ym,
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
                                    (
                                        SELECT a."year",
                                            a.qrtr_no,
                                            a.mnth_id,
                                            a.mnth_no,
                                            a.mnth_nm,
                                            a.dstrbtr_grp_cd,
                                            a.dstrbtr_soldto_code,
                                            a.cust_cd,
                                            a.slsmn_cd,
                                            a.cust_nm,
                                            a.sap_soldto_code,
                                            a.sap_matl_num,
                                            (upper(TRIM((a.dstrbtr_matl_num)::text)))::character varying AS dstrbtr_matl_num,
                                            a.bar_cd,
                                            a.region_nm,
                                            a.town_nm,
                                            a.chnl_desc,
                                            a.sub_chnl_desc,
                                            a.chnl_attr1_desc,
                                            a.chnl_attr2_desc,
                                            a.wh_id,
                                            a.doc_type,
                                            a.doc_type_desc,
                                            a.base_sls,
                                            a.bill_date,
                                            a.bill_doc,
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
                                            a.jj_net_trd_sls,
                                            0 AS contribution
                                        FROM (
                                                SELECT veotd."year",
                                                    veotd.qrtr_no,
                                                    veotd.mnth_id,
                                                    veotd.mnth_no,
                                                    veotd.mnth_nm,
                                                    t1.dstrbtr_grp_cd,
                                                    t1.dstrbtr_soldto_code,
                                                    t1.cust_cd,
                                                    t1.slsmn_cd,
                                                    t1.bill_date,
                                                    t1.bill_doc,
                                                    evodcd.cust_nm,
                                                    evodcd.sap_soldto_code,
                                                    t1.sap_matl_num,
                                                    t1.dstrbtr_matl_num,
                                                    t1.bar_cd,
                                                    evodcd.region_nm,
                                                    evodcd.town_nm,
                                                    evodcd.chnl_desc,
                                                    evodcd.sub_chnl_desc,
                                                    evodcd.chnl_attr1_desc,
                                                    evodcd.chnl_attr2_desc,
                                                    t1.wh_id,
                                                    t1.doc_type,
                                                    t1.doc_type_desc,
                                                    t1.base_sls,
                                                    t1.sls_qty,
                                                    t1.ret_qty,
                                                    t1.uom,
                                                    t1.sls_qty_pc,
                                                    t1.ret_qty_pc,
                                                    t1.grs_trd_sls,
                                                    t1.ret_val,
                                                    t1.trd_discnt,
                                                    t1.trd_sls,
                                                    t1.net_trd_sls,
                                                    t1.jj_grs_trd_sls,
                                                    t1.jj_ret_val,
                                                    t1.jj_trd_sls,
                                                    t1.jj_net_trd_sls
                                                FROM (
                                                        SELECT DISTINCT edw_vw_os_time_dim.cal_year AS "year",
                                                            edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                                                            edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                                                            edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                                                            edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                                                            edw_vw_os_time_dim.cal_date,
                                                            edw_vw_os_time_dim.cal_date_id
                                                        FROM edw_vw_os_time_dim
                                                    ) veotd,
                                                    (
                                                        EDW_VW_MY_SELLOUT_SALES_FACT t1
                                                        LEFT JOIN (
                                                            SELECT EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cntry_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cntry_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.dstrbtr_grp_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.dstrbtr_soldto_code,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sap_soldto_code,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.alt_cust_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.alt_cust_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.addr,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.area_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.area_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.state_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.state_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.region_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.region_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.prov_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.prov_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.town_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.town_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.city_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.city_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.post_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.post_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.slsmn_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.slsmn_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sub_chnl_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sub_chnl_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_attr1_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_attr1_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_attr2_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_attr2_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.outlet_type_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.outlet_type_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_attr1_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_attr1_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_attr2_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_attr2_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_dstrct_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_dstrct_nm,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_office_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_office_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_grp_cd,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_grp_desc,
                                                                EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.status
                                                            FROM EDW_VW_MY_DSTRBTR_CUSTOMER_DIM
                                                            WHERE (
                                                                    (EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cntry_cd)::text = ('MY'::character varying)::text
                                                                )
                                                        ) evodcd ON (
                                                            (
                                                                (
                                                                    (
                                                                        ltrim(
                                                                            (evodcd.cust_cd)::text,
                                                                            ('0'::character varying)::text
                                                                        ) = ltrim(
                                                                            (t1.cust_cd)::text,
                                                                            ('0'::character varying)::text
                                                                        )
                                                                    )
                                                                    AND (
                                                                        ltrim(
                                                                            (evodcd.dstrbtr_grp_cd)::text,
                                                                            ('0'::character varying)::text
                                                                        ) = ltrim(
                                                                            (t1.dstrbtr_grp_cd)::text,
                                                                            ('0'::character varying)::text
                                                                        )
                                                                    )
                                                                )
                                                                AND (
                                                                    ltrim(
                                                                        (evodcd.sap_soldto_code)::text,
                                                                        ('0'::character varying)::text
                                                                    ) = ltrim(
                                                                        (t1.dstrbtr_soldto_code)::text,
                                                                        ('0'::character varying)::text
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                WHERE (
                                                        (
                                                            (
                                                                to_date((veotd.cal_date)::timestamp without time zone) = to_date(t1.bill_date)
                                                            )
                                                            AND (
                                                                (t1.cntry_cd)::text = ('MY'::character varying)::text
                                                            )
                                                        )
                                                        AND (
                                                            NOT (
                                                                (
                                                                    COALESCE(
                                                                        ltrim(
                                                                            (t1.dstrbtr_soldto_code)::text,
                                                                            ('0'::character varying)::text
                                                                        ),
                                                                        ('0'::character varying)::text
                                                                    ) || COALESCE(
                                                                        TRIM((t1.cust_cd)::text),
                                                                        ('0'::character varying)::text
                                                                    )
                                                                ) IN (
                                                                    SELECT DISTINCT (
                                                                            (
                                                                                COALESCE(
                                                                                    itg_my_gt_outlet_exclusion.dstrbtr_cd,
                                                                                    '0'::character varying
                                                                                )
                                                                            )::text || (
                                                                                COALESCE(
                                                                                    itg_my_gt_outlet_exclusion.outlet_cd,
                                                                                    '0'::character varying
                                                                                )
                                                                            )::text
                                                                        )
                                                                    FROM itg_my_gt_outlet_exclusion
                                                                )
                                                            )
                                                        )
                                                    )
                                            ) a
                                        UNION ALL
                                        SELECT trgt."year",
                                            trgt.qrtr_no,
                                            trgt.trgt_period AS mnth_id,
                                            trgt.mnth_no,
                                            trgt.mnth_nm,
                                            NULL::character varying AS dstrbtr_grp_cd,
                                            trgt.dstrbtr_id AS dstrbtr_soldto_code,
                                            trgt.cust_cd,
                                            NULL::character varying AS slsmn_cd,
                                            evodcd.cust_nm,
                                            trgt.dstrbtr_id AS sap_soldto_code,
                                            trgt.sap_matl_num,
                                            (upper(TRIM((trgt.dstrbtr_matl_num)::text)))::character varying AS dstrbtr_matl_num,
                                            trgt.bar_cd,
                                            evodcd.region_nm,
                                            evodcd.town_nm,
                                            evodcd.chnl_desc,
                                            evodcd.sub_chnl_desc,
                                            evodcd.chnl_attr1_desc,
                                            evodcd.chnl_attr2_desc,
                                            NULL::character varying AS wh_id,
                                            NULL::character varying AS doc_type,
                                            NULL::character varying AS doc_type_desc,
                                            0 AS base_sls,
                                            NULL::timestamp without time zone AS bill_date,
                                            NULL::character varying AS bill_doc,
                                            0 AS sls_qty,
                                            0 AS ret_qty,
                                            NULL::character varying AS uom,
                                            0 AS sls_qty_pc,
                                            0 AS ret_qty_pc,
                                            0 AS grs_trd_sls,
                                            0 AS ret_val,
                                            0 AS trd_discnt,
                                            0 AS trd_sls,
                                            0 AS net_trd_sls,
                                            0 AS jj_grs_trd_sls,
                                            0 AS jj_ret_val,
                                            0 AS jj_trd_sls,
                                            0 AS jj_net_trd_sls,
                                            trgt.contribution
                                        FROM (
                                                (
                                                    SELECT derived_table1."year",
                                                        derived_table1.qrtr_no,
                                                        derived_table1.mnth_no,
                                                        derived_table1.mnth_nm,
                                                        derived_table1.mnth_id,
                                                        derived_table1.trgt_period,
                                                        derived_table1.dstrbtr_id,
                                                        derived_table1.sap_matl_num,
                                                        derived_table1.dstrbtr_matl_num,
                                                        derived_table1.bar_cd,
                                                        derived_table1.cust_cd,
                                                        sum(derived_table1.contribution) AS contribution
                                                    FROM (
                                                            SELECT t1."year",
                                                                t1.qrtr_no,
                                                                t1.mnth_no,
                                                                t1.mnth_nm,
                                                                t1.mnth_id,
                                                                t1.trgt_period,
                                                                t1.dstrbtr_id,
                                                                t1.sap_matl_num,
                                                                t1.dstrbtr_matl_num,
                                                                t1.bar_cd,
                                                                t1.cust_cd,
                                                                (trgt.trgt_val * (t1.base_sls / t1.total_sales)) AS contribution
                                                            FROM (
                                                                    SELECT (
                                                                            "substring"(
                                                                                to_char(
                                                                                    add_months(
                                                                                        (
                                                                                            to_date(
                                                                                                ((veotd.mnth_id)::character varying)::text,
                                                                                                ('YYYYMM'::character varying)::text
                                                                                            )
                                                                                        )::timestamp without time zone,
                                                                                        (12)::bigint
                                                                                    ),
                                                                                    ('YYYYMM'::character varying)::text
                                                                                ),
                                                                                0,
                                                                                4
                                                                            )
                                                                        )::integer AS "year",
                                                                        veotd.qrtr_no,
                                                                        veotd.mnth_no,
                                                                        veotd.mnth_nm,
                                                                        veotd.mnth_id,
                                                                        (
                                                                            to_char(
                                                                                add_months(
                                                                                    (
                                                                                        to_date(
                                                                                            ((veotd.mnth_id)::character varying)::text,
                                                                                            ('YYYYMM'::character varying)::text
                                                                                        )
                                                                                    )::timestamp without time zone,
                                                                                    (12)::bigint
                                                                                ),
                                                                                ('YYYYMM'::character varying)::text
                                                                            )
                                                                        )::integer AS trgt_period,
                                                                        a.dstrbtr_id,
                                                                        a.dstrbtr_prod_cd AS dstrbtr_matl_num,
                                                                        a.sap_matl_num,
                                                                        a.ean_num AS bar_cd,
                                                                        a.cust_cd,
                                                                        a.total_amt_bfr_tax AS base_sls,
                                                                        sum(a.total_amt_bfr_tax) OVER(
                                                                            PARTITION BY veotd.mnth_id,
                                                                            a.dstrbtr_id ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                        ) AS total_sales
                                                                    FROM (
                                                                            SELECT itg_my_sellout_sales_fact.dstrbtr_id,
                                                                                itg_my_sellout_sales_fact.sls_ord_num,
                                                                                itg_my_sellout_sales_fact.sls_ord_dt,
                                                                                itg_my_sellout_sales_fact.type,
                                                                                itg_my_sellout_sales_fact.cust_cd,
                                                                                itg_my_sellout_sales_fact.dstrbtr_wh_id,
                                                                                itg_my_sellout_sales_fact.item_cd,
                                                                                itg_my_sellout_sales_fact.dstrbtr_prod_cd,
                                                                                itg_my_sellout_sales_fact.ean_num,
                                                                                itg_my_sellout_sales_fact.dstrbtr_prod_desc,
                                                                                itg_my_sellout_sales_fact.grs_prc,
                                                                                itg_my_sellout_sales_fact.qty,
                                                                                itg_my_sellout_sales_fact.uom,
                                                                                itg_my_sellout_sales_fact.qty_pc,
                                                                                itg_my_sellout_sales_fact.qty_aft_conv,
                                                                                itg_my_sellout_sales_fact.subtotal_1,
                                                                                itg_my_sellout_sales_fact.discount,
                                                                                itg_my_sellout_sales_fact.subtotal_2,
                                                                                itg_my_sellout_sales_fact.bottom_line_dscnt,
                                                                                itg_my_sellout_sales_fact.total_amt_aft_tax,
                                                                                itg_my_sellout_sales_fact.total_amt_bfr_tax,
                                                                                itg_my_sellout_sales_fact.sls_emp,
                                                                                itg_my_sellout_sales_fact.custom_field1,
                                                                                itg_my_sellout_sales_fact.custom_field2,
                                                                                itg_my_sellout_sales_fact.sap_matl_num,
                                                                                itg_my_sellout_sales_fact.filename,
                                                                                itg_my_sellout_sales_fact.cdl_dttm,
                                                                                itg_my_sellout_sales_fact.crtd_dttm,
                                                                                itg_my_sellout_sales_fact.updt_dttm
                                                                            FROM itg_my_sellout_sales_fact
                                                                            WHERE (
                                                                                    NOT (
                                                                                        (
                                                                                            COALESCE(
                                                                                                ltrim(
                                                                                                    (itg_my_sellout_sales_fact.dstrbtr_id)::text,
                                                                                                    ('0'::character varying)::text
                                                                                                ),
                                                                                                ('0'::character varying)::text
                                                                                            ) || COALESCE(
                                                                                                TRIM((itg_my_sellout_sales_fact.cust_cd)::text),
                                                                                                ('0'::character varying)::text
                                                                                            )
                                                                                        ) IN (
                                                                                            SELECT DISTINCT (
                                                                                                    (
                                                                                                        COALESCE(
                                                                                                            itg_my_gt_outlet_exclusion.dstrbtr_cd,
                                                                                                            '0'::character varying
                                                                                                        )
                                                                                                    )::text || (
                                                                                                        COALESCE(
                                                                                                            itg_my_gt_outlet_exclusion.outlet_cd,
                                                                                                            '0'::character varying
                                                                                                        )
                                                                                                    )::text
                                                                                                )
                                                                                            FROM itg_my_gt_outlet_exclusion
                                                                                        )
                                                                                    )
                                                                                )
                                                                        ) a,
                                                                        (
                                                                            SELECT DISTINCT edw_vw_os_time_dim.cal_year AS "year",
                                                                                edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                                                                                edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                                                                                edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                                                                                edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                                                                                edw_vw_os_time_dim.cal_date,
                                                                                edw_vw_os_time_dim.cal_date_id
                                                                            FROM edw_vw_os_time_dim
                                                                        ) veotd
                                                                    WHERE (
                                                                            to_date((veotd.cal_date)::timestamp without time zone) = to_date((a.sls_ord_dt)::timestamp without time zone)
                                                                        )
                                                                ) t1,
                                                                (
                                                                    SELECT itg_my_trgts.jj_mnth_id,
                                                                        itg_my_trgts.cust_id,
                                                                        sum(itg_my_trgts.trgt_val) AS trgt_val
                                                                    FROM itg_my_trgts
                                                                    WHERE (
                                                                            (itg_my_trgts.trgt_type)::text = ('BP'::character varying)::text
                                                                        )
                                                                    GROUP BY itg_my_trgts.jj_mnth_id,
                                                                        itg_my_trgts.cust_id
                                                                ) trgt
                                                            WHERE (
                                                                    (
                                                                        (trgt.jj_mnth_id)::text = ((t1.trgt_period)::character varying)::text
                                                                    )
                                                                    AND (
                                                                        (COALESCE(trgt.cust_id, ''::character varying))::text = (COALESCE(t1.dstrbtr_id, ''::character varying))::text
                                                                    )
                                                                )
                                                        ) derived_table1
                                                    GROUP BY derived_table1."year",
                                                        derived_table1.qrtr_no,
                                                        derived_table1.mnth_no,
                                                        derived_table1.mnth_nm,
                                                        derived_table1.mnth_id,
                                                        derived_table1.trgt_period,
                                                        derived_table1.dstrbtr_id,
                                                        derived_table1.sap_matl_num,
                                                        derived_table1.cust_cd,
                                                        derived_table1.dstrbtr_matl_num,
                                                        derived_table1.bar_cd
                                                    UNION
                                                    SELECT derived_table1."year",
                                                        derived_table1.qrtr_no,
                                                        derived_table1.mnth_no,
                                                        derived_table1.mnth_nm,
                                                        derived_table1.mnth_id,
                                                        derived_table1.trgt_period,
                                                        derived_table1.dstrbtr_id,
                                                        derived_table1.sap_matl_num,
                                                        derived_table1.dstrbtr_matl_num,
                                                        derived_table1.bar_cd,
                                                        derived_table1.cust_cd,
                                                        derived_table1.contribution
                                                    FROM (
                                                            SELECT trgt1."year",
                                                                trgt1.qrtr_no,
                                                                trgt1.mnth_no,
                                                                trgt1.mnth_nm,
                                                                trgt1.mnth_id,
                                                                trgt1.trgt_period,
                                                                trgt1.dstrbtr_id,
                                                                trgt1.sap_matl_num,
                                                                trgt1.dstrbtr_matl_num,
                                                                trgt1.bar_cd,
                                                                trgt1.cust_cd,
                                                                trgt1.contribution,
                                                                trgt2.trgt_period AS trgt_period_present,
                                                                trgt2.dstrbtr_id AS dstrbtr_id_present
                                                            FROM (
                                                                    (
                                                                        SELECT derived_table1."year",
                                                                            derived_table1.qrtr_no,
                                                                            derived_table1.mnth_no,
                                                                            derived_table1.mnth_nm,
                                                                            derived_table1.mnth_id,
                                                                            derived_table1.trgt_period,
                                                                            derived_table1.dstrbtr_id,
                                                                            derived_table1.sap_matl_num,
                                                                            derived_table1.dstrbtr_matl_num,
                                                                            derived_table1.bar_cd,
                                                                            derived_table1.cust_cd,
                                                                            sum(derived_table1.contribution) AS contribution
                                                                        FROM (
                                                                                SELECT t1."year",
                                                                                    t1.qrtr_no,
                                                                                    t1.mnth_no,
                                                                                    t1.mnth_nm,
                                                                                    t1.mnth_id,
                                                                                    t1.trgt_period,
                                                                                    t1.dstrbtr_id,
                                                                                    t1.sap_matl_num,
                                                                                    t1.dstrbtr_matl_num,
                                                                                    t1.bar_cd,
                                                                                    t1.cust_cd,
                                                                                    (trgt.trgt_val * (t1.base_sls / t1.total_sales)) AS contribution
                                                                                FROM (
                                                                                        SELECT (
                                                                                                "substring"(
                                                                                                    to_char(
                                                                                                        add_months(
                                                                                                            (
                                                                                                                to_date(
                                                                                                                    ((veotd.mnth_id)::character varying)::text,
                                                                                                                    ('YYYYMM'::character varying)::text
                                                                                                                )
                                                                                                            )::timestamp without time zone,
                                                                                                            (0)::bigint
                                                                                                        ),
                                                                                                        ('YYYYMM'::character varying)::text
                                                                                                    ),
                                                                                                    0,
                                                                                                    4
                                                                                                )
                                                                                            )::integer AS "year",
                                                                                            veotd.qrtr_no,
                                                                                            veotd.mnth_no,
                                                                                            veotd.mnth_nm,
                                                                                            veotd.mnth_id AS trgt_period,
                                                                                            (
                                                                                                to_char(
                                                                                                    add_months(
                                                                                                        (
                                                                                                            to_date(
                                                                                                                ((veotd.mnth_id)::character varying)::text,
                                                                                                                ('YYYYMM'::character varying)::text
                                                                                                            )
                                                                                                        )::timestamp without time zone,
                                                                                                        (- (12)::bigint)
                                                                                                    ),
                                                                                                    ('YYYYMM'::character varying)::text
                                                                                                )
                                                                                            )::integer AS mnth_id,
                                                                                            a.dstrbtr_id,
                                                                                            a.dstrbtr_prod_cd AS dstrbtr_matl_num,
                                                                                            a.sap_matl_num,
                                                                                            a.ean_num AS bar_cd,
                                                                                            a.cust_cd,
                                                                                            a.total_amt_bfr_tax AS base_sls,
                                                                                            sum(a.total_amt_bfr_tax) OVER(
                                                                                                PARTITION BY veotd.mnth_id,
                                                                                                a.dstrbtr_id ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                            ) AS total_sales
                                                                                        FROM (
                                                                                                SELECT itg_my_sellout_sales_fact.dstrbtr_id,
                                                                                                    itg_my_sellout_sales_fact.sls_ord_num,
                                                                                                    itg_my_sellout_sales_fact.sls_ord_dt,
                                                                                                    itg_my_sellout_sales_fact.type,
                                                                                                    itg_my_sellout_sales_fact.cust_cd,
                                                                                                    itg_my_sellout_sales_fact.dstrbtr_wh_id,
                                                                                                    itg_my_sellout_sales_fact.item_cd,
                                                                                                    itg_my_sellout_sales_fact.dstrbtr_prod_cd,
                                                                                                    itg_my_sellout_sales_fact.ean_num,
                                                                                                    itg_my_sellout_sales_fact.dstrbtr_prod_desc,
                                                                                                    itg_my_sellout_sales_fact.grs_prc,
                                                                                                    itg_my_sellout_sales_fact.qty,
                                                                                                    itg_my_sellout_sales_fact.uom,
                                                                                                    itg_my_sellout_sales_fact.qty_pc,
                                                                                                    itg_my_sellout_sales_fact.qty_aft_conv,
                                                                                                    itg_my_sellout_sales_fact.subtotal_1,
                                                                                                    itg_my_sellout_sales_fact.discount,
                                                                                                    itg_my_sellout_sales_fact.subtotal_2,
                                                                                                    itg_my_sellout_sales_fact.bottom_line_dscnt,
                                                                                                    itg_my_sellout_sales_fact.total_amt_aft_tax,
                                                                                                    itg_my_sellout_sales_fact.total_amt_bfr_tax,
                                                                                                    itg_my_sellout_sales_fact.sls_emp,
                                                                                                    itg_my_sellout_sales_fact.custom_field1,
                                                                                                    itg_my_sellout_sales_fact.custom_field2,
                                                                                                    itg_my_sellout_sales_fact.sap_matl_num,
                                                                                                    itg_my_sellout_sales_fact.filename,
                                                                                                    itg_my_sellout_sales_fact.cdl_dttm,
                                                                                                    itg_my_sellout_sales_fact.crtd_dttm,
                                                                                                    itg_my_sellout_sales_fact.updt_dttm
                                                                                                FROM itg_my_sellout_sales_fact
                                                                                                WHERE (
                                                                                                        NOT (
                                                                                                            (
                                                                                                                COALESCE(
                                                                                                                    ltrim(
                                                                                                                        (itg_my_sellout_sales_fact.dstrbtr_id)::text,
                                                                                                                        ('0'::character varying)::text
                                                                                                                    ),
                                                                                                                    ('0'::character varying)::text
                                                                                                                ) || COALESCE(
                                                                                                                    TRIM((itg_my_sellout_sales_fact.cust_cd)::text),
                                                                                                                    ('0'::character varying)::text
                                                                                                                )
                                                                                                            ) IN (
                                                                                                                SELECT DISTINCT (
                                                                                                                        (
                                                                                                                            COALESCE(
                                                                                                                                itg_my_gt_outlet_exclusion.dstrbtr_cd,
                                                                                                                                '0'::character varying
                                                                                                                            )
                                                                                                                        )::text || (
                                                                                                                            COALESCE(
                                                                                                                                itg_my_gt_outlet_exclusion.outlet_cd,
                                                                                                                                '0'::character varying
                                                                                                                            )
                                                                                                                        )::text
                                                                                                                    )
                                                                                                                FROM itg_my_gt_outlet_exclusion
                                                                                                            )
                                                                                                        )
                                                                                                    )
                                                                                            ) a,
                                                                                            (
                                                                                                SELECT DISTINCT edw_vw_os_time_dim.cal_year AS "year",
                                                                                                    edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                                                                                                    edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                                                                                                    edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                                                                                                    edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                                                                                                    edw_vw_os_time_dim.cal_date,
                                                                                                    edw_vw_os_time_dim.cal_date_id
                                                                                                FROM edw_vw_os_time_dim
                                                                                            ) veotd
                                                                                        WHERE (
                                                                                                to_date((veotd.cal_date)::timestamp without time zone) = to_date((a.sls_ord_dt)::timestamp without time zone)
                                                                                            )
                                                                                    ) t1,
                                                                                    (
                                                                                        SELECT itg_my_trgts.jj_mnth_id,
                                                                                            itg_my_trgts.cust_id,
                                                                                            sum(itg_my_trgts.trgt_val) AS trgt_val
                                                                                        FROM itg_my_trgts
                                                                                        WHERE (
                                                                                                (itg_my_trgts.trgt_type)::text = ('BP'::character varying)::text
                                                                                            )
                                                                                        GROUP BY itg_my_trgts.jj_mnth_id,
                                                                                            itg_my_trgts.cust_id
                                                                                    ) trgt
                                                                                WHERE (
                                                                                        (
                                                                                            (trgt.jj_mnth_id)::text = ((t1.trgt_period)::character varying)::text
                                                                                        )
                                                                                        AND (
                                                                                            (COALESCE(trgt.cust_id, ''::character varying))::text = (COALESCE(t1.dstrbtr_id, ''::character varying))::text
                                                                                        )
                                                                                    )
                                                                            ) derived_table1
                                                                        GROUP BY derived_table1."year",
                                                                            derived_table1.qrtr_no,
                                                                            derived_table1.mnth_no,
                                                                            derived_table1.mnth_nm,
                                                                            derived_table1.mnth_id,
                                                                            derived_table1.trgt_period,
                                                                            derived_table1.dstrbtr_id,
                                                                            derived_table1.sap_matl_num,
                                                                            derived_table1.cust_cd,
                                                                            derived_table1.dstrbtr_matl_num,
                                                                            derived_table1.bar_cd
                                                                    ) trgt1
                                                                    LEFT JOIN (
                                                                        SELECT derived_table1."year",
                                                                            derived_table1.qrtr_no,
                                                                            derived_table1.mnth_no,
                                                                            derived_table1.mnth_nm,
                                                                            derived_table1.mnth_id,
                                                                            derived_table1.trgt_period,
                                                                            derived_table1.dstrbtr_id,
                                                                            derived_table1.sap_matl_num,
                                                                            derived_table1.dstrbtr_matl_num,
                                                                            derived_table1.bar_cd,
                                                                            derived_table1.cust_cd,
                                                                            sum(derived_table1.contribution) AS contribution
                                                                        FROM (
                                                                                SELECT t1."year",
                                                                                    t1.qrtr_no,
                                                                                    t1.mnth_no,
                                                                                    t1.mnth_nm,
                                                                                    t1.mnth_id,
                                                                                    t1.trgt_period,
                                                                                    t1.dstrbtr_id,
                                                                                    t1.sap_matl_num,
                                                                                    t1.dstrbtr_matl_num,
                                                                                    t1.bar_cd,
                                                                                    t1.cust_cd,
                                                                                    (trgt.trgt_val * (t1.base_sls / t1.total_sales)) AS contribution
                                                                                FROM (
                                                                                        SELECT (
                                                                                                "substring"(
                                                                                                    to_char(
                                                                                                        add_months(
                                                                                                            (
                                                                                                                to_date(
                                                                                                                    ((veotd.mnth_id)::character varying)::text,
                                                                                                                    ('YYYYMM'::character varying)::text
                                                                                                                )
                                                                                                            )::timestamp without time zone,
                                                                                                            (12)::bigint
                                                                                                        ),
                                                                                                        ('YYYYMM'::character varying)::text
                                                                                                    ),
                                                                                                    0,
                                                                                                    4
                                                                                                )
                                                                                            )::integer AS "year",
                                                                                            veotd.qrtr_no,
                                                                                            veotd.mnth_no,
                                                                                            veotd.mnth_nm,
                                                                                            veotd.mnth_id,
                                                                                            (
                                                                                                to_char(
                                                                                                    add_months(
                                                                                                        (
                                                                                                            to_date(
                                                                                                                ((veotd.mnth_id)::character varying)::text,
                                                                                                                ('YYYYMM'::character varying)::text
                                                                                                            )
                                                                                                        )::timestamp without time zone,
                                                                                                        (12)::bigint
                                                                                                    ),
                                                                                                    ('YYYYMM'::character varying)::text
                                                                                                )
                                                                                            )::integer AS trgt_period,
                                                                                            a.dstrbtr_id,
                                                                                            a.dstrbtr_prod_cd AS dstrbtr_matl_num,
                                                                                            a.sap_matl_num,
                                                                                            a.ean_num AS bar_cd,
                                                                                            a.cust_cd,
                                                                                            a.total_amt_bfr_tax AS base_sls,
                                                                                            sum(a.total_amt_bfr_tax) OVER(
                                                                                                PARTITION BY veotd.mnth_id,
                                                                                                a.dstrbtr_id ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                            ) AS total_sales
                                                                                        FROM (
                                                                                                SELECT itg_my_sellout_sales_fact.dstrbtr_id,
                                                                                                    itg_my_sellout_sales_fact.sls_ord_num,
                                                                                                    itg_my_sellout_sales_fact.sls_ord_dt,
                                                                                                    itg_my_sellout_sales_fact.type,
                                                                                                    itg_my_sellout_sales_fact.cust_cd,
                                                                                                    itg_my_sellout_sales_fact.dstrbtr_wh_id,
                                                                                                    itg_my_sellout_sales_fact.item_cd,
                                                                                                    itg_my_sellout_sales_fact.dstrbtr_prod_cd,
                                                                                                    itg_my_sellout_sales_fact.ean_num,
                                                                                                    itg_my_sellout_sales_fact.dstrbtr_prod_desc,
                                                                                                    itg_my_sellout_sales_fact.grs_prc,
                                                                                                    itg_my_sellout_sales_fact.qty,
                                                                                                    itg_my_sellout_sales_fact.uom,
                                                                                                    itg_my_sellout_sales_fact.qty_pc,
                                                                                                    itg_my_sellout_sales_fact.qty_aft_conv,
                                                                                                    itg_my_sellout_sales_fact.subtotal_1,
                                                                                                    itg_my_sellout_sales_fact.discount,
                                                                                                    itg_my_sellout_sales_fact.subtotal_2,
                                                                                                    itg_my_sellout_sales_fact.bottom_line_dscnt,
                                                                                                    itg_my_sellout_sales_fact.total_amt_aft_tax,
                                                                                                    itg_my_sellout_sales_fact.total_amt_bfr_tax,
                                                                                                    itg_my_sellout_sales_fact.sls_emp,
                                                                                                    itg_my_sellout_sales_fact.custom_field1,
                                                                                                    itg_my_sellout_sales_fact.custom_field2,
                                                                                                    itg_my_sellout_sales_fact.sap_matl_num,
                                                                                                    itg_my_sellout_sales_fact.filename,
                                                                                                    itg_my_sellout_sales_fact.cdl_dttm,
                                                                                                    itg_my_sellout_sales_fact.crtd_dttm,
                                                                                                    itg_my_sellout_sales_fact.updt_dttm
                                                                                                FROM itg_my_sellout_sales_fact
                                                                                                WHERE (
                                                                                                        NOT (
                                                                                                            (
                                                                                                                COALESCE(
                                                                                                                    ltrim(
                                                                                                                        (itg_my_sellout_sales_fact.dstrbtr_id)::text,
                                                                                                                        ('0'::character varying)::text
                                                                                                                    ),
                                                                                                                    ('0'::character varying)::text
                                                                                                                ) || COALESCE(
                                                                                                                    TRIM((itg_my_sellout_sales_fact.cust_cd)::text),
                                                                                                                    ('0'::character varying)::text
                                                                                                                )
                                                                                                            ) IN (
                                                                                                                SELECT DISTINCT (
                                                                                                                        (
                                                                                                                            COALESCE(
                                                                                                                                itg_my_gt_outlet_exclusion.dstrbtr_cd,
                                                                                                                                '0'::character varying
                                                                                                                            )
                                                                                                                        )::text || (
                                                                                                                            COALESCE(
                                                                                                                                itg_my_gt_outlet_exclusion.outlet_cd,
                                                                                                                                '0'::character varying
                                                                                                                            )
                                                                                                                        )::text
                                                                                                                    )
                                                                                                                FROM itg_my_gt_outlet_exclusion
                                                                                                            )
                                                                                                        )
                                                                                                    )
                                                                                            ) a,
                                                                                            (
                                                                                                SELECT DISTINCT edw_vw_os_time_dim.cal_year AS "year",
                                                                                                    edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                                                                                                    edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                                                                                                    edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                                                                                                    edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                                                                                                    edw_vw_os_time_dim.cal_date,
                                                                                                    edw_vw_os_time_dim.cal_date_id
                                                                                                FROM edw_vw_os_time_dim
                                                                                            ) veotd
                                                                                        WHERE (
                                                                                                to_date((veotd.cal_date)::timestamp without time zone) = to_date((a.sls_ord_dt)::timestamp without time zone)
                                                                                            )
                                                                                    ) t1,
                                                                                    (
                                                                                        SELECT itg_my_trgts.jj_mnth_id,
                                                                                            itg_my_trgts.cust_id,
                                                                                            sum(itg_my_trgts.trgt_val) AS trgt_val
                                                                                        FROM itg_my_trgts
                                                                                        WHERE (
                                                                                                (itg_my_trgts.trgt_type)::text = ('BP'::character varying)::text
                                                                                            )
                                                                                        GROUP BY itg_my_trgts.jj_mnth_id,
                                                                                            itg_my_trgts.cust_id
                                                                                    ) trgt
                                                                                WHERE (
                                                                                        (
                                                                                            (trgt.jj_mnth_id)::text = ((t1.trgt_period)::character varying)::text
                                                                                        )
                                                                                        AND (
                                                                                            (COALESCE(trgt.cust_id, ''::character varying))::text = (COALESCE(t1.dstrbtr_id, ''::character varying))::text
                                                                                        )
                                                                                    )
                                                                            ) derived_table1
                                                                        GROUP BY derived_table1."year",
                                                                            derived_table1.qrtr_no,
                                                                            derived_table1.mnth_no,
                                                                            derived_table1.mnth_nm,
                                                                            derived_table1.mnth_id,
                                                                            derived_table1.trgt_period,
                                                                            derived_table1.dstrbtr_id,
                                                                            derived_table1.sap_matl_num,
                                                                            derived_table1.cust_cd,
                                                                            derived_table1.dstrbtr_matl_num,
                                                                            derived_table1.bar_cd
                                                                    ) trgt2 ON (
                                                                        (
                                                                            (trgt1.trgt_period = trgt2.trgt_period)
                                                                            AND (
                                                                                (trgt1.dstrbtr_id)::text = (trgt2.dstrbtr_id)::text
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                        ) derived_table1
                                                    WHERE (
                                                            (derived_table1.trgt_period_present IS NULL)
                                                            OR (derived_table1.dstrbtr_id_present IS NULL)
                                                        )
                                                ) trgt
                                                LEFT JOIN (
                                                    SELECT EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cntry_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cntry_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.dstrbtr_grp_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.dstrbtr_soldto_code,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sap_soldto_code,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.alt_cust_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.alt_cust_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.addr,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.area_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.area_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.state_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.state_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.region_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.region_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.prov_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.prov_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.town_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.town_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.city_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.city_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.post_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.post_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.slsmn_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.slsmn_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sub_chnl_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sub_chnl_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_attr1_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_attr1_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_attr2_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.chnl_attr2_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.outlet_type_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.outlet_type_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_attr1_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_attr1_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_attr2_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cust_grp_attr2_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_dstrct_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_dstrct_nm,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_office_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_office_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_grp_cd,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.sls_grp_desc,
                                                        EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.status
                                                    FROM EDW_VW_MY_DSTRBTR_CUSTOMER_DIM
                                                    WHERE (
                                                            (EDW_VW_MY_DSTRBTR_CUSTOMER_DIM.cntry_cd)::text = ('MY'::character varying)::text
                                                        )
                                                ) evodcd ON (
                                                    (
                                                        (
                                                            ltrim(
                                                                (evodcd.cust_cd)::text,
                                                                ('0'::character varying)::text
                                                            ) = ltrim(
                                                                (trgt.cust_cd)::text,
                                                                ('0'::character varying)::text
                                                            )
                                                        )
                                                        AND (
                                                            ltrim(
                                                                (evodcd.sap_soldto_code)::text,
                                                                ('0'::character varying)::text
                                                            ) = ltrim(
                                                                (trgt.dstrbtr_id)::text,
                                                                ('0'::character varying)::text
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                    ) veosf
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
                                            ) = ltrim(
                                                (veosf.sap_soldto_code)::text,
                                                ('0'::character varying)::text
                                            )
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
                                        ) = ltrim(
                                            (veosf.sap_matl_num)::text,
                                            ('0'::character varying)::text
                                        )
                                    )
                                )
                            )
                            LEFT JOIN itg_my_dstrbtrr_dim imdd ON (
                                (
                                    ltrim(
                                        (imdd.cust_id)::text,
                                        ('0'::character varying)::text
                                    ) = ltrim(
                                        (veosf.sap_soldto_code)::text,
                                        ('0'::character varying)::text
                                    )
                                )
                            )
                        )
                        LEFT JOIN itg_my_material_dim immd ON (
                            (
                                ltrim(
                                    (immd.item_cd)::text,
                                    ('0'::character varying)::text
                                ) = ltrim(
                                    (
                                        COALESCE(veosf.sap_matl_num, ''::character varying)
                                    )::text,
                                    ('0'::character varying)::text
                                )
                            )
                        )
                    )
                    LEFT JOIN itg_my_customer_dim imcd ON (
                        (
                            TRIM((imcd.cust_id)::text) = TRIM((veosf.sap_soldto_code)::text)
                        )
                    )
                )
                LEFT JOIN (
                    SELECT EDW_VW_my_LISTPRICE.cntry_key,
                        EDW_VW_my_LISTPRICE.cntry_nm,
                        EDW_VW_my_LISTPRICE.plant,
                        EDW_VW_my_LISTPRICE.cnty,
                        EDW_VW_my_LISTPRICE.item_cd,
                        EDW_VW_my_LISTPRICE.item_desc,
                        EDW_VW_my_LISTPRICE.valid_from,
                        EDW_VW_my_LISTPRICE.valid_to,
                        EDW_VW_my_LISTPRICE.rate,
                        EDW_VW_my_LISTPRICE.currency,
                        EDW_VW_my_LISTPRICE.price_unit,
                        EDW_VW_my_LISTPRICE.uom,
                        EDW_VW_my_LISTPRICE.yearmo,
                        EDW_VW_my_LISTPRICE.mnth_type,
                        EDW_VW_my_LISTPRICE.snapshot_dt
                    FROM EDW_VW_my_LISTPRICE
                    WHERE (
                            (
                                (EDW_VW_my_LISTPRICE.cntry_key)::text = ('MY'::character varying)::text
                            )
                            AND (
                                (EDW_VW_my_LISTPRICE.mnth_type)::text = ('CAL'::character varying)::text
                            )
                        )
                ) lp ON (
                    (
                        (
                            ltrim(
                                (lp.item_cd)::text,
                                ('0'::character varying)::text
                            ) = ltrim(
                                (veosf.sap_matl_num)::text,
                                ('0'::character varying)::text
                            )
                        )
                        AND (
                            (lp.yearmo)::text = ((veosf.mnth_id)::character varying)::text
                        )
                    )
                )
            )
        WHERE (
                (
                    ((veosf.mnth_id)::character varying)::text >= veocurd.start_period
                )
                AND (
                    ((veosf.mnth_id)::character varying)::text <= veocurd.end_period
                )
            )
),
target as (
    SELECT 'Target' AS data_src,
            veotd."year",
            veotd.qrtr_no,
            veotd.mnth_id,
            veotd.mnth_no,
            veotd.mnth_nm,
            NULL AS max_yearmo,
            'Malaysia' AS cntry_nm,
            imcd.dstrbtr_grp_cd,
            imcd.dstrbtr_grp_nm,
            NULL AS dstrbtr_cust_cd,
            NULL AS dstrbtr_cust_nm,
            (
                ltrim(
                    (imdd.cust_id)::text,
                    ('0'::character varying)::text
                )
            )::character varying AS sap_soldto_code,
            imdd.cust_nm AS sap_soldto_nm,
            imdd.lvl1 AS dstrbtr_lvl1,
            imdd.lvl2 AS dstrbtr_lvl2,
            imdd.lvl3 AS dstrbtr_lvl3,
            imdd.lvl4 AS dstrbtr_lvl4,
            imdd.lvl5 AS dstrbtr_lvl5,
            NULL AS region_nm,
            NULL AS town_nm,
            NULL AS slsmn_cd,
            NULL AS chnl_desc,
            NULL AS sub_chnl_desc,
            NULL AS chnl_attr1_desc,
            NULL AS chnl_attr2_desc,
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
            NULL AS dstrbtr_matl_num,
            NULL AS bar_cd,
            NULL AS sku,
            NULL AS frnchse_desc,
            NULL AS brnd_desc,
            NULL AS vrnt_desc,
            NULL AS putup_desc,
            NULL AS item_desc2,
            NULL AS sku_desc,
            NULL AS sap_mat_type_cd,
            NULL AS sap_mat_type_desc,
            NULL AS sap_base_uom_cd,
            NULL AS sap_prchse_uom_cd,
            NULL AS sap_prod_sgmt_cd,
            NULL AS sap_prod_sgmt_desc,
            NULL AS sap_base_prod_cd,
            NULL AS sap_base_prod_desc,
            NULL AS sap_mega_brnd_cd,
            NULL AS sap_mega_brnd_desc,
            NULL AS sap_brnd_cd,
            NULL AS sap_brnd_desc,
            NULL AS sap_vrnt_cd,
            NULL AS sap_vrnt_desc,
            NULL AS sap_put_up_cd,
            NULL AS sap_put_up_desc,
            NULL AS sap_grp_frnchse_cd,
            NULL AS sap_grp_frnchse_desc,
            NULL AS sap_frnchse_cd,
            NULL AS sap_frnchse_desc,
            NULL AS sap_prod_frnchse_cd,
            NULL AS sap_prod_frnchse_desc,
            NULL AS sap_prod_mjr_cd,
            NULL AS sap_prod_mjr_desc,
            NULL AS sap_prod_mnr_cd,
            NULL AS sap_prod_mnr_desc,
            NULL AS sap_prod_hier_cd,
            NULL AS sap_prod_hier_desc,
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
            NULL AS global_prod_sub_brand,
            NULL AS global_prod_need_state,
            NULL AS global_prod_category,
            NULL AS global_prod_subcategory,
            NULL AS global_prod_segment,
            veomd1.gph_prod_subsgmnt AS global_prod_subsegment,
            NULL AS global_prod_size,
            NULL AS global_prod_size_uom,
            veocurd.from_ccy,
            veocurd.to_ccy,
            veocurd.exch_rate,
            NULL AS wh_id,
            NULL AS doc_type,
            NULL AS doc_type_desc,
            NULL AS bill_date,
            NULL AS bill_doc,
            0 AS base_sls,
            0 AS sls_qty,
            0 AS ret_qty,
            NULL AS uom,
            0 AS sls_qty_pc,
            0 AS ret_qty_pc,
            0 AS in_transit_qty,
            0 AS mat_list_price,
            0 AS grs_trd_sls,
            0 AS ret_val,
            0 AS trd_discnt,
            0 AS trd_sls,
            0 AS net_trd_sls,
            0 AS jj_grs_trd_sls,
            0 AS jj_ret_val,
            0 AS jj_trd_sls,
            0 AS jj_net_trd_sls,
            0 AS in_transit_val,
            (imt.trgt_val * veocurd.exch_rate) AS trgt_val,
            NULL AS inv_dt,
            NULL AS warehse_cd,
            0 AS end_stock_qty,
            0 AS end_stock_val_raw,
            0 AS end_stock_val,
            NULL AS is_npi,
            NULL AS npi_str_period,
            NULL AS npi_end_period,
            NULL AS is_reg,
            NULL AS is_promo,
            NULL AS promo_strt_period,
            NULL AS promo_end_period,
            NULL AS is_mcl,
            NULL AS is_hero,
            0 AS contribution
        FROM (
                SELECT edw_vw_os_time_dim.cal_year AS "year",
                    edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                    edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                    edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                    edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm
                FROM edw_vw_os_time_dim
                GROUP BY edw_vw_os_time_dim.cal_year,
                    edw_vw_os_time_dim.cal_qrtr_no,
                    edw_vw_os_time_dim.cal_mnth_id,
                    edw_vw_os_time_dim.cal_mnth_no,
                    edw_vw_os_time_dim.cal_mnth_nm
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
                                itg_my_trgts imt
                                LEFT JOIN itg_my_customer_dim imcd ON (
                                    (
                                        ltrim(
                                            (imcd.cust_id)::text,
                                            ('0'::character varying)::text
                                        ) = ltrim(
                                            (imt.cust_id)::text,
                                            ('0'::character varying)::text
                                        )
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
                                        TRIM(upper((veomd.gph_prod_brnd)::text)) = TRIM(upper((imt.brnd_desc)::text))
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
                                        TRIM(upper((veomd1.gph_prod_subsgmnt)::text)) = TRIM(upper((imt.sub_segment)::text))
                                    )
                                    AND (
                                        TRIM(upper((veomd1.gph_prod_brnd)::text)) = TRIM(upper((imt.brnd_desc)::text))
                                    )
                                )
                                ELSE NULL::boolean
                            END
                        )
                    )
                    LEFT JOIN itg_my_dstrbtrr_dim imdd ON (
                        (
                            ltrim(
                                (imdd.cust_id)::text,
                                ('0'::character varying)::text
                            ) = ltrim(
                                (imt.cust_id)::text,
                                ('0'::character varying)::text
                            )
                        )
                    )
                )
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
                        ) = ltrim(
                            (imt.cust_id)::text,
                            ('0'::character varying)::text
                        )
                    )
                )
            )
        WHERE (
                (
                    (
                        (
                            (imt.trgt_type)::text = ('BP'::character varying)::text
                        )
                        AND (
                            ((veotd.mnth_id)::character varying)::text = (imt.jj_mnth_id)::text
                        )
                    )
                    AND ((imt.jj_mnth_id)::text >= veocurd.start_period)
                )
                AND ((imt.jj_mnth_id)::text <= veocurd.end_period)
            )
),
gt_inventory as (
    SELECT 'GT Inventory' AS data_src,
        evosif."year",
        evosif.qrtr_no,
        evosif.mnth_id,
        evosif.mnth_no,
        evosif.mnth_nm,
        NULL AS max_yearmo,
        'Malaysia' AS cntry_nm,
        evosif.dstrbtr_grp_cd,
        imcd.dstrbtr_grp_nm,
        NULL AS dstrbtr_cust_cd,
        NULL AS dstrbtr_cust_nm,
        (
            ltrim(
                (imdd.cust_id)::text,
                ('0'::character varying)::text
            )
        )::character varying AS sap_soldto_code,
        imdd.cust_nm AS sap_soldto_nm,
        imdd.lvl1 AS dstrbtr_lvl1,
        imdd.lvl2 AS dstrbtr_lvl2,
        imdd.lvl3 AS dstrbtr_lvl3,
        imdd.lvl4 AS dstrbtr_lvl4,
        imdd.lvl5 AS dstrbtr_lvl5,
        imdd.region AS region_nm,
        NULL AS town_nm,
        NULL AS slsmn_cd,
        NULL AS chnl_desc,
        NULL AS sub_chnl_desc,
        NULL AS chnl_attr1_desc,
        NULL AS chnl_attr2_desc,
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
        (evosif.dstrbtr_matl_num)::character varying AS dstrbtr_matl_num,
        evosif.bar_cd,
        (
            ltrim(
                (veomd.sap_matl_num)::text,
                ('0'::character varying)::text
            )
        )::character varying AS sku,
        immd.frnchse_desc,
        immd.brnd_desc,
        immd.vrnt_desc,
        immd.putup_desc,
        immd.item_desc2,
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
        veocurd.from_ccy,
        veocurd.to_ccy,
        veocurd.exch_rate,
        NULL AS wh_id,
        NULL AS doc_type,
        NULL AS doc_type_desc,
        NULL AS bill_date,
        NULL AS bill_doc,
        0 AS base_sls,
        0 AS sls_qty,
        0 AS ret_qty,
        NULL AS uom,
        0 AS sls_qty_pc,
        0 AS ret_qty_pc,
        0 AS in_transit_qty,
        lp.rate AS mat_list_price,
        0 AS grs_trd_sls,
        0 AS ret_val,
        0 AS trd_discnt,
        0 AS trd_sls,
        0 AS net_trd_sls,
        0 AS jj_grs_trd_sls,
        0 AS jj_ret_val,
        0 AS jj_trd_sls,
        0 AS jj_net_trd_sls,
        0 AS in_transit_val,
        0 AS trgt_val,
        (to_date(evosif.inv_dt))::character varying AS inv_dt,
        evosif.warehse_cd,
        evosif.end_stock_qty,
        (evosif.end_stock_val * veocurd.exch_rate) AS end_stock_val_raw,
        (
            (evosif.end_stock_qty * veocurd.exch_rate) * lp.rate
        ) AS end_stock_val,
        immd.npi_ind AS is_npi,
        immd.npi_strt_period AS npi_str_period,
        NULL AS npi_end_period,
        NULL AS is_reg,
        immd.promo_reg_ind AS is_promo,
        NULL AS promo_strt_period,
        NULL AS promo_end_period,
        NULL AS is_mcl,
        immd.hero_ind AS is_hero,
        0 AS contribution
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
            (
                (
                    (
                        (
                            (
                                (
                                    SELECT veotd."year",
                                        veotd.qrtr_no,
                                        veotd.mnth_id,
                                        veotd.mnth_no,
                                        veotd.mnth_nm,
                                        t1.warehse_cd,
                                        t1.dstrbtr_grp_cd,
                                        t1.dstrbtr_soldto_code,
                                        t1.bar_cd,
                                        t1.sap_matl_num,
                                        TRIM((t1.dstrbtr_matl_num)::text) AS dstrbtr_matl_num,
                                        t1.inv_dt,
                                        t1.soh,
                                        t1.soh_val,
                                        t1.end_stock_qty,
                                        t1.end_stock_val
                                    FROM EDW_VW_my_SELLOUT_INVENTORY_FACT t1,
                                        (
                                            SELECT DISTINCT edw_vw_os_time_dim.cal_year AS "year",
                                                edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                                                edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                                                edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                                                edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                                                edw_vw_os_time_dim.cal_date,
                                                edw_vw_os_time_dim.cal_date_id
                                            FROM edw_vw_os_time_dim
                                        ) veotd
                                    WHERE (
                                            (
                                                (t1.cntry_cd)::text = ('MY'::character varying)::text
                                            )
                                            AND (
                                                to_date((veotd.cal_date)::timestamp without time zone) = to_date(t1.inv_dt)
                                            )
                                        )
                                ) evosif
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
                                        ) = ltrim(
                                            (evosif.dstrbtr_soldto_code)::text,
                                            ('0'::character varying)::text
                                        )
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
                                    ) = ltrim(
                                        (
                                            COALESCE(evosif.sap_matl_num, ''::character varying)
                                        )::text,
                                        ('0'::character varying)::text
                                    )
                                )
                            )
                        )
                        LEFT JOIN itg_my_dstrbtrr_dim imdd ON (
                            (
                                ltrim(
                                    (imdd.cust_id)::text,
                                    ('0'::character varying)::text
                                ) = ltrim(
                                    (evosif.dstrbtr_soldto_code)::text,
                                    ('0'::character varying)::text
                                )
                            )
                        )
                    )
                    LEFT JOIN itg_my_material_dim immd ON (
                        (
                            ltrim(
                                (immd.item_cd)::text,
                                ('0'::character varying)::text
                            ) = ltrim(
                                (evosif.sap_matl_num)::text,
                                ('0'::character varying)::text
                            )
                        )
                    )
                )
                LEFT JOIN itg_my_customer_dim imcd ON (
                    (
                        TRIM((imcd.cust_id)::text) = TRIM((evosif.dstrbtr_soldto_code)::text)
                    )
                )
            )
            LEFT JOIN (
                SELECT EDW_VW_my_LISTPRICE.cntry_key,
                    EDW_VW_my_LISTPRICE.cntry_nm,
                    EDW_VW_my_LISTPRICE.plant,
                    EDW_VW_my_LISTPRICE.cnty,
                    EDW_VW_my_LISTPRICE.item_cd,
                    EDW_VW_my_LISTPRICE.item_desc,
                    EDW_VW_my_LISTPRICE.valid_from,
                    EDW_VW_my_LISTPRICE.valid_to,
                    EDW_VW_my_LISTPRICE.rate,
                    EDW_VW_my_LISTPRICE.currency,
                    EDW_VW_my_LISTPRICE.price_unit,
                    EDW_VW_my_LISTPRICE.uom,
                    EDW_VW_my_LISTPRICE.yearmo,
                    EDW_VW_my_LISTPRICE.mnth_type,
                    EDW_VW_my_LISTPRICE.snapshot_dt
                FROM EDW_VW_my_LISTPRICE
                WHERE (
                        (
                            (EDW_VW_my_LISTPRICE.cntry_key)::text = ('MY'::character varying)::text
                        )
                        AND (
                            (EDW_VW_my_LISTPRICE.mnth_type)::text = ('CAL'::character varying)::text
                        )
                    )
            ) lp ON (
                (
                    (
                        ltrim(
                            (lp.item_cd)::text,
                            ('0'::character varying)::text
                        ) = ltrim(
                            (evosif.sap_matl_num)::text,
                            ('0'::character varying)::text
                        )
                    )
                    AND (
                        (lp.yearmo)::text = ((evosif.mnth_id)::character varying)::text
                    )
                )
            )
        )
    WHERE (
            (
                ((evosif.mnth_id)::character varying)::text >= veocurd.start_period
            )
            AND (
                ((evosif.mnth_id)::character varying)::text <= veocurd.end_period
            )
        )
),
in_transit as (
    SELECT 'IN Transit' AS data_src,
    veoint."year",
    veoint.qrtr_no,
    veoint.mnth_id,
    veoint.mnth_no,
    veoint.mnth_nm,
    NULL AS max_yearmo,
    'Malaysia' AS cntry_nm,
    imcd.dstrbtr_grp_cd,
    imcd.dstrbtr_grp_nm,
    NULL AS dstrbtr_cust_cd,
    NULL AS dstrbtr_cust_nm,
    (
        ltrim(
            (imdd.cust_id)::text,
            ('0'::character varying)::text
        )
    )::character varying AS sap_soldto_code,
    imdd.cust_nm AS sap_soldto_nm,
    imdd.lvl1 AS dstrbtr_lvl1,
    imdd.lvl2 AS dstrbtr_lvl2,
    imdd.lvl3 AS dstrbtr_lvl3,
    imdd.lvl4 AS dstrbtr_lvl4,
    imdd.lvl5 AS dstrbtr_lvl5,
    imdd.region AS region_nm,
    NULL AS town_nm,
    NULL AS slsmn_cd,
    NULL AS chnl_desc,
    NULL AS sub_chnl_desc,
    NULL AS chnl_attr1_desc,
    NULL AS chnl_attr2_desc,
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
    NULL AS dstrbtr_matl_num,
    NULL AS bar_cd,
    (
        ltrim(
            (veomd.sap_matl_num)::text,
            ('0'::character varying)::text
        )
    )::character varying AS sku,
    immd.frnchse_desc,
    immd.brnd_desc,
    immd.vrnt_desc,
    immd.putup_desc,
    immd.item_desc2,
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
    veocurd.from_ccy,
    veocurd.to_ccy,
    veocurd.exch_rate,
    NULL AS wh_id,
    NULL AS doc_type,
    NULL AS doc_type_desc,
    NULL AS bill_date,
    NULL AS bill_doc,
    0 AS base_sls,
    0 AS sls_qty,
    0 AS ret_qty,
    NULL AS uom,
    0 AS sls_qty_pc,
    0 AS ret_qty_pc,
    veoint.bill_qty_pc AS in_transit_qty,
    0 AS mat_list_price,
    0 AS grs_trd_sls,
    0 AS ret_val,
    0 AS trd_discnt,
    0 AS trd_sls,
    0 AS net_trd_sls,
    0 AS jj_grs_trd_sls,
    0 AS jj_ret_val,
    0 AS jj_trd_sls,
    0 AS jj_net_trd_sls,
    (veoint.billing_gross_val * veocurd.exch_rate) AS in_transit_val,
    0 AS trgt_val,
    NULL AS inv_dt,
    NULL AS warehse_cd,
    0 AS end_stock_qty,
    0 AS end_stock_val_raw,
    0 AS end_stock_val,
    NULL AS is_npi,
    NULL AS npi_str_period,
    NULL AS npi_end_period,
    NULL AS is_reg,
    NULL AS is_promo,
    NULL AS promo_strt_period,
    NULL AS promo_end_period,
    NULL AS is_mcl,
    NULL AS is_hero,
    0 AS contribution
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
        (
            (
                (
                    (
                        (
                            SELECT a.bill_doc,
                                b.bill_num,
                                b.sold_to,
                                b.matl_num,
                                b.bill_qty_pc,
                                b.billing_gross_val,
                                veotd."year",
                                veotd.qrtr_no,
                                veotd.mnth_id,
                                veotd.mnth_no,
                                veotd.mnth_nm
                            FROM itg_my_in_transit a,
                                (
                                    SELECT EDW_VW_MY_BILLING_FACT.bill_num,
                                        EDW_VW_MY_BILLING_FACT.sold_to,
                                        EDW_VW_MY_BILLING_FACT.matl_num,
                                        sum(EDW_VW_MY_BILLING_FACT.bill_qty_pc) AS bill_qty_pc,
                                        sum(
                                            (
                                                EDW_VW_MY_BILLING_FACT.grs_trd_sls * abs(EDW_VW_MY_BILLING_FACT.exchg_rate)
                                            )
                                        ) AS billing_gross_val
                                    FROM EDW_VW_MY_BILLING_FACT
                                    WHERE (
                                            EDW_VW_MY_BILLING_FACT.cntry_key = ('MY'::character varying)::text
                                        )
                                    GROUP BY EDW_VW_MY_BILLING_FACT.bill_num,
                                        EDW_VW_MY_BILLING_FACT.sold_to,
                                        EDW_VW_MY_BILLING_FACT.matl_num
                                ) b,
                                (
                                    SELECT edw_vw_os_time_dim.cal_year AS "year",
                                        edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                                        edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                                        edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                                        edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                                        edw_vw_os_time_dim.cal_date
                                    FROM edw_vw_os_time_dim
                                    GROUP BY edw_vw_os_time_dim.cal_year,
                                        edw_vw_os_time_dim.cal_qrtr_no,
                                        edw_vw_os_time_dim.cal_mnth_id,
                                        edw_vw_os_time_dim.cal_mnth_no,
                                        edw_vw_os_time_dim.cal_mnth_nm,
                                        edw_vw_os_time_dim.cal_date
                                ) veotd
                            WHERE (
                                    (b.bill_num = (a.bill_doc)::text)
                                    AND (a.closing_dt = veotd.cal_date)
                                )
                        ) veoint
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
                                ) = ltrim(veoint.sold_to, ('0'::character varying)::text)
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
                            ) = ltrim(veoint.matl_num, ('0'::character varying)::text)
                        )
                    )
                )
                LEFT JOIN itg_my_dstrbtrr_dim imdd ON (
                    (
                        ltrim(
                            (imdd.cust_id)::text,
                            ('0'::character varying)::text
                        ) = ltrim(veoint.sold_to, ('0'::character varying)::text)
                    )
                )
            )
            LEFT JOIN itg_my_material_dim immd ON (
                (
                    ltrim(
                        (immd.item_cd)::text,
                        ('0'::character varying)::text
                    ) = ltrim(veoint.matl_num, ('0'::character varying)::text)
                )
            )
        )
        LEFT JOIN itg_my_customer_dim imcd ON (
            (
                ltrim(
                    (imcd.cust_id)::text,
                    ('0'::character varying)::text
                ) = ltrim(veoint.sold_to, ('0'::character varying)::text)
            )
        )
    )
WHERE (
        (
            ((veoint.mnth_id)::character varying)::text >= veocurd.start_period
        )
        AND (
            ((veoint.mnth_id)::character varying)::text <= veocurd.end_period
        )
    )
),
final as (
    select * from gt_sellout
    union all
    select * from target
    union all
    select * from gt_inventory
    union all
    select * from in_transit
)
select * from final