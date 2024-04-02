with edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_vn_dms_d_sellout_sales_fact as (
    select * from {{ ref('vnmitg_integration__itg_vn_dms_d_sellout_sales_fact') }}
),
itg_vn_dms_product_dim as (
    select * from {{ ref('vnmitg_integration__itg_vn_dms_product_dim') }}
),
itg_vn_dms_distributor_dim as (
    select * from {{ ref('vnmitg_integration__itg_vn_dms_distributor_dim') }}
),
itg_vn_distributor_sap_sold_to_mapping as (
    select * from {{ source('vnmitg_integration','itg_vn_distributor_sap_sold_to_mapping') }}
),
itg_mds_vn_gt_gts_ratio as (
    select * from {{ ref('vnmitg_integration__itg_mds_vn_gt_gts_ratio') }}
),
edw_vw_vn_customer_dim as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_customer_dim') }}
),
edw_vw_vn_sellout_inventory_fact as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_sellout_inventory_fact') }}
),
edw_vw_vn_billing_fact as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_billing_fact') }}
),
cust_dim as (
    SELECT DISTINCT edw_vw_vn_customer_dim.sap_cust_id,
            edw_vw_vn_customer_dim.sap_cust_nm,
            edw_vw_vn_customer_dim.sap_sls_org,
            edw_vw_vn_customer_dim.sap_cmp_id,
            edw_vw_vn_customer_dim.sap_cntry_cd,
            edw_vw_vn_customer_dim.sap_cntry_nm,
            edw_vw_vn_customer_dim.sap_addr,
            edw_vw_vn_customer_dim.sap_region,
            edw_vw_vn_customer_dim.sap_state_cd,
            edw_vw_vn_customer_dim.sap_city,
            edw_vw_vn_customer_dim.sap_post_cd,
            edw_vw_vn_customer_dim.sap_chnl_cd,
            edw_vw_vn_customer_dim.sap_chnl_desc,
            edw_vw_vn_customer_dim.sap_sls_office_cd,
            edw_vw_vn_customer_dim.sap_sls_office_desc,
            edw_vw_vn_customer_dim.sap_sls_grp_cd,
            edw_vw_vn_customer_dim.sap_sls_grp_desc,
            edw_vw_vn_customer_dim.sap_curr_cd,
            edw_vw_vn_customer_dim.sap_prnt_cust_key,
            edw_vw_vn_customer_dim.sap_prnt_cust_desc,
            edw_vw_vn_customer_dim.sap_cust_chnl_key,
            edw_vw_vn_customer_dim.sap_cust_chnl_desc,
            edw_vw_vn_customer_dim.sap_cust_sub_chnl_key,
            edw_vw_vn_customer_dim.sap_sub_chnl_desc,
            edw_vw_vn_customer_dim.sap_go_to_mdl_key,
            edw_vw_vn_customer_dim.sap_go_to_mdl_desc,
            edw_vw_vn_customer_dim.sap_bnr_key,
            edw_vw_vn_customer_dim.sap_bnr_desc,
            edw_vw_vn_customer_dim.sap_bnr_frmt_key,
            edw_vw_vn_customer_dim.sap_bnr_frmt_desc,
            edw_vw_vn_customer_dim.retail_env,
            edw_vw_vn_customer_dim.gch_region,
            edw_vw_vn_customer_dim.gch_cluster,
            edw_vw_vn_customer_dim.gch_subcluster,
            edw_vw_vn_customer_dim.gch_market,
            edw_vw_vn_customer_dim.gch_retail_banner
        FROM edw_vw_vn_customer_dim
),
time_dim as (
    SELECT DISTINCT edw_vw_os_time_dim."year",
            edw_vw_os_time_dim.qrtr,
            edw_vw_os_time_dim.mnth_id,
            edw_vw_os_time_dim.mnth_no,
            edw_vw_os_time_dim.wk,
            edw_vw_os_time_dim.mnth_wk_no,
            edw_vw_os_time_dim.cal_date,
            edw_vw_os_time_dim.cal_date_id
        FROM edw_vw_os_time_dim
),
sales_fact as (
    SELECT veotd."year" AS jj_year,
            veotd.qrtr::character varying AS jj_qrtr,
            veotd.mnth_id::character varying AS jj_mnth_id,
            veotd.mnth_no AS jj_mnth_no,
            evvssf.cntry_cd,
            ltrim(
                evvssf.soldto_code::text,
                '0'::character varying::text
            )::character varying AS soldto_code,
            evvcd.sap_prnt_cust_key AS sap_prnt_cust_key,
            evvcd.sap_prnt_cust_desc AS sap_prnt_cust_desc,
            evvssf.dstrbtr_matl_num,
            ltrim(
                evvssf.sap_matl_num::text,
                '0'::character varying::text
            )::character varying AS sku,
            evvssf.sls_qty_pc AS so_sls_qty_pc,
            evvssf.ret_qty_pc AS so_ret_qty_pc,
            CASE
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'DUONG ANH'::character varying::text THEN evvssf.grs_trd_sls / 1.1 * ra.percentage / 100::numeric::numeric(18, 0)
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'TIEN THANH'::character varying::text THEN evvssf.grs_trd_sls / 1.1 * ra.percentage / 100::numeric::numeric(18, 0)
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'DIETHALM'::character varying::text THEN evvssf.grs_trd_sls / 1.1 * ra.percentage / 100::numeric::numeric(18, 0)
                ELSE NULL::numeric::numeric(18, 0)
            END AS so_grs_trd_sls,
            CASE
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'DUONG ANH'::character varying::text THEN evvssf.ret_val / 1.1 * ra.percentage / 100::numeric::numeric(18, 0)
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'TIEN THANH'::character varying::text THEN evvssf.ret_val / 1.1 * ra.percentage / 100::numeric::numeric(18, 0)
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'DIETHALM'::character varying::text THEN evvssf.ret_val / 1.1 * ra.percentage / 100::numeric::numeric(18, 0)
                ELSE NULL::numeric::numeric(18, 0)
            END AS so_ret_val,
            CASE
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'DUONG ANH'::character varying::text THEN evvssf.jj_net_trd_sls * ra.percentage / 100::numeric::numeric(18, 0)
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'TIEN THANH'::character varying::text THEN evvssf.jj_net_trd_sls * ra.percentage / 100::numeric::numeric(18, 0)
                WHEN upper(evvcd.sap_prnt_cust_desc::text) = 'DIETHALM'::character varying::text THEN evvssf.jj_net_trd_sls * ra.percentage / 100::numeric::numeric(18, 0)
                ELSE NULL::numeric::numeric(18, 0)
            END AS jj_so_grs_trd_sls,
            0 AS end_stock_qty,
            0 AS end_stock_val,
            0 AS si_sls_qty,
            0 AS si_gts_val,
            0 AS si_nts_val
        FROM time_dim veotd,
            (
                SELECT sellout.cntry_code AS cntry_cd,
                    'Vietnam' AS cntry_nm,
                    sellout.dstrbtr_id AS dstrbtr_grp_cd,
                    distributor.mapped_spk,
                    sst_map.sap_sold_to_code AS soldto_code,
                    sellout.outlet_id AS cust_cd,
                    sellout.material_code AS dstrbtr_matl_num,
                    product_dim.productcodesap AS sap_matl_num,
                    NULL AS bar_cd,
                    sellout.invoice_date AS bill_date,
                    sellout.invoice_no AS bill_doc,
                    sellout.salesrep_id AS slsmn_cd,
                    sellout.salesrep_name AS slsmn_nm,
                    NULL AS doc_type,
                    NULL AS doc_type_desc,
                    0 AS base_sls,
                    sellout.quantity,
                    CASE
                        WHEN sellout.total_sellout_afvat_bfdisc >= 0::numeric::numeric(18, 0)::numeric(15, 4) THEN sellout.quantity
                        ELSE 0::numeric::numeric(18, 0)
                    END AS sls_qty,
                    CASE
                        WHEN sellout.total_sellout_afvat_bfdisc < 0::numeric::numeric(18, 0)::numeric(15, 4) THEN sellout.quantity
                        ELSE 0::numeric::numeric(18, 0)
                    END AS ret_qty,
                    sellout.uom,
                    CASE
                        WHEN sellout.total_sellout_afvat_bfdisc >= 0::numeric::numeric(18, 0)::numeric(15, 4) THEN sellout.quantity
                        ELSE 0::numeric::numeric(18, 0)
                    END AS sls_qty_pc,
                    CASE
                        WHEN sellout.total_sellout_afvat_bfdisc < 0::numeric::numeric(18, 0)::numeric(15, 4) THEN sellout.quantity
                        ELSE 0::numeric::numeric(18, 0)
                    END AS ret_qty_pc,
                    CASE
                        WHEN sellout.total_sellout_afvat_bfdisc >= 0::numeric::numeric(18, 0)::numeric(15, 4) THEN sellout.total_sellout_afvat_bfdisc
                        ELSE 0::numeric::numeric(18, 0)
                    END AS grs_trd_sls,
                    CASE
                        WHEN sellout.total_sellout_afvat_bfdisc < 0::numeric::numeric(18, 0)::numeric(15, 4) THEN sellout.total_sellout_afvat_bfdisc
                        ELSE 0::numeric::numeric(18, 0)
                    END AS ret_val,
                    sellout.discount AS trd_discnt,
                    NULL::integer AS trd_discnt_item_lvl,
                    NULL::integer AS trd_discnt_bill_lvl,
                    NULL::integer AS trd_sls,
                    sellout.total_sellout_afvat_afdisc AS net_trd_sls,
                    NULL AS cn_reason_cd,
                    NULL AS cn_reason_desc,
                    CASE
                        WHEN sellout.total_sellout_afvat_bfdisc >= 0::numeric::numeric(18, 0)::numeric(15, 4) THEN sellout.total_sellout_afvat_bfdisc
                        ELSE 0::numeric::numeric(18, 0)
                    END AS jj_grs_trd_sls,
                    CASE
                        WHEN sellout.total_sellout_afvat_bfdisc < 0::numeric::numeric(18, 0)::numeric(15, 4) THEN sellout.total_sellout_afvat_bfdisc
                        ELSE 0::numeric::numeric(18, 0)
                    END AS jj_ret_val,
                    NULL::integer AS jj_trd_sls,
                    sellout.total_sellout_afvat_afdisc AS jj_net_trd_sls,
                    trim(
                        product_dim.tax_rate::text,
                        'VAT'::character varying::text
                    )::integer AS tax_rate,
                    sellout.total_sellout_afvat_afdisc / (
                        (
                            100::numeric::numeric(18, 0) + trim(
                                product_dim.tax_rate::text,
                                'VAT'::character varying::text
                            )::integer::numeric::numeric(18, 0)
                        ) / 100::numeric::numeric(18, 0)
                    ) AS jj_net_sls
                FROM itg_vn_dms_d_sellout_sales_fact sellout,
                    itg_vn_dms_product_dim product_dim,
                    (
                        SELECT derived_table1.dstrbtr_id,
                            derived_table1.mapped_spk
                        FROM (
                                SELECT itg_vn_dms_distributor_dim.dstrbtr_id,
                                    itg_vn_dms_distributor_dim.mapped_spk,
                                    row_number() OVER(
                                        PARTITION BY itg_vn_dms_distributor_dim.dstrbtr_id
                                        ORDER BY itg_vn_dms_distributor_dim.crtd_dttm DESC
                                    ) AS rn
                                FROM itg_vn_dms_distributor_dim
                            ) derived_table1
                        WHERE derived_table1.rn = 1
                    ) distributor
                    LEFT JOIN itg_vn_distributor_sap_sold_to_mapping sst_map ON distributor.mapped_spk::text = sst_map.distributor_id::text
                WHERE sellout.material_code::text = product_dim.product_code::text
                    AND sellout.dstrbtr_id::text = distributor.dstrbtr_id::text
                    AND sellout.status::text <> 'V'::character varying::text
            ) evvssf
            LEFT JOIN (
            cust_dim  evvcd
                LEFT JOIN itg_mds_vn_gt_gts_ratio ra ON upper(
                    COALESCE(ra.distributor, '#'::character varying)::text
                ) = upper(
                    COALESCE(evvcd.sap_prnt_cust_desc, '#'::character varying)::text
                )
            ) ON ltrim(
                evvcd.sap_cust_id::text,
                '0'::character varying::text
            ) = ltrim(
                evvssf.soldto_code::text,
                '0'::character varying::text
            )
        WHERE to_date(veotd.cal_date::timestamp_ntz(9)) = to_date(evvssf.bill_date::timestamp_ntz(9))
            AND veotd.mnth_id >= ra.from_month::text
            AND veotd.mnth_id <= ra.to_month::text
            AND evvssf.net_trd_sls <> 0::numeric::numeric(18, 0)::numeric(15, 4)
),
inv_fact as (
    SELECT veotd."year" AS jj_year,
            veotd.qrtr::character varying AS jj_qrtr,
            veotd.mnth_id::character varying AS jj_mnth_id,
            veotd.mnth_no AS jj_mnth_no,
            evvssf.cntry_cd,
            ltrim(
                soldto_map.sap_sold_to_code::text,
                '0'::character varying::text
            )::character varying AS soldto_code,
            evvcd.sap_prnt_cust_key AS sap_prnt_cust_key,
            evvcd.sap_prnt_cust_desc AS sap_prnt_cust_desc,
            evvssf.dstrbtr_matl_num,
            ltrim(
                evvssf.sap_matl_num::text,
                '0'::character varying::text
            )::character varying AS sku,
            0 AS so_sls_qty_pc,
            0 AS so_ret_qty_pc,
            0 AS so_grs_trd_sls,
            0 AS so_ret_val,
            0 AS jj_so_grs_trd_sls,
            evvssf.end_stock_qty,
            evvssf.end_stock_val,
            0 AS si_sls_qty,
            0 AS si_gts_val,
            0 AS si_nts_val
        FROM (
                SELECT a.cntry_cd,
                    a.cntry_nm,
                    a.warehse_cd,
                    a.dstrbtr_grp_cd,
                    a.dstrbtr_soldto_code,
                    a.dstrbtr_matl_num,
                    a.sap_matl_num,
                    a.bar_cd,
                    a.inv_dt,
                    a.soh,
                    a.soh_val,
                    a.jj_soh_val,
                    a.beg_stock_qty,
                    a.end_stock_qty,
                    a.beg_stock_val,
                    a.end_stock_val,
                    a.jj_beg_stock_qty,
                    a.jj_end_stock_qty,
                    a.jj_beg_stock_val,
                    a.jj_end_stock_val,
                    latest_inv.dstrbtr_id,
                    latest_inv.wh_code,
                    latest_inv.mnth_id,
                    latest_inv.dstrb_max_inv_date
                FROM edw_vw_vn_sellout_inventory_fact a,
                    (
                        SELECT DISTINCT derived_table1.dstrbtr_id,
                            derived_table1.wh_code,
                            derived_table1.mnth_id,
                            "max"(derived_table1.inv_dt) OVER(
                                PARTITION BY derived_table1.mnth_id,
                                derived_table1.dstrbtr_id,
                                derived_table1.wh_code order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                            ) AS dstrb_max_inv_date
                        FROM (
                                SELECT a.dstrbtr_id,
                                    a.inv_dt,
                                    a.wh_code,
                                    timedim1.mnth_id
                                FROM (
                                        SELECT DISTINCT edw_vw_vn_sellout_inventory_fact.dstrbtr_grp_cd AS dstrbtr_id,
                                            edw_vw_vn_sellout_inventory_fact.warehse_cd AS wh_code,
                                            edw_vw_vn_sellout_inventory_fact.inv_dt
                                        FROM edw_vw_vn_sellout_inventory_fact
                                    ) a,
                                    edw_vw_os_time_dim timedim1
                                WHERE a.inv_dt = timedim1.cal_date
                            ) derived_table1
                    ) latest_inv
                WHERE latest_inv.dstrbtr_id::text = a.dstrbtr_grp_cd::text
                    AND latest_inv.wh_code::text = a.warehse_cd::text
                    AND latest_inv.dstrb_max_inv_date = a.inv_dt
            ) evvssf,
            time_dim veotd,
            (
                SELECT DISTINCT dist.dstrbtr_id,
                    COALESCE(dist.mapped_spk, dist.dstrbtr_id) AS mapped_spk,
                    mapp.sap_sold_to_code,
                    mapp.sap_ship_to_code
                FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                    itg_vn_dms_distributor_dim dist
                WHERE mapp.distributor_id::text = COALESCE(dist.mapped_spk, dist.dstrbtr_id)::text
                    AND (
                        upper(dist.dstrbtr_type::text) = 'SPK'::character varying::text
                        OR upper(dist.dstrbtr_type::text) = 'SP'::character varying::text
                    )
            ) soldto_map
            LEFT JOIN cust_dim evvcd ON ltrim(
                evvcd.sap_cust_id::text,
                '0'::character varying::text
            ) = ltrim(
                soldto_map.sap_sold_to_code::text,
                '0'::character varying::text
            )
        WHERE to_date(veotd.cal_date::timestamp_ntz(9)) = to_date(evvssf.inv_dt::timestamp_ntz(9))
            AND evvssf.dstrbtr_grp_cd::text = soldto_map.dstrbtr_id::text
            AND evvssf.end_stock_val <> 0::numeric::numeric(18, 0)::numeric(15, 4)
),
billing_fact as (
    SELECT veotd."year" AS jj_year,
    veotd.qrtr::character varying AS jj_qrtr,
    veotd.mnth_id::character varying AS jj_mnth_id,
    veotd.mnth_no AS jj_mnth_no,
    bill_ft.cntry_key AS cntry_cd,
    ltrim(bill_ft.sold_to, '0'::character varying::text)::character varying AS soldto_code,
    evvcd.sap_prnt_cust_key AS sap_prnt_cust_key,
    evvcd.sap_prnt_cust_desc AS sap_prnt_cust_desc,
    NULL AS dstrbtr_matl_num,
    ltrim(bill_ft.matl_num, '0'::character varying::text)::character varying AS sku,
    0 AS so_sls_qty_pc,
    0 AS so_ret_qty_pc,
    0 AS so_grs_trd_sls,
    0 AS so_ret_val,
    0 AS jj_so_grs_trd_sls,
    0 AS end_stock_qty,
    0 AS end_stock_val,
    bill_ft.bill_qty_pc AS si_sls_qty,
    bill_ft.grs_trd_sls AS si_gts_val,
    bill_ft.net_amt AS si_nts_val
    FROM time_dim veotd,
        edw_vw_vn_billing_fact bill_ft
        LEFT JOIN cust_dim evvcd ON ltrim(
            evvcd.sap_cust_id::text,
            '0'::character varying::text
        ) = ltrim(bill_ft.sold_to, '0'::character varying::text)
    WHERE bill_ft.bill_qty_pc <> 0::numeric::numeric(18, 0)::numeric(38, 4)
        AND bill_ft.cntry_key::text = 'VN'::character varying::text
        AND bill_ft.bill_dt = veotd.cal_date
        AND (
            bill_ft.sold_to::character varying(30) IN (
                SELECT DISTINCT itg_vn_distributor_sap_sold_to_mapping.sap_sold_to_code
                FROM itg_vn_distributor_sap_sold_to_mapping
            )
        )
        AND bill_ft.bill_type::text = 'ZF2V'::character varying::text
),
final as (
    select * from sales_fact
    union all
    select * from inv_fact
    union all
    select * from billing_fact
)
select * from final