with
edw_vn_dksh_stock as
(
    select * from {{ ref('vnmedw_integration__edw_vn_dksh_stock') }}
),
sdl_mds_vn_distributor_products as
(
    select * from {{ source('vnmsdl_raw', 'sdl_mds_vn_distributor_products') }}
),
wks_dksh_unmapped as
(
    select * from {{ source('vnmwks_integration','wks_dksh_unmapped') }}
),
edw_vw_os_time_dim as
(
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_vn_billing_fact as
(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_billing_fact') }}
),
edw_vn_dksh_stock as
(
    select * from {{ ref('vnmedw_integration__edw_vn_dksh_stock') }}
),
edw_vw_vn_material_dim as
(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_material_dim') }}
),
itg_query_parameters as
(
    select * from {{ source('aspitg_integration','itg_query_parameters') }}
),
edw_list_price as
(
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
itg_vn_mt_sellin_dksh as 
(
    select * from {{ ref('vnmitg_integration__itg_vn_mt_sellin_dksh') }}
),
itg_parameter_reg_inventory as 
(
    select * from {{ source('aspitg_integration','itg_parameter_reg_inventory') }}
),
edw_vw_vn_customer_dim as 
(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_customer_dim') }}
),
derived_table1 as
(
select 
    a.inv_dt,
    a.sap_sold_to_code,
    veotd."year",
    veotd.qrtr,
    veotd.mnth_id,
    veotd.mnth_no,
    a.material AS dstrbtr_matl_num,
    a.materialdescription,
    smdp.code,
    smdp.jnj_sap_code
FROM (
        (
            (
                SELECT DISTINCT edw_vn_dksh_stock.transaction_date AS inv_dt,
                    edw_vn_dksh_stock.sold_to_code AS sap_sold_to_code,
                    edw_vn_dksh_stock.material,
                    edw_vn_dksh_stock.materialdescription
                FROM edw_vn_dksh_stock
                WHERE (
                        edw_vn_dksh_stock.values_lc <> ((0)::numeric)::numeric(18, 0)
                    )
            ) a
            LEFT JOIN (
                SELECT (sdl_mds_vn_distributor_products.jnj_sap_code)::character varying AS jnj_sap_code,
                    sdl_mds_vn_distributor_products.code
                FROM sdl_mds_vn_distributor_products
                WHERE (
                        (
                            NOT (
                                sdl_mds_vn_distributor_products.code IN (
                                    SELECT DISTINCT wks_dksh_unmapped.product_id
                                    FROM wks_dksh_unmapped
                                )
                            )
                        )
                        AND (
                            sdl_mds_vn_distributor_products.jnj_sap_code IS NOT NULL
                        )
                    )
                UNION ALL
                SELECT DISTINCT wks_dksh_unmapped.jnj_sap_code,
                    wks_dksh_unmapped.product_id
                FROM wks_dksh_unmapped
            ) smdp ON (((a.material)::text = (smdp.code)::text))
        )
        LEFT JOIN (
            SELECT DISTINCT edw_vw_os_time_dim.cal_date,
                edw_vw_os_time_dim."year",
                edw_vw_os_time_dim.qrtr,
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.mnth_no
            FROM edw_vw_os_time_dim
        ) veotd ON ((veotd.cal_date = a.inv_dt))
    )
),
latest_inv as 
(
    SELECT DISTINCT derived_table1."year",
        derived_table1.qrtr,
        derived_table1.mnth_id,
        derived_table1.mnth_no,
        "max"(derived_table1.inv_dt) OVER(
            PARTITION BY derived_table1."year",
            derived_table1.mnth_id 
        ) AS dstrb_max_inv_date
    FROM  derived_table1
),
smdp as
(
SELECT (sdl_mds_vn_distributor_products.jnj_sap_code)::character varying AS jnj_sap_code,
    sdl_mds_vn_distributor_products.code
FROM sdl_mds_vn_distributor_products
WHERE (
        (
            NOT (
                sdl_mds_vn_distributor_products.code IN (
                    SELECT DISTINCT wks_dksh_unmapped.product_id
                    FROM wks_dksh_unmapped
                )
            )
        )
        AND (
            sdl_mds_vn_distributor_products.jnj_sap_code IS NOT NULL
        )
    )
UNION ALL
SELECT DISTINCT wks_dksh_unmapped.jnj_sap_code,
    wks_dksh_unmapped.product_id
FROM wks_dksh_unmapped
),
veotd as 
(
    SELECT DISTINCT edw_vw_os_time_dim."year",
        edw_vw_os_time_dim.qrtr,
        edw_vw_os_time_dim.mnth_id,
        edw_vw_os_time_dim.mnth_no,
        edw_vw_os_time_dim.cal_date
    FROM edw_vw_os_time_dim
),
cust as
(
    
SELECT DISTINCT edw_vw_vn_customer_dim.sap_cust_id,
    edw_vw_vn_customer_dim.sap_cust_nm,
    edw_vw_vn_customer_dim.sap_sls_org,
    edw_vw_vn_customer_dim.sap_prnt_cust_key,
    edw_vw_vn_customer_dim.sap_prnt_cust_desc
FROM edw_vw_vn_customer_dim
),
final as 
(
SELECT 
    cust.sap_prnt_cust_key,
    cust.sap_prnt_cust_desc,
    dksh.channel,
    dksh.jj_year,
    dksh.jj_qrtr,
    dksh.jj_mnth_id,
    dksh.jj_mnth_no,
    dksh.soldto_code,
    dksh.dstrbtr_matl_num,
    dksh.sku,
    dksh.sku_description,
    dksh.so_sls_qty_pc,
    dksh.so_grs_trd_sls,
    dksh.end_stock_qty,
    dksh.end_stock_val,
    dksh.si_sls_qty,
    dksh.si_gts_val
FROM (
        (
            (
                SELECT 'VN_DKSH'::character varying AS channel,
                    evvssf."year" AS jj_year,
                    (evvssf.qrtr)::character varying AS jj_qrtr,
                    (evvssf.mnth_id)::character varying AS jj_mnth_id,
                    evvssf.mnth_no AS jj_mnth_no,
                    (
                        ltrim(
                            (evvssf.sap_sold_to_code)::text,
                            ('0'::character varying)::text
                        )
                    )::character varying AS soldto_code,
                    evvssf.dstrbtr_matl_num,
                    (
                        ltrim(
                            (evvssf.sap_matl_num)::text,
                            ('0'::character varying)::text
                        )
                    )::character varying AS sku,
                    veomd.sap_mat_desc AS sku_description,
                    0 AS so_sls_qty_pc,
                    0 AS so_grs_trd_sls,
                    evvssf.end_stock_qty,
                    evvssf.end_stock_val,
                    0 AS si_sls_qty,
                    0 AS si_gts_val
                FROM (
                        (
                            SELECT a.cntry_cd,
                                a.cntry_nm,
                                a.sold_to_code AS sap_sold_to_code,
                                NULL::character varying AS dstrbtr_grp_cd,
                                a.material AS dstrbtr_matl_num,
                                smdp.jnj_sap_code AS sap_matl_num,
                                a.transaction_date AS inv_dt,
                                a.total AS end_stock_qty,
                                a.values_lc AS end_stock_val,
                                latest_inv."year",
                                latest_inv.qrtr,
                                latest_inv.mnth_no,
                                latest_inv.mnth_id,
                                latest_inv.dstrb_max_inv_date
                            FROM latest_inv,
                                (
                                    (
                                        SELECT edw_vn_dksh_stock.cntry_cd,
                                            edw_vn_dksh_stock.cntry_nm,
                                            edw_vn_dksh_stock.sold_to_code,
                                            edw_vn_dksh_stock.group_ds,
                                            edw_vn_dksh_stock.category,
                                            edw_vn_dksh_stock.material,
                                            edw_vn_dksh_stock.materialdescription,
                                            edw_vn_dksh_stock.syslot,
                                            edw_vn_dksh_stock.batchno,
                                            edw_vn_dksh_stock.exp_date,
                                            edw_vn_dksh_stock.total,
                                            edw_vn_dksh_stock.hcm,
                                            edw_vn_dksh_stock.vsip,
                                            edw_vn_dksh_stock.langha,
                                            edw_vn_dksh_stock.thanhtri,
                                            edw_vn_dksh_stock.danang,
                                            edw_vn_dksh_stock.values_lc,
                                            edw_vn_dksh_stock.reason,
                                            edw_vn_dksh_stock.transaction_date,
                                            edw_vn_dksh_stock.run_id,
                                            edw_vn_dksh_stock.file_name,
                                            edw_vn_dksh_stock.crtd_dttm,
                                            edw_vn_dksh_stock.updt_dttm
                                        FROM edw_vn_dksh_stock
                                        WHERE (
                                                edw_vn_dksh_stock.values_lc <> ((0)::numeric)::numeric(18, 0)
                                            )
                                    ) a
                                    LEFT JOIN  smdp ON (((a.material)::text = (smdp.code)::text))
                                )
                            WHERE (
                                    latest_inv.dstrb_max_inv_date = a.transaction_date
                                )
                        ) evvssf
                        LEFT JOIN edw_vw_vn_material_dim veomd ON (
                            (
                                ltrim(
                                    (evvssf.sap_matl_num)::text,
                                    ('0'::character varying)::text
                                ) = ltrim(
                                    (veomd.sap_matl_num)::text,
                                    ('0'::character varying)::text
                                )
                            )
                        )
                    )
                UNION ALL
                SELECT 'VN_DKSH'::character varying AS channel,
                    veotd."year" AS jj_year,
                    (veotd.qrtr)::character varying AS jj_qrtr,
                    (veotd.mnth_id)::character varying AS jj_mnth_id,
                    veotd.mnth_no AS jj_mnth_no,
                    (
                        ltrim(bill_ft.sold_to, ('0'::character varying)::text)
                    )::character varying AS soldto_code,
                    NULL::character varying AS dstrbtr_matl_num,
                    (
                        ltrim(bill_ft.matl_num, ('0'::character varying)::text)
                    )::character varying AS sku,
                    veomd.sap_mat_desc AS sku_description,
                    0 AS so_sls_qty_pc,
                    0 AS so_grs_trd_sls,
                    0 AS end_stock_qty,
                    0 AS end_stock_val,
                    bill_ft.bill_qty_pc AS si_sls_qty,
                    bill_ft.net_amt AS si_gts_val
                FROM veotd,
                    (
                        (
                            SELECT DISTINCT edw_vw_vn_billing_fact.cntry_key,
                                edw_vw_vn_billing_fact.cntry_nm,
                                edw_vw_vn_billing_fact.bill_dt,
                                edw_vw_vn_billing_fact.bill_num,
                                edw_vw_vn_billing_fact.bill_item,
                                edw_vw_vn_billing_fact.bill_type,
                                edw_vw_vn_billing_fact.sls_doc_num,
                                edw_vw_vn_billing_fact.sls_doc_item,
                                edw_vw_vn_billing_fact.doc_curr,
                                edw_vw_vn_billing_fact.sd_doc_catgy,
                                edw_vw_vn_billing_fact.sold_to,
                                edw_vw_vn_billing_fact.ship_to,
                                edw_vw_vn_billing_fact.matl_num,
                                edw_vw_vn_billing_fact.sls_org,
                                edw_vw_vn_billing_fact.exchg_rate,
                                edw_vw_vn_billing_fact.bill_qty_pc,
                                edw_vw_vn_billing_fact.grs_trd_sls,
                                edw_vw_vn_billing_fact.net_amt,
                                edw_vw_vn_billing_fact.est_nts,
                                edw_vw_vn_billing_fact.net_val,
                                edw_vw_vn_billing_fact.gross_val
                            FROM edw_vw_vn_billing_fact
                            WHERE (
                                    edw_vw_vn_billing_fact.sold_to = (
                                        (
                                            SELECT DISTINCT edw_vn_dksh_stock.sold_to_code
                                            FROM edw_vn_dksh_stock
                                        )
                                    )::text
                                )
                        ) bill_ft
                        LEFT JOIN edw_vw_vn_material_dim veomd ON (
                            (
                                ltrim(
                                    (veomd.sap_matl_num)::text,
                                    ('0'::character varying)::text
                                ) = ltrim(bill_ft.matl_num, ('0'::character varying)::text)
                            )
                        )
                    )
                WHERE (
                        (
                            (
                                (
                                    bill_ft.bill_qty_pc <> (((0)::numeric)::numeric(18, 0))::numeric(38, 4)
                                )
                                AND (
                                    (bill_ft.cntry_key)::text = ('VN'::character varying)::text
                                )
                            )
                            AND (bill_ft.bill_dt = veotd.cal_date)
                        )
                        AND (
                            (bill_ft.bill_type)::text = ('ZF2V'::character varying)::text
                        )
                    )
            )
            UNION ALL
            SELECT 'VN_DKSH'::character varying AS channel,
                veotd."year" AS jj_year,
                (veotd.qrtr)::character varying AS jj_qrtr,
                (veotd.mnth_id)::character varying AS jj_mnth_id,
                veotd.mnth_no AS jj_mnth_no,
                (
                    ltrim(
                        (evvssf.sap_sold_to_code)::text,
                        ('0'::character varying)::text
                    )
                )::character varying AS soldto_code,
                NULL::character varying AS dstrbtr_matl_num,
                (
                    ltrim(
                        (evvssf.matl_id)::text,
                        ('0'::character varying)::text
                    )
                )::character varying AS sku,
                veomd.sap_mat_desc AS sku_description,
                evvssf.sellout_qty AS so_sls_qty_pc,
                (
                    evvssf.sellout_value * ((100)::numeric)::numeric(18, 0)
                ) AS so_grs_trd_sls,
                0 AS end_stock_qty,
                0 AS end_stock_val,
                0 AS si_sls_qty,
                0 AS si_gts_val
            FROM (
                    SELECT DISTINCT edw_vw_os_time_dim."year",
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.mnth_no,
                        edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim
                ) veotd,
                (
                    (
                        SELECT qp.sap_sold_to_code,
                            smdp.jnj_sap_code AS matl_id,
                            a.region,
                            a.zone,
                            try_to_date(
                                (
                                    (
                                        (
                                            (
                                                "substring"((a.invoice_date)::text, 0, 5) || ('-'::character varying)::text
                                            ) || "substring"((a.invoice_date)::text, 5, 2)
                                        ) || ('-'::character varying)::text
                                    ) || "substring"((a.invoice_date)::text, 7, 2)
                                ),
                                ('YYYY-MM-DD'::character varying)::text
                            ) AS bill_date,
                            a.qty_exclude_foc AS sellout_qty,
                            (
                                ((a.qty_exclude_foc)::numeric)::numeric(18, 0) * lp.amount
                            ) AS sellout_value
                        FROM (
                                SELECT itg_query_parameters.parameter_value AS sap_sold_to_code
                                FROM itg_query_parameters
                                WHERE (
                                        (
                                            (itg_query_parameters.country_code)::text = ('VN'::character varying)::text
                                        )
                                        AND (
                                            (itg_query_parameters.parameter_name)::text = ('vn_dksh_soldto_code'::character varying)::text
                                        )
                                    )
                            ) qp,
                            (
                                itg_vn_mt_sellin_dksh a
                                LEFT JOIN (
                                    (
                                        SELECT (sdl_mds_vn_distributor_products.jnj_sap_code)::character varying AS jnj_sap_code,
                                            sdl_mds_vn_distributor_products.code
                                        FROM sdl_mds_vn_distributor_products
                                        WHERE (
                                                (
                                                    NOT (
                                                        sdl_mds_vn_distributor_products.code IN (
                                                            SELECT DISTINCT wks_dksh_unmapped.product_id
                                                            FROM wks_dksh_unmapped
                                                        )
                                                    )
                                                )
                                                AND (
                                                    sdl_mds_vn_distributor_products.jnj_sap_code IS NOT NULL
                                                )
                                            )
                                        UNION ALL
                                        SELECT DISTINCT wks_dksh_unmapped.jnj_sap_code,
                                            wks_dksh_unmapped.product_id
                                        FROM wks_dksh_unmapped
                                    ) smdp
                                    LEFT JOIN (
                                        SELECT lp.material,
                                            lp.list_price,
                                            b.parameter_value,
                                            (
                                                (
                                                    (
                                                        (lp.list_price)::double precision * (b.parameter_value)::double precision
                                                    )
                                                )::numeric(18, 0)
                                            )::numeric(10, 4) AS amount
                                        FROM (
                                                SELECT ltrim(
                                                        (edw_list_price.material)::text,
                                                        ((0)::character varying)::text
                                                    ) AS material,
                                                    edw_list_price.amount AS list_price,
                                                    row_number() OVER(
                                                        PARTITION BY ltrim(
                                                            (edw_list_price.material)::text,
                                                            ((0)::character varying)::text
                                                        )
                                                        ORDER BY to_date(
                                                                (edw_list_price.valid_to)::text,
                                                                ('YYYYMMDD'::character varying)::text
                                                            ) DESC,
                                                            to_date(
                                                                (edw_list_price.dt_from)::text,
                                                                ('YYYYMMDD'::character varying)::text
                                                            ) DESC
                                                    ) AS rn
                                                FROM edw_list_price
                                                WHERE (
                                                        (edw_list_price.sls_org)::text = ('260S'::character varying)::text
                                                    )
                                            ) lp,
                                            itg_parameter_reg_inventory b
                                        WHERE (
                                                (
                                                    (lp.rn = 1)
                                                    AND (
                                                        (b.country_name)::text = ('VT'::character varying)::text
                                                    )
                                                )
                                                AND (
                                                    (b.parameter_name)::text = ('VT_DKSH'::character varying)::text
                                                )
                                            )
                                    ) lp ON (((smdp.jnj_sap_code)::text = lp.material))
                                ) ON (
                                    (
                                        ltrim(
                                            (a.productid)::text,
                                            ((0)::character varying)::text
                                        ) = ltrim(
                                            (smdp.code)::text,
                                            ((0)::character varying)::text
                                        )
                                    )
                                )
                            )
                    ) evvssf
                    LEFT JOIN edw_vw_vn_material_dim veomd ON (
                        (
                            ltrim(
                                (veomd.sap_matl_num)::text,
                                ('0'::character varying)::text
                            ) = ltrim(
                                (evvssf.matl_id)::text,
                                ('0'::character varying)::text
                            )
                        )
                    )
                )
            WHERE (
                    veotd.cal_date = evvssf.bill_date
                )
        ) dksh
        LEFT JOIN cust ON (
            (
                (dksh.soldto_code)::text = ltrim(
                    (cust.sap_cust_id)::text,
                    ((0)::character varying)::text
                )
            )
        )
    )
)
select * from final