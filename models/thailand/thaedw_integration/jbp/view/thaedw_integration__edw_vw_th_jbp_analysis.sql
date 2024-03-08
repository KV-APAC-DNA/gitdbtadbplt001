with edw_th_sellout_analysis as 
(
    select * from {{ ref('thaedw_integration__edw_th_sellout_analysis') }}
),
itg_th_dtsmcl as
(
    select * from {{ ref("thaitg_integration__itg_th_dtsmcl") }}
),
itg_th_jbp_cop as
(
    select * from {{ ref('thaitg_integration__itg_th_jbp_cop') }}
),
edw_vw_th_sellout_sales_fact as
(
    select * from DEV_DNA_CORE.snaposeedw_integration.edw_vw_os_sellout_sales_fact
    where cntry_cd = 'TH'
),
edw_vw_th_sellout_inventory_fact as
(
    select * from DEV_DNA_CORE.snaposeedw_integration.edw_vw_os_sellout_inventory_fact
    where cntry_cd = 'TH'   
),
itg_th_dstrbtr_material_dim as
(
    select * from DEV_DNA_CORE.snaposeitg_integration.itg_th_dstrbtr_material_dim
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_th_material_dim as
(
    select * from DEV_DNA_CORE.snaposeedw_integration.edw_vw_os_material_dim
    where cntry_key = 'TH'
),
edw_vw_th_sellin_sales_fact as
(
    select * from DEV_DNA_CORE.snaposeedw_integration.edw_vw_os_sellin_sales_fact
    where cntry_nm = 'TH'
),
itg_th_ciw_account_lookup as
(
    select * from {{ ref('thaitg_integration__itg_th_ciw_account_lookup') }}
),
itg_th_jbp_rolling_forecast as
(
    select * from {{ ref('thaitg_integration__itg_th_jbp_rolling_forecast') }}
),
edw_vw_th_customer_dim as
(
    select * from DEV_DNA_CORE.snaposeedw_integration.edw_vw_os_customer_dim
    where sap_cntry_cd = 'TH'
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_vw_th_dstrbtr_customer_dim as
(
    select * from DEV_DNA_CORE.snaposeedw_integration.edw_vw_os_dstrbtr_customer_dim
    where cntry_cd = 'TH'
),
edw_vw_th_dstrbtr_material_dim as
(
    select * from DEV_DNA_CORE.snaposeedw_integration.edw_vw_os_dstrbtr_material_dim
    where cntry_cd = 'TH'
),
edw_gch_customerhierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
sellout as
(
    SELECT 
        'Sellout'::varchar AS data_type,
        edw_vw_th_sellout_analysis.year as "year",
        edw_vw_th_sellout_analysis.year_quarter,
        edw_vw_th_sellout_analysis.month_year,
        left(edw_vw_th_sellout_analysis.order_date,19)::varchar AS order_date,
        edw_vw_th_sellout_analysis.month_number,
        edw_vw_th_sellout_analysis.distributor_id,
        edw_vw_th_sellout_analysis.region_desc,
        edw_vw_th_sellout_analysis.city,
        NULL::varchar AS whcode,
        NULL::varchar AS whgroup,
        edw_vw_th_sellout_analysis.district,
        edw_vw_th_sellout_analysis.ar_code,
        edw_vw_th_sellout_analysis.ar_name,
        edw_vw_th_sellout_analysis.channel_code,
        edw_vw_th_sellout_analysis.channel,
        edw_vw_th_sellout_analysis.sales_office_code,
        edw_vw_th_sellout_analysis.sales_office_name,
        edw_vw_th_sellout_analysis.sales_group,
        edw_vw_th_sellout_analysis.cluster as "cluster",
        edw_vw_th_sellout_analysis.ar_type_code,
        edw_vw_th_sellout_analysis.ar_type_name,
        edw_vw_th_sellout_analysis.distributor_name,
        edw_vw_th_sellout_analysis.sap_cust_id,
        edw_vw_th_sellout_analysis.sku_code,
        edw_vw_th_sellout_analysis.sku_description,
        edw_vw_th_sellout_analysis.bar_code,
        mcl.barcd AS mcl_barcd,
        edw_vw_th_sellout_analysis.franchise,
        edw_vw_th_sellout_analysis.brand,
        edw_vw_th_sellout_analysis.variant,
        edw_vw_th_sellout_analysis.segment,
        edw_vw_th_sellout_analysis.put_up_description,
        edw_vw_th_sellout_analysis.salesman_name,
        edw_vw_th_sellout_analysis.salesman_code,
        edw_vw_th_sellout_analysis.cn_reason_code,
        edw_vw_th_sellout_analysis.cn_reason_description,
        sum(edw_vw_th_sellout_analysis.gross_trade_sales) AS gross_trade_sales,
        sum(edw_vw_th_sellout_analysis.cn_damaged_goods) AS cn_damaged_goods,
        sum(edw_vw_th_sellout_analysis.credit_note_amount) AS credit_note_amount,
        sum(edw_vw_th_sellout_analysis.line_discount) AS line_discount,
        sum(edw_vw_th_sellout_analysis.bottom_line_discount) AS bottom_line_discount,
        sum(edw_vw_th_sellout_analysis.sales_quantity) AS sales_quantity,
        sum(edw_vw_th_sellout_analysis.return_quantity) AS return_quantity,
        sum(edw_vw_th_sellout_analysis.quantity_dz) AS quantity_dz,
        sum(edw_vw_th_sellout_analysis.net_invoice) AS net_invoice,
        sum(edw_vw_th_sellout_analysis.target_calls) AS target_calls,
        avg(edw_vw_th_sellout_analysis.target_sales) AS target_sales,
        0 AS str_count,
        0 AS inventory_quantity,
        0 AS inventory,
        0 AS si_sellin_target,
        0 AS si_gross_trade_sales_value
    FROM (
            edw_th_sellout_analysis edw_vw_th_sellout_analysis
            LEFT JOIN (
                SELECT DISTINCT itg_th_dtsmcl.ar_typ_cd,
                    itg_th_dtsmcl.barcd
                FROM itg_th_dtsmcl
            ) mcl ON (
                (
                    (
                        (edw_vw_th_sellout_analysis.bar_code)::text = (mcl.barcd)::text
                    )
                    AND (
                        (edw_vw_th_sellout_analysis.ar_type_code)::text = (mcl.ar_typ_cd)::text
                    )
                )
            )
        )
    GROUP BY 1,
        edw_vw_th_sellout_analysis.year,
        edw_vw_th_sellout_analysis.year_quarter,
        edw_vw_th_sellout_analysis.month_year,
        edw_vw_th_sellout_analysis.order_date,
        edw_vw_th_sellout_analysis.month_number,
        edw_vw_th_sellout_analysis.distributor_id,
        edw_vw_th_sellout_analysis.region_desc,
        edw_vw_th_sellout_analysis.city,
        10,
        11,
        edw_vw_th_sellout_analysis.district,
        edw_vw_th_sellout_analysis.ar_code,
        edw_vw_th_sellout_analysis.ar_name,
        edw_vw_th_sellout_analysis.channel_code,
        edw_vw_th_sellout_analysis.channel,
        edw_vw_th_sellout_analysis.sales_office_code,
        edw_vw_th_sellout_analysis.sales_office_name,
        edw_vw_th_sellout_analysis.sales_group,
        edw_vw_th_sellout_analysis.cluster,
        edw_vw_th_sellout_analysis.ar_type_code,
        edw_vw_th_sellout_analysis.ar_type_name,
        edw_vw_th_sellout_analysis.distributor_name,
        edw_vw_th_sellout_analysis.sap_cust_id,
        edw_vw_th_sellout_analysis.sku_code,
        edw_vw_th_sellout_analysis.sku_description,
        edw_vw_th_sellout_analysis.bar_code,
        mcl.barcd,
        edw_vw_th_sellout_analysis.franchise,
        edw_vw_th_sellout_analysis.brand,
        edw_vw_th_sellout_analysis.variant,
        edw_vw_th_sellout_analysis.segment,
        edw_vw_th_sellout_analysis.put_up_description,
        edw_vw_th_sellout_analysis.salesman_name,
        edw_vw_th_sellout_analysis.salesman_code,
        edw_vw_th_sellout_analysis.cn_reason_code,
        edw_vw_th_sellout_analysis.cn_reason_description
),
cop as
(
    SELECT 
        'COP' AS data_type,
        itg_th_jbp_cop.year as "year",
        NULL AS year_quarter,
        (
            (
                ((itg_th_jbp_cop.year)::varchar)::text || ((itg_th_jbp_cop.month)::varchar)::text
            )
        )::varchar AS month_year,
        NULL::varchar AS order_date,
        itg_th_jbp_cop.month AS month_number,
        itg_th_jbp_cop.dist_id AS distributor_id,
        NULL AS region_desc,
        NULL AS city,
        NULL AS whcode,
        NULL AS whgroup,
        NULL AS district,
        NULL AS ar_code,
        NULL AS ar_name,
        NULL AS channel_code,
        NULL AS channel,
        NULL AS sales_office_code,
        NULL AS sales_office_name,
        NULL AS sales_group,
        NULL AS "cluster",
        NULL AS ar_type_code,
        NULL AS ar_type_name,
        NULL AS distributor_name,
        NULL AS sap_cust_id,
        NULL AS sku_code,
        NULL AS sku_description,
        NULL AS bar_code,
        NULL AS mcl_barcd,
        NULL AS franchise,
        NULL AS brand,
        NULL AS variant,
        NULL AS segment,
        NULL AS put_up_description,
        NULL AS salesman_name,
        NULL AS salesman_code,
        NULL AS cn_reason_code,
        NULL AS cn_reason_description,
        0 AS gross_trade_sales,
        0 AS cn_damaged_goods,
        0 AS credit_note_amount,
        0 AS line_discount,
        0 AS bottom_line_discount,
        0 AS sales_quantity,
        0 AS return_quantity,
        0 AS quantity_dz,
        0 AS net_invoice,
        0 AS target_calls,
        0 AS target_sales,
        itg_th_jbp_cop.str_count,
        0 AS inventory_quantity,
        0 AS inventory,
        0 AS si_sellin_target,
        0 AS si_gross_trade_sales_value
    FROM itg_th_jbp_cop
),
sales as
(
    SELECT 
        'Sales'::varchar AS typ,
        edw_vw_th_sellout_sales_fact.cntry_cd,
        edw_vw_th_sellout_sales_fact.cntry_nm,
        edw_vw_th_sellout_sales_fact.bill_date,
        edw_vw_th_sellout_sales_fact.dstrbtr_grp_cd,
        edw_vw_th_sellout_sales_fact.dstrbtr_matl_num,
        NULL::varchar AS warehse_cd,
        NULL::varchar AS warehse_grp,
        sum(edw_vw_th_sellout_sales_fact.sls_qty) AS sls_qty,
        sum(edw_vw_th_sellout_sales_fact.ret_qty) AS ret_qty,
        sum(
            CASE
                WHEN (
                    (
                        edw_vw_th_sellout_sales_fact.cn_reason_cd IS NULL
                    )
                    OR (
                        "left"(
                            (edw_vw_th_sellout_sales_fact.cn_reason_cd)::text,
                            1
                        ) <> ('N'::varchar)::text
                    )
                ) THEN (
                    (
                        edw_vw_th_sellout_sales_fact.grs_trd_sls + edw_vw_th_sellout_sales_fact.ret_val
                    )
                )::double precision
                ELSE NULL::double precision
            END
        ) AS grs_trd_sls,
        0 AS soh,
        0 AS amt_bfr_disc,
        0 AS amount_sls
    FROM edw_vw_th_sellout_sales_fact
    GROUP BY 
        edw_vw_th_sellout_sales_fact.cntry_cd,
        edw_vw_th_sellout_sales_fact.cntry_nm,
        edw_vw_th_sellout_sales_fact.bill_date,
        edw_vw_th_sellout_sales_fact.dstrbtr_grp_cd,
        edw_vw_th_sellout_sales_fact.dstrbtr_matl_num
),
inventory as
(
    SELECT 
        'Inventory'::varchar AS typ,
        'TH'::varchar AS cntry_cd,
        'Thailand'::varchar AS cntry_nm,
        inventory.inv_dt,
        inventory.dstrbtr_grp_cd,
        inventory.dstrbtr_matl_num,
        inventory.warehse_cd,
        CASE
            WHEN (
                "right"((inventory.warehse_cd)::text, 3) = ('700'::varchar)::text
            ) THEN 'Damage Goods'::varchar
            WHEN (
                (inventory.warehse_cd)::text = ('V902'::varchar)::text
            ) THEN 'Damage Goods'::varchar
            WHEN (
                (
                    "right"((inventory.warehse_cd)::text, 3) <> ('700'::varchar)::text
                )
                OR (
                    (inventory.warehse_cd)::text <> ('V902'::varchar)::text
                )
            ) THEN 'Normal Goods'::varchar
            ELSE NULL::varchar
        END AS warehse_grp,
        0 AS sls_qty,
        0 AS ret_qty,
        0 AS net_trd_sls,
        inventory.soh,
        (
            inventory.soh * (itg_material.sls_prc3)::double precision
        ) AS amt_bfr_disc,
        (
            (itg_material.sls_prc_credit)::double precision * inventory.soh
        ) AS amount_inv
    FROM (
            (
                SELECT DISTINCT inventory.warehse_cd,
                    inventory.dstrbtr_grp_cd,
                    inventory.dstrbtr_matl_num,
                    inventory.inv_dt,
                    (
                        (sum(inventory.soh))::double precision / (12)::double precision
                    ) AS soh
                FROM edw_vw_th_sellout_inventory_fact inventory
                WHERE (
                        (
                            (inventory.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        AND (
                            inventory.soh > (((0)::numeric)::numeric(18, 0))::numeric(22, 6)
                        )
                    )
                GROUP BY inventory.warehse_cd,
                    inventory.dstrbtr_grp_cd,
                    inventory.dstrbtr_matl_num,
                    inventory.inv_dt
            ) inventory
            LEFT JOIN (
                SELECT DISTINCT itg_th_dstrbtr_material_dim.item_cd,
                    itg_th_dstrbtr_material_dim.sls_prc3,
                    itg_th_dstrbtr_material_dim.sls_prc_credit
                FROM itg_th_dstrbtr_material_dim
            ) itg_material ON (
                (
                    (inventory.dstrbtr_matl_num)::text = (itg_material.item_cd)::text
                )
            )
        )
),
sales_inventory as
(
    SELECT 
        sales.typ AS data_type,
        (sales.bill_date)::varchar AS order_date,
        sales."year",
        (sales.qrtr)::varchar AS year_quarter,
        (sales.mnth_id)::varchar AS month_year,
        sales.mnth_no AS month_number,
        (sales.wk)::varchar AS year_week_number,
        (sales.mnth_wk_no)::varchar AS month_week_number,
        sales.cntry_cd AS country_code,
        sales.cntry_nm AS country_name,
        sales.dstrbtr_grp_cd AS distributor_id,
        sales.warehse_cd AS whcode,
        sales.warehse_grp AS whgroup,
        sales.dstrbtr_matl_num AS sku_code,
        matl.sap_mat_desc AS sku_description,
        matl.gph_prod_frnchse AS franchise,
        matl.gph_prod_brnd AS brand,
        matl.gph_prod_vrnt AS variant,
        matl.gph_prod_sgmnt AS segment,
        matl.gph_prod_put_up_desc AS put_up_description,
        sales.sls_qty AS sales_quantity,
        sales.ret_qty AS return_quantity,
        sales.grs_trd_sls AS gross_trade_sales,
        sales.soh AS inventory_quantity,
        sales.amt_bfr_disc AS amount_before_discount,
        sales.amount_sls AS inventory,
        0 AS si_gross_trade_sales_value,
        0 AS si_tp_value,
        0 AS si_net_trade_sales_value,
        0 AS si_sellin_target
    FROM
    (
            (
                SELECT sales.typ,
                    sales.cntry_cd,
                    sales.cntry_nm,
                    "time"."year",
                    "time".qrtr,
                    "time".mnth_id,
                    "time".mnth_no,
                    "time".wk,
                    "time".mnth_wk_no,
                    "time".cal_date,
                    sales.warehse_cd,
                    sales.warehse_grp,
                    sales.bill_date,
                    sales.dstrbtr_grp_cd,
                    sales.dstrbtr_matl_num,
                    sales.sls_qty,
                    sales.ret_qty,
                    sales.grs_trd_sls,
                    sales.soh,
                    sales.amt_bfr_disc,
                    sales.amount_sls
                FROM (
                        (
                            select * from sales
                            UNION ALL
                            select * from inventory
                        ) sales
                        JOIN (
                            SELECT DISTINCT edw_vw_os_time_dim."year",
                                edw_vw_os_time_dim.qrtr,
                                edw_vw_os_time_dim.mnth_id,
                                edw_vw_os_time_dim.mnth_no,
                                edw_vw_os_time_dim.wk,
                                edw_vw_os_time_dim.mnth_wk_no,
                                edw_vw_os_time_dim.cal_date
                            FROM edw_vw_os_time_dim 
                            WHERE (
                                    (
                                        edw_vw_os_time_dim."year" > date_part(year,(current_timestamp()::timestamp_ntz)) - 3
                                    )
                                    OR (
                                        edw_vw_os_time_dim."year" > date_part(year,(current_timestamp()::timestamp_ntz)) - 3
                                    )
                                )
                        ) "time" ON (
                            (
                                sales.bill_date = ("time".cal_date)::timestamp_ntz
                            )
                        )
                    )
            ) sales
            LEFT JOIN (
                SELECT DISTINCT edw_vw_th_material_dim.cntry_key,
                    edw_vw_th_material_dim.sap_matl_num,
                    edw_vw_th_material_dim.sap_mat_desc,
                    edw_vw_th_material_dim.gph_region,
                    edw_vw_th_material_dim.gph_prod_frnchse,
                    edw_vw_th_material_dim.gph_prod_brnd,
                    edw_vw_th_material_dim.gph_prod_vrnt,
                    edw_vw_th_material_dim.gph_prod_sgmnt,
                    edw_vw_th_material_dim.gph_prod_put_up_desc
                FROM edw_vw_th_material_dim
            ) matl ON (
                (
                    upper((sales.dstrbtr_matl_num)::text) = upper(
                        ltrim(
                            (matl.sap_matl_num)::text,
                            ('0'::varchar)::text
                        )
                    )
                )
            )
        )
),
sellin as
(
    SELECT 
        'Sellin'::varchar AS data_type,
        NULL::varchar AS order_date,
        sellin.year_jnj AS "year",
        (sellin.year_quarter_jnj)::varchar AS year_quarter,
        (sellin.year_month_jnj)::varchar AS month_year,
        sellin.month_number_jnj AS month_number,
        NULL::varchar AS year_week_number,
        NULL::varchar AS month_week_number,
        'TH'::varchar AS country_code,
        'Thailand'::varchar AS country_name,
        sellin.dstrbtr_grp_cd AS distributor_id,
        NULL::varchar AS whcode,
        NULL::varchar AS whgroup,
        sellin.item_code AS sku_code,
        sellin.item_description AS sku_description,
        sellin.franchise,
        sellin.brand,
        sellin.variant,
        sellin.segment,
        sellin.put_up AS put_up_description,
        0 AS sales_quantity,
        0 AS return_quantity,
        0 AS gross_trade_sales,
        0 AS inventory_quantity,
        0 AS amount_before_discount,
        0 AS inventory,
        sum(sellin.gross_trade_sales_value) AS si_gross_trade_sales_value,
        sum(sellin.tp_value) AS si_tp_value,
        sum(sellin.net_trade_sales_value) AS si_net_trade_sales_value,
        "max"(sellin.sellin_target) AS sellin_target
    FROM (
            SELECT 
                "time"."year" AS year_jnj,
                "time".qrtr AS year_quarter_jnj,
                "time".mnth_id AS year_month_jnj,
                "time".mnth_no AS month_number_jnj,
                sellin_fact.cust_id AS customer_id,
                cust.sap_cust_nm AS sap_customer_name,
                cust.sap_sls_org AS sap_sales_org,
                cust.sap_cmp_id AS sap_company_id,
                cmp.company_nm AS sap_company_name,
                cust.sap_addr AS sap_address,
                cust.sap_region,
                cust.sap_city,
                cust.sap_post_cd AS sap_post_code,
                cust.sap_chnl_desc AS sap_channel_description,
                cust.sap_sls_office_cd AS sap_sales_office_code,
                cust.sap_sls_office_desc AS sap_sales_office_description,
                cust.sap_sls_grp_cd AS sap_sales_group_code,
                cust.sap_sls_grp_desc AS sap_sales_group_description,
                cust.sap_curr_cd AS sap_currency_code,
                cust.gch_region,
                cust.gch_cluster,
                cust.gch_subcluster,
                cust.gch_market,
                cust.gch_retail_banner,
                so_cust.dstrbtr_grp_cd,
                rg_cust.banner_description,
                rg_cust.go_to_model_description,
                sellin_fact.plnt AS plant,
                sellin_fact.acct_no AS account_number,
                sellin_fact.cust_grp AS customer_group,
                sellin_fact.cust_sls AS customer_sales,
                sellin_fact.pstng_per AS posting_per,
                sellin_fact.dstr_chnl AS distributor_channel,
                sellin_fact.item_cd AS item_code,
                mat.sap_mat_desc AS item_description,
                mat.gph_prod_frnchse AS franchise,
                mat.gph_prod_brnd AS brand,
                mat.gph_prod_vrnt AS variant,
                mat.gph_prod_sgmnt AS segment,
                mat.gph_prod_put_up_desc AS put_up,
                mat.is_npi AS npi_indicator,
                mat.npi_str_period AS npi_start_date,
                mat.npi_end_period AS npi_end_date,
                mat.is_reg AS reg_indicator,
                mat.is_hero AS hero_indicator,
                sellin_fact.base_val AS base_value,
                sellin_fact.sls_qty AS sales_quantity,
                sellin_fact.ret_qty AS return_quantity,
                sellin_fact.sls_less_rtn_qty AS sales_less_return_quantity,
                sellin_fact.gts_val AS gross_trade_sales_value,
                sellin_fact.ret_val AS return_value,
                sellin_fact.gts_less_rtn_val AS gross_trade_sales_less_return_value,
                sellin_fact.tp_val AS tp_value,
                sellin_fact.nts_val AS net_trade_sales_value,
                sellin_fact.nts_qty AS net_trade_sales_quantity,
                sellin_fact.sellin_target
            FROM (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            SELECT 
                                                edw_vw_th_sellin_sales_fact.item_cd,
                                                edw_vw_th_sellin_sales_fact.cust_id,
                                                edw_vw_th_sellin_sales_fact.sls_org,
                                                edw_vw_th_sellin_sales_fact.sls_grp,
                                                edw_vw_th_sellin_sales_fact.plnt,
                                                edw_vw_th_sellin_sales_fact.acct_no,
                                                edw_vw_th_sellin_sales_fact.cust_grp,
                                                edw_vw_th_sellin_sales_fact.cust_sls,
                                                edw_vw_th_sellin_sales_fact.pstng_per,
                                                edw_vw_th_sellin_sales_fact.dstr_chnl,
                                                edw_vw_th_sellin_sales_fact.jj_mnth_id,
                                                itg_th_ciw_account_lookup.area,
                                                itg_th_ciw_account_lookup.category,
                                                itg_th_ciw_account_lookup.account_name,
                                                "max"((edw_vw_th_sellin_sales_fact.pstng_dt)::text) AS max_pstng_dt,
                                                sum(edw_vw_th_sellin_sales_fact.base_val) AS base_val,
                                                sum(edw_vw_th_sellin_sales_fact.sls_qty) AS sls_qty,
                                                sum(edw_vw_th_sellin_sales_fact.ret_qty) AS ret_qty,
                                                sum(edw_vw_th_sellin_sales_fact.sls_less_rtn_qty) AS sls_less_rtn_qty,
                                                sum(edw_vw_th_sellin_sales_fact.gts_val) AS gts_val,
                                                sum(edw_vw_th_sellin_sales_fact.ret_val) AS ret_val,
                                                sum(edw_vw_th_sellin_sales_fact.gts_less_rtn_val) AS gts_less_rtn_val,
                                                sum(edw_vw_th_sellin_sales_fact.tp_val) AS tp_val,
                                                sum(edw_vw_th_sellin_sales_fact.nts_val) AS nts_val,
                                                sum(edw_vw_th_sellin_sales_fact.nts_qty) AS nts_qty,
                                                (
                                                    sum(edw_vw_th_sellin_sales_fact.base_val) - sum(edw_vw_th_sellin_sales_fact.nts_val)
                                                ) AS ciw_account_value,
                                                "max"(sellin_target.sls_target) AS sellin_target
                                            FROM (
                                                    (
                                                        edw_vw_th_sellin_sales_fact
                                                        LEFT JOIN itg_th_ciw_account_lookup ON (
                                                            (
                                                                (edw_vw_th_sellin_sales_fact.acct_no)::text = (itg_th_ciw_account_lookup.account_num)::text
                                                            )
                                                        )
                                                    )
                                                    LEFT JOIN (
                                                        SELECT 
                                                            "left"
                                                            (
                                                                (
                                                                    (itg_th_jbp_rolling_forecast.fisc_yr_per)::varchar
                                                                )::text,
                                                                4
                                                            ) AS "year",
                                                            "right"(
                                                                (
                                                                    (itg_th_jbp_rolling_forecast.fisc_yr_per)::varchar
                                                                )::text,
                                                                2
                                                            ) AS month,
                                                            (
                                                                "left"(
                                                                    (
                                                                        (itg_th_jbp_rolling_forecast.fisc_yr_per)::varchar
                                                                    )::text,
                                                                    4
                                                                ) || "right"(
                                                                    (
                                                                        (itg_th_jbp_rolling_forecast.fisc_yr_per)::varchar
                                                                    )::text,
                                                                    2
                                                                )
                                                            ) AS year_month,
                                                            itg_th_jbp_rolling_forecast.vrsn,
                                                            itg_th_jbp_rolling_forecast.manual_typ,
                                                            itg_th_jbp_rolling_forecast.sls_org,
                                                            itg_th_jbp_rolling_forecast.dstr_chnl,
                                                            itg_th_jbp_rolling_forecast.dvsn,
                                                            itg_th_jbp_rolling_forecast.cust_num,
                                                            itg_th_jbp_rolling_forecast.sls_target
                                                        FROM itg_th_jbp_rolling_forecast
                                                        WHERE (
                                                                (itg_th_jbp_rolling_forecast.cust_num IS NOT NULL)
                                                                AND (itg_th_jbp_rolling_forecast.matl_num IS NULL)
                                                            )
                                                    ) sellin_target ON (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        sellin_target.year_month = (edw_vw_th_sellin_sales_fact.jj_mnth_id)::text
                                                                    )
                                                                    AND (
                                                                        (edw_vw_th_sellin_sales_fact.sls_org)::text = (sellin_target.sls_org)::text
                                                                    )
                                                                )
                                                                AND (
                                                                    (edw_vw_th_sellin_sales_fact.dstr_chnl)::text = (sellin_target.dstr_chnl)::text
                                                                )
                                                            )
                                                            AND (
                                                                (edw_vw_th_sellin_sales_fact.cust_id)::text = (sellin_target.cust_num)::text
                                                            )
                                                        )
                                                    )
                                                )
                                            GROUP BY edw_vw_th_sellin_sales_fact.item_cd,
                                                edw_vw_th_sellin_sales_fact.cust_id,
                                                edw_vw_th_sellin_sales_fact.sls_grp,
                                                edw_vw_th_sellin_sales_fact.sls_org,
                                                edw_vw_th_sellin_sales_fact.plnt,
                                                edw_vw_th_sellin_sales_fact.acct_no,
                                                edw_vw_th_sellin_sales_fact.dstr_chnl,
                                                edw_vw_th_sellin_sales_fact.cust_grp,
                                                edw_vw_th_sellin_sales_fact.cust_sls,
                                                edw_vw_th_sellin_sales_fact.pstng_per,
                                                edw_vw_th_sellin_sales_fact.jj_mnth_id,
                                                itg_th_ciw_account_lookup.area,
                                                itg_th_ciw_account_lookup.category,
                                                itg_th_ciw_account_lookup.account_name
                                        ) sellin_fact
                                        JOIN (
                                            SELECT DISTINCT edw_vw_os_time_dim."year",
                                                edw_vw_os_time_dim.qrtr,
                                                edw_vw_os_time_dim.mnth_id,
                                                edw_vw_os_time_dim.mnth_no
                                            FROM edw_vw_os_time_dim
                                            WHERE (
                                                    edw_vw_os_time_dim."year" > date_part(year,(current_timestamp()::timestamp_ntz)) - 3
                                                )
                                        ) "time" ON (
                                            ((sellin_fact.jj_mnth_id)::text = "time".mnth_id)
                                        )
                                    )
                                    LEFT JOIN (
                                        SELECT 
                                            edw_vw_th_customer_dim.sap_cust_id,
                                            edw_vw_th_customer_dim.sap_cust_nm,
                                            edw_vw_th_customer_dim.sap_sls_org,
                                            edw_vw_th_customer_dim.sap_cmp_id,
                                            edw_vw_th_customer_dim.sap_addr,
                                            edw_vw_th_customer_dim.sap_region,
                                            edw_vw_th_customer_dim.sap_city,
                                            edw_vw_th_customer_dim.sap_post_cd,
                                            edw_vw_th_customer_dim.sap_chnl_cd,
                                            edw_vw_th_customer_dim.sap_chnl_desc,
                                            edw_vw_th_customer_dim.sap_sls_office_cd,
                                            edw_vw_th_customer_dim.sap_sls_office_desc,
                                            edw_vw_th_customer_dim.sap_sls_grp_cd,
                                            edw_vw_th_customer_dim.sap_sls_grp_desc,
                                            edw_vw_th_customer_dim.sap_curr_cd,
                                            edw_vw_th_customer_dim.gch_region,
                                            edw_vw_th_customer_dim.gch_cluster,
                                            edw_vw_th_customer_dim.gch_subcluster,
                                            edw_vw_th_customer_dim.gch_market,
                                            edw_vw_th_customer_dim.gch_retail_banner
                                        FROM edw_vw_th_customer_dim
                                    ) cust ON (
                                        (
                                            (
                                                (
                                                    (sellin_fact.cust_id)::text = (cust.sap_cust_id)::text
                                                )
                                                AND (
                                                    (sellin_fact.dstr_chnl)::text = (cust.sap_chnl_cd)::text
                                                )
                                            )
                                            AND (
                                                (sellin_fact.sls_org)::text = (cust.sap_sls_org)::text
                                            )
                                        )
                                    )
                                )
                                LEFT JOIN (
                                    SELECT edw_company_dim.co_cd,
                                        edw_company_dim.company_nm
                                    FROM edw_company_dim
                                    WHERE (
                                            (edw_company_dim.ctry_key)::text = ('TH'::varchar)::text
                                        )
                                ) cmp ON (((cust.sap_cmp_id)::text = (cmp.co_cd)::text))
                            )
                            JOIN (
                                SELECT DISTINCT edw_vw_th_dstrbtr_customer_dim.dstrbtr_grp_cd,
                                    edw_vw_th_dstrbtr_customer_dim.sap_soldto_code
                                FROM edw_vw_th_dstrbtr_customer_dim
                            ) so_cust ON (
                                (
                                    (cust.sap_cust_id)::text = (so_cust.sap_soldto_code)::text
                                )
                            )
                        )
                        LEFT JOIN (
                            SELECT sellin_mat.sap_matl_num,
                                sellin_mat.sap_mat_desc,
                                sellin_mat.gph_region,
                                sellin_mat.gph_prod_frnchse,
                                sellin_mat.gph_prod_brnd,
                                sellin_mat.gph_prod_vrnt,
                                sellin_mat.gph_prod_sgmnt,
                                sellin_mat.gph_prod_put_up_desc,
                                sellout_mat.is_npi,
                                sellout_mat.npi_str_period,
                                sellout_mat.npi_end_period,
                                sellout_mat.is_reg,
                                sellout_mat.is_promo,
                                sellout_mat.promo_strt_period,
                                sellout_mat.promo_end_period,
                                sellout_mat.is_mcl,
                                sellout_mat.is_hero
                            FROM (
                                    (
                                        SELECT DISTINCT edw_vw_th_material_dim.sap_matl_num,
                                            edw_vw_th_material_dim.sap_mat_desc,
                                            edw_vw_th_material_dim.gph_region,
                                            edw_vw_th_material_dim.gph_prod_frnchse,
                                            edw_vw_th_material_dim.gph_prod_brnd,
                                            edw_vw_th_material_dim.gph_prod_vrnt,
                                            edw_vw_th_material_dim.gph_prod_sgmnt,
                                            edw_vw_th_material_dim.gph_prod_put_up_desc
                                        FROM edw_vw_th_material_dim
                                    ) sellin_mat
                                    LEFT JOIN (
                                        SELECT DISTINCT edw_vw_th_dstrbtr_material_dim.dstrbtr_matl_num,
                                            edw_vw_th_dstrbtr_material_dim.is_npi,
                                            edw_vw_th_dstrbtr_material_dim.npi_str_period,
                                            edw_vw_th_dstrbtr_material_dim.npi_end_period,
                                            edw_vw_th_dstrbtr_material_dim.is_reg,
                                            edw_vw_th_dstrbtr_material_dim.is_promo,
                                            edw_vw_th_dstrbtr_material_dim.promo_strt_period,
                                            edw_vw_th_dstrbtr_material_dim.promo_end_period,
                                            edw_vw_th_dstrbtr_material_dim.is_mcl,
                                            edw_vw_th_dstrbtr_material_dim.is_hero
                                        FROM edw_vw_th_dstrbtr_material_dim
                                    ) sellout_mat ON (
                                        (
                                            (sellin_mat.sap_matl_num)::text = (sellout_mat.dstrbtr_matl_num)::text
                                        )
                                    )
                                )
                        ) mat ON (
                            (
                                (sellin_fact.item_cd)::text = (mat.sap_matl_num)::text
                            )
                        )
                    )
                    LEFT JOIN (
                        SELECT DISTINCT edw_gch_customerhierarchy.customer,
                            edw_gch_customerhierarchy.regional_banner AS banner_description,
                            edw_gch_customerhierarchy.apac_go_to_model AS go_to_model_description
                        FROM edw_gch_customerhierarchy
                        WHERE (
                                (edw_gch_customerhierarchy.country)::text = ('TH'::varchar)::text
                            )
                    ) rg_cust ON (
                        (
                            upper((cust.sap_cust_id)::text) = upper(
                                ltrim(
                                    (rg_cust.customer)::text,
                                    ('0'::varchar)::text
                                )
                            )
                        )
                    )
                )
        ) sellin
    GROUP BY 1,
        2,
        sellin.year_jnj,
        (sellin.year_quarter_jnj)::varchar,
        (sellin.year_month_jnj)::varchar,
        sellin.month_number_jnj,
        7,
        8,
        9,
        10,
        sellin.dstrbtr_grp_cd,
        12,
        13,
        sellin.item_code,
        sellin.item_description,
        sellin.franchise,
        sellin.brand,
        sellin.variant,
        sellin.segment,
        sellin.put_up
),
sales_inventory_sellin_combined as
(
    SELECT 
        edw_vw_th_inventory_analysis.data_type,
        edw_vw_th_inventory_analysis."year",
        edw_vw_th_inventory_analysis.year_quarter,
        edw_vw_th_inventory_analysis.month_year,
        left(edw_vw_th_inventory_analysis.order_date,19)::varchar as order_date,
        edw_vw_th_inventory_analysis.month_number,
        edw_vw_th_inventory_analysis.distributor_id,
        NULL::varchar AS region_desc,
        NULL::varchar AS city,
        edw_vw_th_inventory_analysis.whcode,
        edw_vw_th_inventory_analysis.whgroup,
        NULL::varchar AS district,
        NULL::varchar AS ar_code,
        NULL::varchar AS ar_name,
        NULL::varchar AS channel_code,
        NULL::varchar AS channel,
        NULL::varchar AS sales_office_code,
        NULL::varchar AS sales_office_name,
        NULL::varchar AS sales_group,
        NULL::varchar AS "cluster",
        NULL::varchar AS ar_type_code,
        NULL::varchar AS ar_type_name,
        NULL::varchar AS distributor_name,
        NULL::varchar AS sap_cust_id,
        edw_vw_th_inventory_analysis.sku_code,
        edw_vw_th_inventory_analysis.sku_description,
        NULL::varchar AS bar_code,
        NULL::varchar AS mcl_barcd,
        edw_vw_th_inventory_analysis.franchise,
        edw_vw_th_inventory_analysis.brand,
        edw_vw_th_inventory_analysis.variant,
        edw_vw_th_inventory_analysis.segment,
        edw_vw_th_inventory_analysis.put_up_description,
        NULL::varchar AS salesman_name,
        NULL::varchar AS salesman_code,
        NULL::varchar AS cn_reason_code,
        NULL::varchar AS cn_reason_description,
        sum(edw_vw_th_inventory_analysis.gross_trade_sales) AS gross_trade_sales,
        0 AS cn_damaged_goods,
        0 AS credit_note_amount,
        0 AS line_discount,
        0 AS bottom_line_discount,
        0 AS sales_quantity,
        0 AS return_quantity,
        0 AS quantity_dz,
        0 AS net_invoice,
        0 AS target_calls,
        0 AS target_sales,
        0 AS str_count,
        sum(edw_vw_th_inventory_analysis.inventory_quantity) AS inventory_quantity,
        sum(edw_vw_th_inventory_analysis.inventory) AS inventory,
        sum(edw_vw_th_inventory_analysis.si_sellin_target) AS si_sellin_target,
        sum(edw_vw_th_inventory_analysis.si_gross_trade_sales_value) AS si_gross_trade_sales_value
    FROM (
            select * from sales_inventory
            UNION ALL
            select * from sellin
        ) edw_vw_th_inventory_analysis
    GROUP BY 
    edw_vw_th_inventory_analysis.data_type,
    edw_vw_th_inventory_analysis."year",
    edw_vw_th_inventory_analysis.year_quarter,
    edw_vw_th_inventory_analysis.month_year,
    edw_vw_th_inventory_analysis.order_date,
    edw_vw_th_inventory_analysis.month_number,
    edw_vw_th_inventory_analysis.distributor_id,
    8,
    9,
    edw_vw_th_inventory_analysis.whcode,
    edw_vw_th_inventory_analysis.whgroup,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    edw_vw_th_inventory_analysis.sku_code,
    edw_vw_th_inventory_analysis.sku_description,
    27,
    28,
    edw_vw_th_inventory_analysis.franchise,
    edw_vw_th_inventory_analysis.brand,
    edw_vw_th_inventory_analysis.variant,
    edw_vw_th_inventory_analysis.segment,
    edw_vw_th_inventory_analysis.put_up_description,
    34,
    35,
    36,
    37
),
ar_type_placeholder as
(
    SELECT 
        'Ar_Type_Placeholder' AS data_type,
        (a.year)::integer AS "year",
        NULL AS year_quarter,
        (
            (
                a.year || ((a.month)::varchar)::text
            )
        )::varchar AS month_year,
        NULL AS order_date,
        (a.month)::integer AS month_number,
        a.distributor_id,
        NULL AS region_desc,
        NULL AS city,
        NULL AS whcode,
        NULL AS whgroup,
        NULL AS district,
        NULL AS ar_code,
        NULL AS ar_name,
        NULL AS channel_code,
        NULL AS channel,
        NULL AS sales_office_code,
        NULL AS sales_office_name,
        NULL AS sales_group,
        NULL AS "cluster",
        NULL AS ar_type_code,
        a.ar_type_name,
        NULL AS distributor_name,
        NULL AS sap_cust_id,
        NULL AS sku_code,
        NULL AS sku_description,
        NULL AS bar_code,
        NULL AS mcl_barcd,
        NULL AS franchise,
        NULL AS brand,
        NULL AS variant,
        NULL AS segment,
        NULL AS put_up_description,
        NULL AS salesman_name,
        NULL AS salesman_code,
        NULL AS cn_reason_code,
        NULL AS cn_reason_description,
        0 AS gross_trade_sales,
        0 AS cn_damaged_goods,
        0 AS credit_note_amount,
        0 AS line_discount,
        0 AS bottom_line_discount,
        0 AS sales_quantity,
        0 AS return_quantity,
        0 AS quantity_dz,
        0 AS net_invoice,
        0 AS target_calls,
        0 AS target_sales,
        0 AS str_count,
        0 AS inventory_quantity,
        0 AS inventory,
        0 AS si_sellin_target,
        0 AS si_gross_trade_sales_value
    FROM 
    (
            SELECT a.year,
                a.month,
                a.ar_type_name,
                a.distributor_id
            FROM (
                    (
                        SELECT b.ar_type_name,
                            b.distributor_id,
                            a.year,
                            a.month
                        FROM (
                                (
                                    SELECT DISTINCT edw_vw_th_sellout_analysis.ar_type_name,
                                        edw_vw_th_sellout_analysis.distributor_id
                                    FROM edw_th_sellout_analysis edw_vw_th_sellout_analysis
                                ) b
                                JOIN (
                                    SELECT DISTINCT to_char(
                                            edw_vw_th_sellout_analysis.order_date,
                                            ('YYYY'::varchar)::text
                                        ) AS year,
                                        to_char(
                                            edw_vw_th_sellout_analysis.order_date,
                                            ('mm'::varchar)::text
                                        ) AS month
                                    FROM edw_th_sellout_analysis
                                ) a ON ((1 = 1))
                            )
                    ) a
                    LEFT JOIN (
                        SELECT DISTINCT to_char(
                                edw_vw_th_sellout_analysis.order_date,
                                ('YYYY'::varchar)::text
                            ) AS year,
                            to_char(
                                edw_vw_th_sellout_analysis.order_date,
                                ('mm'::varchar)::text
                            ) AS month,
                            edw_vw_th_sellout_analysis.ar_type_name,
                            edw_vw_th_sellout_analysis.distributor_id
                        FROM edw_th_sellout_analysis
                    ) b ON (
                        (
                            (
                                (
                                    ((a.ar_type_name)::text = (b.ar_type_name)::text)
                                    AND (
                                        (a.distributor_id)::text = (b.distributor_id)::text
                                    )
                                )
                                AND (a.year = b.year)
                            )
                            AND (a.month = b.month)
                        )
                    )
                )
            WHERE (
                    (b.distributor_id IS NULL)
                    AND (b.year IS NULL)
                )
    ) a
),
franchise_placeholder as
(
    SELECT 
        'Franchise_Placeholder'::varchar AS data_type,
        (a.year)::integer AS "year",
        NULL AS year_quarter,
        (
            (
                a.year || ((a.month)::varchar)::text
            )
        )::varchar AS month_year,
        NULL AS order_date,
        (a.month)::integer AS month_number,
        a.distributor_id,
        NULL AS region_desc,
        NULL AS city,
        NULL AS whcode,
        NULL AS whgroup,
        NULL AS district,
        NULL AS ar_code,
        NULL AS ar_name,
        NULL AS channel_code,
        NULL AS channel,
        NULL AS sales_office_code,
        NULL AS sales_office_name,
        NULL AS sales_group,
        NULL AS "cluster",
        NULL AS ar_type_code,
        NULL AS ar_type_name,
        NULL AS distributor_name,
        NULL AS sap_cust_id,
        NULL AS sku_code,
        NULL AS sku_description,
        NULL AS bar_code,
        NULL AS mcl_barcd,
        a.franchise,
        NULL AS brand,
        NULL AS variant,
        NULL AS segment,
        NULL AS put_up_description,
        NULL AS salesman_name,
        NULL AS salesman_code,
        NULL AS cn_reason_code,
        NULL AS cn_reason_description,
        0 AS gross_trade_sales,
        0 AS cn_damaged_goods,
        0 AS credit_note_amount,
        0 AS line_discount,
        0 AS bottom_line_discount,
        0 AS sales_quantity,
        0 AS return_quantity,
        0 AS quantity_dz,
        0 AS net_invoice,
        0 AS target_calls,
        0 AS target_sales,
        0 AS str_count,
        0 AS inventory_quantity,
        0 AS inventory,
        0 AS si_sellin_target,
        0 AS si_gross_trade_sales_value
    FROM (
            SELECT a.year,
                a.month,
                a.franchise,
                a.distributor_id
            FROM (
                    (
                        SELECT b.franchise,
                            b.distributor_id,
                            a.year,
                            a.month
                        FROM (
                                (
                                    SELECT DISTINCT edw_vw_th_sellout_analysis.franchise,
                                        edw_vw_th_sellout_analysis.distributor_id
                                    FROM edw_th_sellout_analysis
                                ) b
                                JOIN (
                                    SELECT DISTINCT to_char(
                                            edw_vw_th_sellout_analysis.order_date,
                                            ('YYYY'::varchar)::text
                                        ) AS year,
                                        to_char(
                                            edw_vw_th_sellout_analysis.order_date,
                                            ('mm'::varchar)::text
                                        ) AS month
                                    FROM edw_th_sellout_analysis
                                ) a ON ((1 = 1))
                            )
                    ) a
                    LEFT JOIN (
                        SELECT DISTINCT to_char(
                                edw_vw_th_sellout_analysis.order_date,
                                ('YYYY'::varchar)::text
                            ) AS year,
                            to_char(
                                edw_vw_th_sellout_analysis.order_date,
                                ('mm'::varchar)::text
                            ) AS month,
                            edw_vw_th_sellout_analysis.franchise,
                            edw_vw_th_sellout_analysis.distributor_id
                        FROM edw_th_sellout_analysis
                    ) b ON (
                        (
                            (
                                (
                                    ((a.franchise)::text = (b.franchise)::text)
                                    AND (
                                        (a.distributor_id)::text = (b.distributor_id)::text
                                    )
                                )
                                AND (a.year = b.year)
                            )
                            AND (a.month = b.month)
                        )
                    )
                )
            WHERE (
                    (b.distributor_id IS NULL)
                    AND (b.year IS NULL)
                )
        ) a
),
transformed as
(
    SELECT * from sellout
    UNION ALL
    SELECT * from cop
    UNION ALL
    SELECT * from sales_inventory_sellin_combined
    UNION ALL
    SELECT * from ar_type_placeholder
    UNION ALL
    SELECT * from franchise_placeholder
),
final as
(
    select 
        data_type,
        "year",
        year_quarter,
        month_year,
        order_date,
        month_number,
        distributor_id,
        region_desc,
        city,
        whcode,
        whgroup,
        district,
        ar_code,
        ar_name,
        channel_code,
        channel,
        sales_office_code,
        sales_office_name,
        sales_group,
        "cluster",
        ar_type_code,
        ar_type_name,
        distributor_name,
        sap_cust_id,
        sku_code,
        sku_description,
        bar_code,
        mcl_barcd,
        franchise,
        brand,
        variant,
        segment,
        put_up_description,
        salesman_name,
        salesman_code,
        cn_reason_code,
        cn_reason_description,
        gross_trade_sales,
        cn_damaged_goods,
        credit_note_amount,
        line_discount,
        bottom_line_discount,
        sales_quantity,
        return_quantity,
        quantity_dz,
        net_invoice,
        target_calls,
        target_sales,
        str_count,
        inventory_quantity,
        inventory,
        si_sellin_target,
        si_gross_trade_sales_value
    from transformed
)
select * from final