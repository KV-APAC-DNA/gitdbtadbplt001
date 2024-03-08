with edw_vw_th_sellout_sales_fact as
(
    select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_fact') }}
    
),
edw_vw_th_sellout_sales_foc_fact as
(
    select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_foc_fact') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_th_dstrbtr_customer_dim as
(
    select * from {{ ref('thaedw_integration__edw_vw_th_dstrbtr_customer_dim') }}
    
),
itg_th_target_distribution as
(
    select * from {{ ref('thaitg_integration__itg_th_target_distribution') }}
),
itg_th_productgrouping as
(
    select * from {{ ref('thaitg_integration__itg_th_productgrouping') }}
),
itg_th_target_sales as
(
    select * from {{ ref('thaitg_integration__itg_th_target_sales') }}
),
edw_vw_th_customer_dim as 
(
    select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
),
edw_vw_th_dstrbtr_material_dim as 
(
    select * from {{ ref('thaedw_integration__edw_vw_th_dstrbtr_material_dim') }}
    
),
edw_vw_th_material_dim as 
(
    select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
    
),
itg_th_dstrbtr_material_dim as
(
    select * from {{ ref('thaedw_integration__itg_th_dstrbtr_material_dim') }}
),
union_1 as
(
    SELECT 
        edw_vw_th_sellout_sales_fact.cntry_cd,
        edw_vw_th_sellout_sales_fact.cntry_nm,
        edw_vw_th_sellout_sales_fact.bill_date,
        edw_vw_th_sellout_sales_fact.dstrbtr_grp_cd,
        edw_vw_th_sellout_sales_fact.dstrbtr_matl_num,
        edw_vw_th_sellout_sales_fact.cust_cd,
        edw_vw_th_sellout_sales_fact.slsmn_nm,
        edw_vw_th_sellout_sales_fact.slsmn_cd,
        edw_vw_th_sellout_sales_fact.cn_reason_cd,
        edw_vw_th_sellout_sales_fact.cn_reason_desc,
        sum(
            CASE
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd IS NULL
                OR left(
                    edw_vw_th_sellout_sales_fact.cn_reason_cd::text,
                    1
                ) <> 'N'::varchar::text THEN (
                    edw_vw_th_sellout_sales_fact.grs_trd_sls + edw_vw_th_sellout_sales_fact.ret_val
                )::double precision
                ELSE NULL::double precision
            END
        ) AS grs_trd_sls,
        sum(
            CASE
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd IS NULL
                OR edw_vw_th_sellout_sales_fact.cn_reason_cd::text = ''::varchar::text THEN 0.0::double precision
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd::text LIKE 'D%'::varchar::text THEN 0.0::double precision
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd::text LIKE 'N%'::varchar::text THEN edw_vw_th_sellout_sales_fact.net_trd_sls::double precision
                ELSE NULL::double precision
            END
        ) AS cn_dmgd_gds,
        sum(
            CASE
                WHEN (
                    edw_vw_th_sellout_sales_fact.cn_reason_cd IS NULL
                    OR edw_vw_th_sellout_sales_fact.cn_reason_cd::text = ''::varchar::text
                )
                AND edw_vw_th_sellout_sales_fact.net_trd_sls::double precision < 0::double precision THEN edw_vw_th_sellout_sales_fact.net_trd_sls::double precision
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd::text LIKE 'D%'::varchar::text THEN edw_vw_th_sellout_sales_fact.net_trd_sls::double precision
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd::text LIKE 'N%'::varchar::text THEN 0.0::double precision
                ELSE NULL::double precision
            END
        ) AS crdt_nt_amt,
        sum(edw_vw_th_sellout_sales_fact.trd_discnt_item_lvl) AS trd_discnt_item_lvl,
        sum(edw_vw_th_sellout_sales_fact.trd_discnt_bill_lvl) AS trd_discnt_bill_lvl,
        sum(edw_vw_th_sellout_sales_fact.ret_val) AS ret_val,
        sum(edw_vw_th_sellout_sales_fact.sls_qty) AS sls_qty,
        sum(edw_vw_th_sellout_sales_fact.ret_qty) AS ret_qty,
        sum(
            CASE
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd IS NULL
                OR left(
                    edw_vw_th_sellout_sales_fact.cn_reason_cd::text,
                    1
                ) <> 'N'::varchar::text THEN edw_vw_th_sellout_sales_fact.sls_qty::double precision
                ELSE NULL::double precision
            END
        ) AS quantity_dz,
        sum(
            CASE
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd IS NULL
                OR edw_vw_th_sellout_sales_fact.cn_reason_cd::text = ''::varchar::text THEN edw_vw_th_sellout_sales_fact.net_trd_sls::double precision
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd::text LIKE 'D%'::varchar::text THEN edw_vw_th_sellout_sales_fact.net_trd_sls::double precision
                WHEN edw_vw_th_sellout_sales_fact.cn_reason_cd::text LIKE 'N%'::varchar::text THEN 0.0::double precision
                ELSE NULL::double precision
            END
        ) AS net_trd_sls,
        0 AS foc_net_invc,
        0 AS foc_trd_discnt_item_lvl,
        0 AS foc_trd_discnt_bill_lvl,
        0::double precision AS tot_bf_discount,
        0 AS gross_sales,
        edw_vw_th_sellout_sales_fact.store,
        edw_vw_th_sellout_sales_fact.re_nm
    FROM edw_vw_th_sellout_sales_fact
    GROUP BY 
        edw_vw_th_sellout_sales_fact.cntry_cd,
        edw_vw_th_sellout_sales_fact.cntry_nm,
        edw_vw_th_sellout_sales_fact.bill_date,
        edw_vw_th_sellout_sales_fact.dstrbtr_grp_cd,
        edw_vw_th_sellout_sales_fact.dstrbtr_matl_num,
        edw_vw_th_sellout_sales_fact.cust_cd,
        edw_vw_th_sellout_sales_fact.slsmn_nm,
        edw_vw_th_sellout_sales_fact.slsmn_cd,
        edw_vw_th_sellout_sales_fact.cn_reason_cd,
        edw_vw_th_sellout_sales_fact.cn_reason_desc,
        edw_vw_th_sellout_sales_fact.store,
        edw_vw_th_sellout_sales_fact.re_nm
),
union_2 as
(
    SELECT 
        edw_vw_th_sellout_sales_foc_fact.cntry_cd,
        edw_vw_th_sellout_sales_foc_fact.cntry_nm,
        edw_vw_th_sellout_sales_foc_fact.bill_date,
        edw_vw_th_sellout_sales_foc_fact.dstrbtr_grp_cd,
        edw_vw_th_sellout_sales_foc_fact.dstrbtr_matl_num,
        edw_vw_th_sellout_sales_foc_fact.cust_cd,
        edw_vw_th_sellout_sales_foc_fact.slsmn_nm,
        edw_vw_th_sellout_sales_foc_fact.slsmn_cd,
        edw_vw_th_sellout_sales_foc_fact.cn_reason_cd,
        edw_vw_th_sellout_sales_foc_fact.cn_reason_desc,
        0 AS grs_trd_sls,
        0 AS cn_dmgd_gds,
        0 AS crdt_nt_amt,
        0 AS trd_discnt_item_lvl,
        0 AS trd_discnt_bill_lvl,
        0 AS ret_val,
        0 AS sls_qty,
        0 AS ret_qty,
        0 AS quantity_dz,
        0 AS net_trd_sls,
        sum(edw_vw_th_sellout_sales_foc_fact.net_trd_sls) AS foc_net_invc,
        sum(edw_vw_th_sellout_sales_foc_fact.trd_discnt_item_lvl) AS foc_trd_discnt_item_lvl,
        sum(edw_vw_th_sellout_sales_foc_fact.trd_discnt_bill_lvl) AS foc_trd_discnt_bill_lvl,
        sum(edw_vw_th_sellout_sales_foc_fact.jj_net_trd_sls::double precision) AS tot_bf_discount,
        sum(edw_vw_th_sellout_sales_foc_fact.gross_sales) AS gross_sales,
        NULL::varchar AS store,
        NULL::varchar AS re_nm
    FROM edw_vw_th_sellout_sales_foc_fact
    WHERE edw_vw_th_sellout_sales_foc_fact.cntry_cd::text = 'TH'::varchar::text
        AND edw_vw_th_sellout_sales_foc_fact.iscancel = 0
    GROUP BY 
        edw_vw_th_sellout_sales_foc_fact.cntry_cd,
        edw_vw_th_sellout_sales_foc_fact.cntry_nm,
        edw_vw_th_sellout_sales_foc_fact.bill_date,
        edw_vw_th_sellout_sales_foc_fact.order_no,
        edw_vw_th_sellout_sales_foc_fact.product_name2,
        edw_vw_th_sellout_sales_foc_fact.iscancel,
        edw_vw_th_sellout_sales_foc_fact.grp_cd,
        edw_vw_th_sellout_sales_foc_fact.prom_cd1,
        edw_vw_th_sellout_sales_foc_fact.prom_cd2,
        edw_vw_th_sellout_sales_foc_fact.prom_cd3,
        edw_vw_th_sellout_sales_foc_fact.dstrbtr_grp_cd,
        edw_vw_th_sellout_sales_foc_fact.dstrbtr_matl_num,
        edw_vw_th_sellout_sales_foc_fact.cust_cd,
        edw_vw_th_sellout_sales_foc_fact.slsmn_nm,
        edw_vw_th_sellout_sales_foc_fact.slsmn_cd,
        edw_vw_th_sellout_sales_foc_fact.cn_reason_cd,
        edw_vw_th_sellout_sales_foc_fact.cn_reason_desc,
        edw_vw_th_sellout_sales_foc_fact.grs_prc
),
sales as
(
    SELECT 
        sales.cntry_cd,
        sales.cntry_nm,
        "time"."year",
        "time".qrtr,
        "time".mnth_id,
        "time".mnth_no,
        "time".wk,
        "time".mnth_wk_no,
        "time".cal_date,
        sales.bill_date,
        sales.dstrbtr_grp_cd,
        sales.dstrbtr_matl_num,
        sales.cust_cd,
        sales.slsmn_nm,
        sales.slsmn_cd,
        sales.cn_reason_cd,
        sales.cn_reason_desc,
        sales.grs_trd_sls,
        sales.cn_dmgd_gds,
        sales.crdt_nt_amt,
        sales.trd_discnt_item_lvl,
        sales.trd_discnt_bill_lvl,
        sales.foc_trd_discnt_item_lvl,
        sales.foc_trd_discnt_bill_lvl,
        sales.sls_qty,
        sales.ret_qty,
        sales.quantity_dz,
        sales.net_trd_sls,
        sales.foc_net_invc,
        sales.tot_bf_discount,
        sales.gross_sales,
        sales.store,
        sales.re_nm
    FROM 
    (
        select * from union_1
        UNION ALL
        select * from union_2
    ) sales
    JOIN 
    (
        SELECT DISTINCT edw_vw_os_time_dim."year",
            edw_vw_os_time_dim.qrtr,
            edw_vw_os_time_dim.mnth_id,
            edw_vw_os_time_dim.mnth_no,
            edw_vw_os_time_dim.wk,
            edw_vw_os_time_dim.mnth_wk_no,
            edw_vw_os_time_dim.cal_date
        FROM edw_vw_os_time_dim
        WHERE edw_vw_os_time_dim."year" > date_part(year,(current_timestamp()::timestamp_ntz)) - 3
            OR edw_vw_os_time_dim."year" > date_part(year,(current_timestamp()::timestamp_ntz)) - 3
    ) "time" ON sales.bill_date = "time".cal_date::timestamp_ntz
),
transformed as
(
    SELECT 
        sales.bill_date AS order_date,
        sales."year",
        sales.qrtr AS year_quarter,
        sales.mnth_id AS month_year,
        sales.mnth_no AS month_number,
        sales.wk AS year_week_number,
        sales.mnth_wk_no AS month_week_number,
        sales.cntry_cd AS country_code,
        sales.cntry_nm AS country_name,
        sales.dstrbtr_grp_cd AS distributor_id,
        sellout_cust.region_nm AS region_desc,
        sellout_cust.prov_nm AS city,
        sellout_cust.city_nm AS district,
        sales.cust_cd AS ar_code,
        sellout_cust.cust_nm AS ar_name,
        sellout_cust.chnl_cd AS channel_code,
        sellout_cust.chnl_desc AS channel,
        sellout_cust.sls_office_cd AS sales_office_code,
        CASE
            WHEN "substring"(sellout_cust.sls_grp_cd::text, 2, 1) = '1'::varchar::text THEN sales.dstrbtr_grp_cd::text || ' Van'::varchar::text
            ELSE sales.dstrbtr_grp_cd::text || ' Credit'::varchar::text
        END AS sales_office_name,
        sellout_cust.sls_grp_cd AS sales_group,
        sellout_cust.cust_grp_cd AS "cluster",
        sellout_cust.outlet_type_cd AS ar_type_code,
        sellout_cust.outlet_type_desc AS ar_type_name,
        sellin_cust.sap_cust_nm AS distributor_name,
        sellin_cust.sap_cust_id,
        sellin_cust.sap_cust_nm,
        sellin_cust.sap_sls_org,
        sellin_cust.sap_cmp_id,
        sellin_cust.sap_cntry_cd,
        sellin_cust.sap_cntry_nm,
        sellin_cust.sap_addr,
        sellin_cust.sap_region,
        sellin_cust.sap_state_cd,
        sellin_cust.sap_city,
        sellin_cust.sap_post_cd,
        sellin_cust.sap_chnl_cd,
        sellin_cust.sap_chnl_desc,
        sellin_cust.sap_sls_office_cd,
        sellin_cust.sap_sls_office_desc,
        sellin_cust.sap_sls_grp_cd,
        sellin_cust.sap_sls_grp_desc,
        sellin_cust.sap_prnt_cust_key,
        sellin_cust.sap_prnt_cust_desc,
        sellin_cust.sap_cust_chnl_key,
        sellin_cust.sap_cust_chnl_desc,
        sellin_cust.sap_cust_sub_chnl_key,
        sellin_cust.sap_sub_chnl_desc,
        sellin_cust.sap_go_to_mdl_key,
        sellin_cust.sap_go_to_mdl_desc,
        sellin_cust.sap_bnr_key,
        sellin_cust.sap_bnr_desc,
        sellin_cust.sap_bnr_frmt_key,
        sellin_cust.sap_bnr_frmt_desc,
        sellin_cust.retail_env,
        sales.dstrbtr_matl_num AS sku_code,
        CASE
            WHEN si_matl.sap_mat_desc IS NOT NULL THEN si_matl.sap_mat_desc
            ELSE dstrbtr_matl_dim.name_eng
        END AS sku_description,
        so_matl.dstrbtr_bar_cd AS bar_code,
        si_matl.gph_prod_frnchse AS franchise,
        si_matl.gph_prod_brnd AS brand,
        si_matl.gph_prod_vrnt AS variant,
        si_matl.gph_prod_sgmnt AS segment,
        si_matl.gph_prod_put_up_desc AS put_up_description,
        si_matl.prod_sub_brand,
        si_matl.prod_subsegment,
        si_matl.prod_category,
        si_matl.prod_subcategory,
        sales.slsmn_nm AS salesman_name,
        sales.slsmn_cd AS salesman_code,
        sales.cn_reason_cd AS cn_reason_code,
        sales.cn_reason_desc AS cn_reason_description,
        sales.grs_trd_sls AS gross_trade_sales,
        sales.cn_dmgd_gds AS cn_damaged_goods,
        sales.crdt_nt_amt AS credit_note_amount,
        sales.trd_discnt_item_lvl AS line_discount,
        sales.trd_discnt_bill_lvl AS bottom_line_discount,
        sales.sls_qty AS sales_quantity,
        sales.ret_qty AS return_quantity,
        sales.quantity_dz,
        sales.net_trd_sls AS net_invoice,
        target_distribution.target AS target_calls,
        target_sales.target AS target_sales,
        sales.foc_trd_discnt_item_lvl,
        sales.foc_trd_discnt_bill_lvl,
        sales.tot_bf_discount,
        sales.foc_net_invc,
        sales.gross_sales,
        sales.store,
        sales.re_nm
    FROM sales
    LEFT JOIN 
    (
        SELECT DISTINCT name_eng,
            item_cd
        FROM itg_th_dstrbtr_material_dim
    ) dstrbtr_matl_dim ON sales.dstrbtr_matl_num = dstrbtr_matl_dim.item_cd
    LEFT JOIN 
    (
        SELECT 
            DISTINCT 
            region_nm,
            prov_nm,
            city_nm,
            cust_nm,
            chnl_cd,
            chnl_desc,
            sls_office_cd,
            sls_grp_cd,
            cust_grp_cd,
            outlet_type_cd,
            outlet_type_desc,
            cust_cd,
            dstrbtr_grp_cd,
            sap_soldto_code,
        FROM edw_vw_th_dstrbtr_customer_dim
    ) sellout_cust ON upper(sales.cust_cd::text) = upper(sellout_cust.cust_cd::text)
    AND upper(sales.dstrbtr_grp_cd::text) = upper(sellout_cust.dstrbtr_grp_cd::text)
    LEFT JOIN (
        SELECT 
            target.dstrbtr_id,
            target.period,
            target.target,
            prodgroup.prod_cd
        FROM itg_th_target_distribution target,
            itg_th_productgrouping prodgroup
        WHERE upper(target.prod_nm::text) = upper(prodgroup.prod_grp::text)
    ) target_distribution ON upper(sales.dstrbtr_matl_num::text) = upper(target_distribution.prod_cd::text)
    AND upper(sales.dstrbtr_grp_cd::text) = upper(target_distribution.dstrbtr_id::text)
    AND (
        "substring"(sales.bill_date::varchar::text, 1, 4) || "substring"(sales.bill_date::varchar::text, 6, 2)
    ) = target_distribution.period::text
    LEFT JOIN 
    (
        SELECT 
            itg_th_target_sales.dstrbtr_id,
            itg_th_target_sales.sls_office,
            itg_th_target_sales.sls_grp,
            itg_th_target_sales.target,
            itg_th_target_sales.period
        FROM itg_th_target_sales
    ) target_sales ON sales.dstrbtr_grp_cd::text = target_sales.dstrbtr_id::text
    AND (
        "substring"(sales.bill_date::varchar::text, 1, 4) || "substring"(sales.bill_date::varchar::text, 6, 2)
    ) = target_sales.period::text
    AND upper(sellout_cust.sls_office_cd::text) = upper(target_sales.sls_office::text)
    AND upper(sellout_cust.sls_grp_cd::text) = upper(target_sales.sls_grp::text)
    LEFT JOIN 
    (
        SELECT 
            sap_cust_id,
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
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            sap_cust_chnl_key,
            sap_cust_chnl_desc,
            sap_cust_sub_chnl_key,
            sap_sub_chnl_desc,
            sap_go_to_mdl_key,
            sap_go_to_mdl_desc,
            sap_bnr_key,
            sap_bnr_desc,
            sap_bnr_frmt_key,
            sap_bnr_frmt_desc,
            retail_env,
        FROM edw_vw_th_customer_dim
    ) sellin_cust ON upper(sellout_cust.sap_soldto_code::text) = upper(sellin_cust.sap_cust_id::text)
    LEFT JOIN 
    (
        SELECT DISTINCT 
            edw_vw_th_material_dim.cntry_key,
            edw_vw_th_material_dim.sap_matl_num,
            edw_vw_th_material_dim.sap_mat_desc,
            edw_vw_th_material_dim.gph_region,
            edw_vw_th_material_dim.gph_prod_frnchse,
            edw_vw_th_material_dim.gph_prod_brnd,
            edw_vw_th_material_dim.gph_prod_vrnt,
            edw_vw_th_material_dim.gph_prod_sgmnt,
            edw_vw_th_material_dim.gph_prod_put_up_desc,
            edw_vw_th_material_dim.gph_prod_sub_brnd AS prod_sub_brand,
            edw_vw_th_material_dim.gph_prod_subsgmnt AS prod_subsegment,
            edw_vw_th_material_dim.gph_prod_ctgry AS prod_category,
            edw_vw_th_material_dim.gph_prod_subctgry AS prod_subcategory
        FROM edw_vw_th_material_dim
    ) si_matl ON upper(sales.dstrbtr_matl_num::text) = 
        upper(ltrim(si_matl.sap_matl_num::text,'0'::varchar::text)
    )
    LEFT JOIN 
    (
        SELECT DISTINCT 
            sap_matl_num,
            dstrbtr_bar_cd
        FROM edw_vw_th_dstrbtr_material_dim
    ) so_matl ON si_matl.sap_matl_num::text = so_matl.sap_matl_num::text
),
final as
(
    select 
        order_date,
        "year",
        year_quarter,
        month_year,
        month_number,
        year_week_number,
        month_week_number,
        country_code,
        country_name,
        distributor_id,
        region_desc,
        city,
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
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_cust_chnl_key,
        sap_cust_chnl_desc,
        sap_cust_sub_chnl_key,
        sap_sub_chnl_desc,
        sap_go_to_mdl_key,
        sap_go_to_mdl_desc,
        sap_bnr_key,
        sap_bnr_desc,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env,
        sku_code,
        sku_description,
        bar_code,
        franchise,
        brand,
        variant,
        segment,
        put_up_description,
        prod_sub_brand,
        prod_subsegment,
        prod_category,
        prod_subcategory,
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
        foc_trd_discnt_item_lvl,
        foc_trd_discnt_bill_lvl,
        tot_bf_discount,
        foc_net_invc,
        gross_sales,
        store,
        re_nm
    from transformed
)
select * from final