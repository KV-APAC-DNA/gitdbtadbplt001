with edw_vw_th_sellin_analysis as
(
    select * from {{ ref('thaedw_integration__edw_vw_th_sellin_analysis') }}
),
edw_vw_th_sellout_sales_fact as
(
    select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_fact') }}
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
edw_vw_th_material_dim as
(
    select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
),
union_1 as
(
    select 
        'sellin' AS data_type,
        (year_jnj)::integer AS year_jnj,
        (year_quarter_jnj)::character varying AS year_quarter_jnj,
        (year_month_jnj)::character varying AS year_month_jnj,
        month_number_jnj,
        item_code,
        item_description,
        franchise,
        brand,
        variant,
        segment,
        put_up,
        prod_sub_brand,
        prod_subsegment,
        prod_category,
        prod_subcategory,
        sap_sls_org,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_cust_chnl_key,
        sap_cust_chnl_desc,
        sap_cust_sub_chnl_key,
        sap_sub_chnl_desc,
        sap_go_to_mdl_key,
        go_to_model_description,
        sap_bnr_key,
        banner_description,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env,
        customer_id,
        sap_customer_name,
        sap_sales_office_code,
        sap_sales_office_description,
        sap_sales_group_code,
        sap_sales_group_description,
        0 AS so_gross_trade_sales,
        0 AS so_cn_damaged_goods,
        0 AS so_credit_note_amount,
        0 AS so_line_discount,
        0 AS so_bottom_line_discount,
        0 AS so_sales_quantity,
        0 AS so_return_quantity,
        0 AS so_quantity_dz,
        0 AS so_net_invoice,
        0 AS so_target_calls,
        0 AS so_target_sales,
        sum(base_value) AS si_base_value,
        sum(sales_quantity) AS si_sales_quantity,
        sum(return_quantity) AS si_return_quantity,
        sum(sales_less_return_quantity) AS si_sales_less_return_quantity,
        sum(gross_trade_sales_value) AS si_gross_trade_sales_value,
        sum(return_value) AS si_return_value,
        sum(gross_trade_sales_less_return_value) AS si_gross_trade_sales_less_return_value,
        sum(tp_value) AS si_tp_value,
        sum(net_trade_sales_value) AS si_net_trade_sales_value,
        sum(net_trade_sales_quantity) AS si_net_trade_sales_quantity
    from edw_vw_th_sellin_analysis
    GROUP BY 
        year_jnj,
        year_quarter_jnj,
        year_month_jnj,
        month_number_jnj,
        item_code,
        item_description,
        franchise,
        brand,
        variant,
        segment,
        put_up,
        prod_sub_brand,
        prod_subsegment,
        prod_category,
        prod_subcategory,
        sap_sls_org,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_cust_chnl_key,
        sap_cust_chnl_desc,
        sap_cust_sub_chnl_key,
        sap_sub_chnl_desc,
        sap_go_to_mdl_key,
        go_to_model_description,
        sap_bnr_key,
        banner_description,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env,
        customer_id,
        sap_customer_name,
        sap_sales_office_code,
        sap_sales_office_description,
        sap_sales_group_code,
        sap_sales_group_description
),
sales as
(
    select 
        cntry_cd,
        cntry_nm,
        bill_date,
        dstrbtr_grp_cd,
        dstrbtr_matl_num,
        cust_cd,
        slsmn_nm,
        slsmn_cd,
        cn_reason_cd,
        cn_reason_desc,
        sum
        (
            case
                when 
                (
                    (cn_reason_cd IS NULL)
                    OR ( left((cn_reason_cd)::text,1) <> ('N'::character varying)::text)
                ) 
                then 
                ((grs_trd_sls + ret_val))::double precision
                else NULL::double precision
            END
        ) AS grs_trd_sls,
        sum(
            case
                when 
                (
                    ( cn_reason_cd IS NULL)
                    OR 
                    ((cn_reason_cd)::text = (''::character varying)::text)
                ) 
                then (0.0)::double precision
                when 
                (
                    (cn_reason_cd)::text like ('D%'::character varying)::text
                ) 
                then (0.0)::double precision
                when 
                (
                    (cn_reason_cd)::text like ('N%'::character varying)::text
                ) 
                then (net_trd_sls)::double precision
                else NULL::double precision
            END
        ) AS cn_dmgd_gds,
        sum(
            case
                when 
                (
                    (
                        (cn_reason_cd IS NULL)
                        OR 
                        ((cn_reason_cd)::text = (''::character varying)::text)
                    )
                    AND ((net_trd_sls)::double precision < (0)::double precision)
                )
                then (net_trd_sls)::double precision
                when 
                (
                    (cn_reason_cd)::text like ('D%'::character varying)::text
                ) 
                then (net_trd_sls)::double precision
                when
                (
                    (cn_reason_cd)::text like ('N%'::character varying)::text
                ) 
                then (0.0)::double precision
                else NULL::double precision
            END
        ) AS crdt_nt_amt,
        sum(trd_discnt_item_lvl) AS trd_discnt_item_lvl,
        sum(trd_discnt_bill_lvl) AS trd_discnt_bill_lvl,
        sum(ret_val) AS ret_val,
        sum(sls_qty) AS sls_qty,
        sum(ret_qty) AS ret_qty,
        sum(
            case
                when
                (
                    (
                        cn_reason_cd IS NULL
                    )
                    OR (
                        left((cn_reason_cd)::text,1) <> ('N'::character varying)::text
                    )
                ) 
                then (sls_qty)::double precision
                else NULL::double precision
            END
        ) AS quantity_dz,
        sum(
            case
                when 
                (
                    (
                        cn_reason_cd IS NULL
                    )
                    OR (
                        (cn_reason_cd)::text = (''::character varying)::text
                    )
                )
                then (net_trd_sls)::double precision
                when 
                (
                    (cn_reason_cd)::text like ('D%'::character varying)::text
                ) then (net_trd_sls)::double precision
                when 
                (
                    (cn_reason_cd)::text like ('N%'::character varying)::text
                ) then (0.0)::double precision
                else NULL::double precision
            END
        ) AS net_trd_sls
    from edw_vw_th_sellout_sales_fact
    GROUP BY cntry_cd,
        cntry_nm,
        bill_date,
        dstrbtr_grp_cd,
        dstrbtr_matl_num,
        cust_cd,
        slsmn_nm,
        slsmn_cd,
        cn_reason_cd,
        cn_reason_desc
                                            
),
sales_2 as
(
    select 
        sales.cntry_cd,
        sales.cntry_nm,
        "time"."year",
        "time".qrtr,
        "time".mnth_id,
        "time".mnth_no,
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
        sales.sls_qty,
        sales.ret_qty,
        sales.quantity_dz,
        sales.net_trd_sls
    from sales
    JOIN 
        (
            select edw_vw_os_time_dim.cal_year AS "year",
            (
                (
                    ((edw_vw_os_time_dim.cal_year)::character varying)::text || ('/Q'::character varying)::text
                ) || 
                ((edw_vw_os_time_dim.cal_qrtr_no)::character varying)::text
            ) AS qrtr,
            edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
            edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
            edw_vw_os_time_dim.cal_date
            from edw_vw_os_time_dim
            where 
            (
                (
                    edw_vw_os_time_dim."year" > date_part(year,(current_timestamp()::timestamp_ntz)) - 3
                    OR (
                        edw_vw_os_time_dim."year" > date_part(year,(current_timestamp()::timestamp_ntz)) - 3
                    )
                )
            ) 
        )    
    "time" on 
    sales.bill_date = ("time".cal_date)::timestamp without time zone
),
sellout_cust as
(
    select distinct 
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
        sap_soldto_code
    from edw_vw_th_dstrbtr_customer_dim
),
target_distribution as
(   
    select 
        "target".dstrbtr_id,
        "target".period,
        "target".target,
        prodgroup.prod_cd
    from itg_th_target_distribution "target",
        itg_th_productgrouping prodgroup
    where (upper(("target".prod_nm)::text) = upper((prodgroup.prod_grp)::text))    
),
target_sales as
(
    select 
        itg_th_target_sales.dstrbtr_id,
        itg_th_target_sales.sls_office,
        itg_th_target_sales.sls_grp,
        itg_th_target_sales.target,
        itg_th_target_sales.period
    from itg_th_target_sales
),
sellin_cust as
(
    select 
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
        sap_go_to_mdl_desc AS go_to_model_description,
        sap_bnr_key,
        sap_bnr_desc AS banner_description,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env
    from edw_vw_th_customer_dim sellin_cust
),
matl as
(
    select distinct 
        cntry_key,
        sap_matl_num,
        sap_mat_desc,
        gph_region,
        gph_prod_frnchse,
        gph_prod_brnd,
        gph_prod_vrnt,
        gph_prod_sgmnt,
        gph_prod_put_up_desc,
        gph_prod_sub_brnd AS prod_sub_brand,
        gph_prod_subsgmnt AS prod_subsegment,
        gph_prod_ctgry AS prod_category,
        gph_prod_subctgry AS prod_subcategory
    from edw_vw_th_material_dim
),
union_2 as
(
    select 
        'sellout' AS data_type,
        sellout."year" AS year_jnj,
        (sellout.year_quarter)::character varying AS year_quarter_jnj,
        (sellout.month_year)::character varying AS year_month_jnj,
        sellout.month_number AS month_number_jnj,
        sellout.sku_code AS item_code,
        sellout.sku_description AS item_description,
        sellout.franchise,
        sellout.brand,
        sellout.variant,
        sellout.segment,
        sellout.put_up_description AS put_up,
        sellout.prod_sub_brand,
        sellout.prod_subsegment,
        sellout.prod_category,
        sellout.prod_subcategory,
        sellout.sap_sls_org,
        sellout.sap_prnt_cust_key,
        sellout.sap_prnt_cust_desc,
        sellout.sap_cust_chnl_key,
        sellout.sap_cust_chnl_desc,
        sellout.sap_cust_sub_chnl_key,
        sellout.sap_sub_chnl_desc,
        sellout.sap_go_to_mdl_key,
        sellout.go_to_model_description,
        sellout.sap_bnr_key,
        sellout.banner_description,
        sellout.sap_bnr_frmt_key,
        sellout.sap_bnr_frmt_desc,
        sellout.retail_env,
        sellout.customer_id,
        sellout.sap_customer_name,
        sellout.sap_sales_office_code,
        sellout.sap_sales_office_description,
        sellout.sap_sales_group_code,
        sellout.sap_sales_group_description,
        sum(sellout.gross_trade_sales) AS so_gross_trade_sales,
        sum(sellout.cn_damaged_goods) AS so_cn_damaged_goods,
        sum(sellout.credit_note_amount) AS so_credit_note_amount,
        sum(sellout.line_discount) AS so_line_discount,
        sum(sellout.bottom_line_discount) AS so_bottom_line_discount,
        sum(sellout.sales_quantity) AS so_sales_quantity,
        sum(sellout.return_quantity) AS so_return_quantity,
        sum(sellout.quantity_dz) AS so_quantity_dz,
        sum(sellout.net_invoice) AS so_net_invoice,
        sum(sellout.target_calls) AS so_target_calls,
        sum(sellout.target_sales) AS so_target_sales,
        0 AS si_base_value,
        0 AS si_sales_quantity,
        0 AS si_return_quantity,
        0 AS si_sales_less_return_quantity,
        0 AS si_gross_trade_sales_value,
        0 AS si_return_value,
        0 AS si_gross_trade_sales_less_return_value,
        0 AS si_tp_value,
        0 AS si_net_trade_sales_value,
        0 AS si_net_trade_sales_quantity
    from 
    (
        select 
            sales.bill_date AS order_date,
            sales."year",
            sales.qrtr AS year_quarter,
            sales.mnth_id AS month_year,
            sales.mnth_no AS month_number,
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
            case
                when (
                    substring((sellout_cust.sls_grp_cd)::text, 2, 1) = ('1'::character varying)::text
                ) then (
                    (sales.dstrbtr_grp_cd)::text || (' Van'::character varying)::text
                )
                else (
                    (sales.dstrbtr_grp_cd)::text || (' Credit'::character varying)::text
                )
            end as sales_office_name,
            sellout_cust.sls_grp_cd AS sales_group,
            sellout_cust.cust_grp_cd AS "cluster",
            sellout_cust.outlet_type_cd AS ar_type_code,
            sellout_cust.outlet_type_desc AS ar_type_name,
            sellin_cust.sap_cust_nm AS distributor_name,
            sellout_cust.sap_soldto_code AS customer_id,
            sellin_cust.sap_cust_id,
            sellin_cust.sap_cust_nm AS sap_customer_name,
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
            sellin_cust.sap_sls_office_cd AS sap_sales_office_code,
            sellin_cust.sap_sls_office_desc AS sap_sales_office_description,
            sellin_cust.sap_sls_grp_cd AS sap_sales_group_code,
            sellin_cust.sap_sls_grp_desc AS sap_sales_group_description,
            sellin_cust.sap_prnt_cust_key,
            sellin_cust.sap_prnt_cust_desc,
            sellin_cust.sap_cust_chnl_key,
            sellin_cust.sap_cust_chnl_desc,
            sellin_cust.sap_cust_sub_chnl_key,
            sellin_cust.sap_sub_chnl_desc,
            sellin_cust.sap_go_to_mdl_key,
            sellin_cust.go_to_model_description,
            sellin_cust.sap_bnr_key,
            sellin_cust.banner_description,
            sellin_cust.sap_bnr_frmt_key,
            sellin_cust.sap_bnr_frmt_desc,
            sellin_cust.retail_env,
            sales.dstrbtr_matl_num AS sku_code,
            matl.sap_mat_desc AS sku_description,
            matl.gph_prod_frnchse AS franchise,
            matl.gph_prod_brnd AS brand,
            matl.gph_prod_vrnt AS variant,
            matl.gph_prod_sgmnt AS segment,
            matl.gph_prod_put_up_desc AS put_up_description,
            matl.prod_sub_brand,
            matl.prod_subsegment,
            matl.prod_category,
            matl.prod_subcategory,
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
            target_sales.target AS target_sales
        from sales_2 as sales
        LEFT JOIN  sellout_cust 
        on (upper((sales.cust_cd)::text) = upper((sellout_cust.cust_cd)::text))
        AND (upper((sales.dstrbtr_grp_cd)::text) = upper((sellout_cust.dstrbtr_grp_cd)::text))  
        LEFT JOIN  target_distribution 
        on (upper((sales.dstrbtr_matl_num)::text) = upper((target_distribution.prod_cd)::text))
        AND (upper((sales.dstrbtr_grp_cd)::text) = upper((target_distribution.dstrbtr_id)::text))
        AND (substring(((sales.bill_date)::character varying)::text,1,4) 
        || substring(((sales.bill_date)::character varying)::text, 6,2)) 
        = (target_distribution.period)::text
        LEFT JOIN  target_sales on 
        ((sales.dstrbtr_grp_cd)::text = (target_sales.dstrbtr_id)::text)
        AND (
            substring(
            ((sales.bill_date)::character varying)::text,
            1,
            4
            ) || substring(
            ((sales.bill_date)::character varying)::text,
            6,
            2
            )
            ) = (target_sales.period)::text
        AND (upper((sellout_cust.sls_office_cd)::text) = upper((target_sales.sls_office)::text))
        AND (upper((sellout_cust.sls_grp_cd)::text) = upper((target_sales.sls_grp)::text))
        LEFT JOIN sellin_cust on 
        (upper((sellout_cust.sap_soldto_code)::text) = upper((sellin_cust.sap_cust_id)::text))
        LEFT JOIN  matl on 
        upper((sales.dstrbtr_matl_num)::text) = 
        upper(ltrim((matl.sap_matl_num)::text,('0'::character varying)::text))
            
    ) sellout
    GROUP BY 
        sellout."year",
        sellout.year_quarter,
        sellout.month_year,
        sellout.month_number,
        sellout.sku_code,
        sellout.sku_description,
        sellout.franchise,
        sellout.brand,
        sellout.variant,
        sellout.segment,
        sellout.put_up_description,
        sellout.prod_sub_brand,
        sellout.prod_subsegment,
        sellout.prod_category,
        sellout.prod_subcategory,
        sellout.sap_sls_org,
        sellout.sap_prnt_cust_key,
        sellout.sap_prnt_cust_desc,
        sellout.sap_cust_chnl_key,
        sellout.sap_cust_chnl_desc,
        sellout.sap_cust_sub_chnl_key,
        sellout.sap_sub_chnl_desc,
        sellout.sap_go_to_mdl_key,
        sellout.go_to_model_description,
        sellout.sap_bnr_key,
        sellout.banner_description,
        sellout.sap_bnr_frmt_key,
        sellout.sap_bnr_frmt_desc,
        sellout.retail_env,
        sellout.customer_id,
        sellout.sap_customer_name,
        sellout.sap_sales_office_code,
        sellout.sap_sales_office_description,
        sellout.sap_sales_group_code,
        sellout.sap_sales_group_description
),
combined as
(
    select * from union_1
    union all
    select * from union_2
),
final as
(
    select 
        data_type,
        year_jnj,
        year_quarter_jnj,
        year_month_jnj,
        month_number_jnj,
        item_code,
        item_description,
        franchise,
        brand,
        variant,
        segment,
        put_up,
        prod_sub_brand,
        prod_subsegment,
        prod_category,
        prod_subcategory,
        sap_sls_org,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_cust_chnl_key,
        sap_cust_chnl_desc,
        sap_cust_sub_chnl_key,
        sap_sub_chnl_desc,
        sap_go_to_mdl_key,
        go_to_model_description,
        sap_bnr_key,
        banner_description,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env,
        customer_id,
        sap_customer_name,
        sap_sales_office_code,
        sap_sales_office_description,
        sap_sales_group_code,
        sap_sales_group_description,
        so_gross_trade_sales,
        so_cn_damaged_goods,
        so_credit_note_amount,
        so_line_discount,
        so_bottom_line_discount,
        so_sales_quantity,
        so_return_quantity,
        so_quantity_dz,
        so_net_invoice,
        so_target_calls,
        so_target_sales,
        si_base_value,
        si_sales_quantity,
        si_return_quantity,
        si_sales_less_return_quantity,
        si_gross_trade_sales_value,
        si_return_value,
        si_gross_trade_sales_less_return_value,
        si_tp_value,
        si_net_trade_sales_value,
        si_net_trade_sales_quantity
    from combined 
)
select *  from final   
    