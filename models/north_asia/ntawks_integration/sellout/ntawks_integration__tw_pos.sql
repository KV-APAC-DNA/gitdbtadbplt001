{{ config(
    sql_header = "USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    ) 
}} 
with itg_pos as (
select * from {{ ref('ntaitg_integration__itg_pos') }}
),
edw_customer_attr_flat_dim as (
select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
itg_pos_cust_prod_cd_ean_map as (
select * from {{ ref('ntaitg_integration__itg_pos_cust_prod_cd_ean_map') }}
),
edw_product_attr_dim as (
select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
itg_pos_prom_prc_map as (
select * from {{ ref('ntaitg_integration__itg_pos_prom_prc_map') }}
),
edw_material_dim as (
select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_material_sales_dim as (
select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
itg_pos_cust_prod_to_sap_prod_map as (
select * from {{ source('ntaitg_integration','itg_pos_cust_prod_to_sap_prod_map') }}
),
itg_mds_hk_pos_product_mapping as (
select * from {{ ref('ntaitg_integration__itg_mds_hk_pos_product_mapping') }}
),
itg_query_parameters as (
    select * from {{ source('ntaitg_integration','itg_query_parameters') }}
),
tw_pos_1 as (
    SELECT src.pos_dt,
        src.vend_cd,
        src.vend_nm,
        src.prod_nm,
        CASE
            WHEN src.vend_prod_cd IS NULL
            OR src.vend_prod_cd = '' THEN '#'
            ELSE src.vend_prod_cd
        END AS vend_prod_cd,
        src.vend_prod_nm,
        src.brnd_nm,
        CASE
            WHEN src.barcode IS NULL
            OR src.barcode = '' THEN '#'
            ELSE src.barcode
        END AS ean_num,
        CASE
            WHEN src.str_num IS NULL
            OR src.str_num = '' THEN '#'
            ELSE src.str_num
        END AS str_cd,
        CASE
            WHEN src.str_nm IS NULL
            OR src.str_nm = '' THEN '#'
            ELSE src.str_nm
        END AS str_nm,
        CASE
            WHEN src.sold_to_party IS NULL
            OR src.sold_to_party = '' THEN '#'
            ELSE src.sold_to_party
        END AS sold_to_party,
        src.sls_grp,
        src.mysls_brnd_nm,
        src.sls_qty,
        src.sls_amt,
        src.unit_prc_amt,
        src.sls_excl_vat_amt,
        src.stk_rtrn_amt,
        src.stk_recv_amt,
        src.avg_sell_qty,
        src.cum_ship_qty,
        src.cum_rtrn_qty,
        src.web_ordr_takn_qty,
        src.web_ordr_acpt_qty,
        src.dc_invnt_qty,
        src.invnt_qty,
        src.invnt_amt,
        src.invnt_dt,
        src.serial_num,
        src.prod_delv_type,
        src.prod_type,
        src.dept_cd,
        src.dept_nm,
        src.spec_1_desc,
        src.spec_2_desc,
        src.cat_big,
        src.cat_mid,
        src.cat_small,
        src.dc_prod_cd,
        src.cust_dtls,
        src.dist_cd,
        src.crncy_cd,
        src.src_txn_sts,
        src.src_seq_num,
        src.src_sys_cd,
        src.ctry_cd,
        src.mysls_catg,
        src.sap_matl_num AS matl_num,
        e.matl_desc,
        src.prom_sls_amt,
        src.prom_prc_amt,
        'N' AS hist_flg,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS updt_dttm,
        null as channel,
        null as store_type,
        null as sls_grp_cd
    FROM 
        (
            SELECT x.*,
                COALESCE(d.prod_hier_l4, 'Others') AS mysls_brnd_nm,
                COALESCE(d.prod_hier_l3, 'Others') AS mysls_catg,
                d.sap_matl_num,
                COALESCE((x.sls_qty * e.prom_prc), 0) AS prom_sls_amt,
                COALESCE(e.prom_prc, CAST(0 AS NUMERIC(16, 5))) AS prom_prc_amt,
                x.str_cd AS str_num
            FROM 
                (
                    SELECT a.*,
                        qp.sold_to_party,
                        CASE
                            WHEN a.src_sys_cd LIKE '7-11'
                            OR a.src_sys_cd LIKE 'A-Mart%'
                            OR a.src_sys_cd LIKE 'Cosmed%'
                            OR a.src_sys_cd LIKE 'EC'
                            OR a.src_sys_cd LIKE 'Watsons%' THEN CAST(g.barcd AS VARCHAR(100))
                            ELSE CAST(a.ean_num AS VARCHAR(100))
                        END AS barcode,
                        CASE
                            WHEN c.sls_grp IS NULL
                            OR c.sls_grp = '' THEN a.src_sys_cd
                            ELSE c.sls_grp
                        END AS sls_grp
                    FROM itg_pos a
                        LEFT JOIN (
                            SELECT DISTINCT sls_grp,
                                cust_store_ref,
                                sold_to_party
                            FROM edw_customer_attr_flat_dim
                            WHERE cust_store_ref IS NOT NULL
                                AND cust_store_ref <> ''
                        ) c ON c.cust_store_ref = a.str_cd
                        AND c.sls_grp = a.src_sys_cd
                        LEFT JOIN (
                            SELECT CASE
                                    WHEN cust_nm = 'ibonMart' THEN 'ibonMart'
                                    WHEN cust_nm = 'EC' THEN 'EC'
                                    WHEN cust_nm = '7-11' THEN '7-11'
                                    WHEN cust_nm = 'Carrefour' THEN 'Carrefour 家樂福'
                                    WHEN cust_nm = 'Cosmed' THEN 'Cosmed 康是美'
                                    WHEN cust_nm = 'PX-Civilian' THEN 'PX 全聯'
                                    WHEN cust_nm = 'A-Mart' THEN 'A-Mart 愛買'
                                    WHEN cust_nm = 'Poya' THEN 'Poya 寶雅'
                                    WHEN cust_nm = 'Watsons' THEN 'Watsons 屈臣氏'
                                    WHEN cust_nm = 'RT-Mart' THEN 'RT-Mart 大潤發'
                                END AS cust_nm,
                                rtrim(cust_prod_cd) as cust_prod_cd,
                                MIN(barcd) AS barcd
                            FROM itg_pos_cust_prod_cd_ean_map
                            GROUP BY rtrim(cust_prod_cd),
                                cust_nm
                        ) g ON rtrim(a.vend_prod_cd) = rtrim(g.cust_prod_cd)
                        and a.src_sys_cd = g.cust_nm
                        left join (
                            select distinct country_code,
                                parameter_name as src_sys_cd,
                                parameter_value as sold_to_party
                            from itg_query_parameters
                            where country_code = 'TW'
                                and parameter_type = 'sold_to_party'
                        ) qp on qp.src_sys_cd = a.src_sys_cd
                ) x
                LEFT JOIN (
                    SELECT DISTINCT ean,
                        sap_matl_num,
                        prod_hier_l4,
                        prod_hier_l3,
                        cntry
                    FROM edw_product_attr_dim
                ) d ON rtrim(CAST (x.barcode AS VARCHAR (40))) = rtrim(CAST (d.ean AS VARCHAR (40)))
                AND x.ctry_cd = d.cntry
                LEFT JOIN (
                    select CASE
                            WHEN cust = 'ibonMart' THEN 'ibonMart'
                            WHEN cust = 'EC' THEN 'EC'
                            WHEN cust = '7-11' THEN '7-11'
                            WHEN cust = 'Carrefour' THEN 'Carrefour 家樂福'
                            WHEN cust = 'Cosmed' THEN 'Cosmed 康是美'
                            WHEN cust = 'PX-Civilian' THEN 'PX 全聯'
                            WHEN cust = 'A-Mart' THEN 'A-Mart 愛買'
                            WHEN cust = 'Poya' THEN 'Poya 寶雅'
                            WHEN cust = 'Watsons' THEN 'Watsons 屈臣氏'
                            WHEN cust = 'RT-Mart' THEN 'RT-Mart 大潤發'
                        END AS cust,
                        barcd,
                        cust_prod_cd,
                        prom_prc,
                        prom_strt_dt,
                        prom_end_dt
                    from itg_pos_prom_prc_map
                ) e ON x.pos_dt BETWEEN e.prom_strt_dt AND e.prom_end_dt
                AND rtrim(x.barcode) = rtrim(e.barcd)
                AND x.src_sys_cd = e.cust
                And rtrim(x.vend_prod_cd) = rtrim(e.cust_prod_cd)
            WHERE x.ctry_cd = 'TW' and src_sys_cd in ('Carrefour 家樂福','Cosmed 康是美','EC','Poya 寶雅')
        ) src
        LEFT JOIN (
            SELECT DISTINCT matl_num,
                matl_desc
            FROM edw_material_dim
        ) e ON COALESCE (LTRIM (e.matl_num, 0), '#') = LTRIM (COALESCE (src.sap_matl_num, '#'), 0)
),
tw_pos_2 as (
    SELECT src.pos_dt,
        src.vend_cd,
        src.vend_nm,
        src.prod_nm,
        CASE
            WHEN src.vend_prod_cd IS NULL
            OR src.vend_prod_cd = '' THEN '#'
            ELSE src.vend_prod_cd
        END AS vend_prod_cd,
        src.vend_prod_nm,
        src.brnd_nm,
        CASE
            WHEN src.barcode IS NULL
            OR src.barcode = '' THEN '#'
            ELSE src.barcode
        END AS ean_num,
        CASE
            WHEN src.str_num IS NULL
            OR src.str_num = '' THEN '#'
            ELSE src.str_num
        END AS str_cd,
        CASE
            WHEN src.str_nm IS NULL
            OR src.str_nm = '' THEN '#'
            ELSE src.str_nm
        END AS str_nm,
        CASE
            WHEN src.sold_to_party IS NULL
            OR src.sold_to_party = '' THEN '#'
            ELSE src.sold_to_party
        END AS sold_to_party,
        src.sls_grp,
        src.mysls_brnd_nm,
        src.sls_qty,
        src.sls_amt,
        src.unit_prc_amt,
        src.sls_excl_vat_amt,
        src.stk_rtrn_amt,
        src.stk_recv_amt,
        src.avg_sell_qty,
        src.cum_ship_qty,
        src.cum_rtrn_qty,
        src.web_ordr_takn_qty,
        src.web_ordr_acpt_qty,
        src.dc_invnt_qty,
        src.invnt_qty,
        src.invnt_amt,
        src.invnt_dt,
        src.serial_num,
        src.prod_delv_type,
        src.prod_type,
        src.dept_cd,
        src.dept_nm,
        src.spec_1_desc,
        src.spec_2_desc,
        src.cat_big,
        src.cat_mid,
        src.cat_small,
        src.dc_prod_cd,
        src.cust_dtls,
        src.dist_cd,
        src.crncy_cd,
        src.src_txn_sts,
        src.src_seq_num,
        src.src_sys_cd,
        src.ctry_cd,
        src.mysls_catg,
        src.sap_matl_num AS matl_num,
        e.matl_desc,
        src.prom_sls_amt,
        src.prom_prc_amt,
        'N' AS hist_flg,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS updt_dttm,
        null as channel,
        null as store_type,
        null as sls_grp_cd
    FROM 
        (
            SELECT x.*,
                COALESCE(d.prod_hier_l4, 'Others') AS mysls_brnd_nm,
                COALESCE(d.prod_hier_l3, 'Others') AS mysls_catg,
                d.sap_matl_num,
                COALESCE((x.sls_qty * e.prom_prc), 0) AS prom_sls_amt,
                COALESCE(e.prom_prc, CAST(0 AS NUMERIC(16, 5))) AS prom_prc_amt,
                x.str_cd AS str_num
            FROM 
                (
                    SELECT a.*,
                        c.sold_to_party,
                        CASE
                            WHEN a.src_sys_cd LIKE '7-11'
                            OR a.src_sys_cd LIKE 'A-Mart%'
                            OR a.src_sys_cd LIKE 'Cosmed%'
                            OR a.src_sys_cd LIKE 'EC'
                            OR a.src_sys_cd LIKE 'Watsons%' THEN CAST(g.barcd AS VARCHAR(100))
                            ELSE CAST(a.ean_num AS VARCHAR(100))
                        END AS barcode,
                        CASE
                            WHEN c.sls_grp IS NULL
                            OR c.sls_grp = '' THEN a.src_sys_cd
                            ELSE c.sls_grp
                        END AS sls_grp
                    FROM itg_pos a
                        LEFT JOIN (
                            SELECT DISTINCT sls_grp,
                                cust_store_ref,
                                sold_to_party
                            FROM edw_customer_attr_flat_dim
                            WHERE cust_store_ref IS NOT NULL
                                AND cust_store_ref <> ''
                        ) c ON c.cust_store_ref = a.str_cd
                        AND c.sls_grp = a.src_sys_cd
                        LEFT JOIN (
                            SELECT CASE
                                    WHEN cust_nm = 'ibonMart' THEN 'ibonMart'
                                    WHEN cust_nm = 'EC' THEN 'EC'
                                    WHEN cust_nm = '7-11' THEN '7-11'
                                    WHEN cust_nm = 'Carrefour' THEN 'Carrefour 家樂福'
                                    WHEN cust_nm = 'Cosmed' THEN 'Cosmed 康是美'
                                    WHEN cust_nm = 'PX-Civilian' THEN 'PX 全聯'
                                    WHEN cust_nm = 'A-Mart' THEN 'A-Mart 愛買'
                                    WHEN cust_nm = 'Poya' THEN 'Poya 寶雅'
                                    WHEN cust_nm = 'Watsons' THEN 'Watsons 屈臣氏'
                                    WHEN cust_nm = 'RT-Mart' THEN 'RT-Mart 大潤發'
                                END AS cust_nm,
                                rtrim(cust_prod_cd) as cust_prod_cd,
                                MIN(barcd) AS barcd
                            FROM itg_pos_cust_prod_cd_ean_map
                            GROUP BY rtrim(cust_prod_cd),
                                cust_nm
                        ) g ON rtrim(a.vend_prod_cd) = rtrim(g.cust_prod_cd)
                        and a.src_sys_cd = g.cust_nm
                ) x
                LEFT JOIN (
                    SELECT DISTINCT ean,
                        sap_matl_num,
                        prod_hier_l4,
                        prod_hier_l3,
                        cntry
                    FROM edw_product_attr_dim
                ) d ON rtrim(CAST (x.barcode AS VARCHAR (40))) = rtrim(CAST (d.ean AS VARCHAR (40)))
                AND x.ctry_cd = d.cntry
                LEFT JOIN (
                    select CASE
                            WHEN cust = 'ibonMart' THEN 'ibonMart'
                            WHEN cust = 'EC' THEN 'EC'
                            WHEN cust = '7-11' THEN '7-11'
                            WHEN cust = 'Carrefour' THEN 'Carrefour 家樂福'
                            WHEN cust = 'Cosmed' THEN 'Cosmed 康是美'
                            WHEN cust = 'PX-Civilian' THEN 'PX 全聯'
                            WHEN cust = 'A-Mart' THEN 'A-Mart 愛買'
                            WHEN cust = 'Poya' THEN 'Poya 寶雅'
                            WHEN cust = 'Watsons' THEN 'Watsons 屈臣氏'
                            WHEN cust = 'RT-Mart' THEN 'RT-Mart 大潤發'
                        END AS cust,
                        barcd,
                        cust_prod_cd,
                        prom_prc,
                        prom_strt_dt,
                        prom_end_dt
                    from itg_pos_prom_prc_map
                ) e ON x.pos_dt BETWEEN e.prom_strt_dt AND e.prom_end_dt
                AND rtrim(x.barcode) = rtrim(e.barcd)
                AND x.src_sys_cd = e.cust
                And rtrim(x.vend_prod_cd) = rtrim(e.cust_prod_cd)
            WHERE x.ctry_cd = 'TW'
                and src_sys_cd in ('7-11', 'PX 全聯', 'RT-Mart 大潤發','ibonMart')
        ) src
        LEFT JOIN (
            SELECT DISTINCT matl_num,
                matl_desc
            FROM edw_material_dim
        ) e ON COALESCE (LTRIM (e.matl_num, 0), '#') = LTRIM (COALESCE (src.sap_matl_num, '#'), 0)
),
tw_pos_3 as (
    SELECT src.pos_dt,
        src.vend_cd,
        src.vend_nm,
        src.prod_nm,
        CASE
            WHEN src.vend_prod_cd IS NULL
            OR src.vend_prod_cd = '' THEN '#'
            ELSE src.vend_prod_cd
        END AS vend_prod_cd,
        src.vend_prod_nm,
        src.brnd_nm,
        CASE
            WHEN src.barcode IS NULL
            OR src.barcode = '' THEN '#'
            ELSE src.barcode
        END AS ean_num,
        CASE
            WHEN src.str_num IS NULL
            OR src.str_num = '' THEN '#'
            ELSE src.str_num
        END AS str_cd,
        CASE
            WHEN src.str_nm IS NULL
            OR src.str_nm = '' THEN '#'
            ELSE src.str_nm
        END AS str_nm,
        CASE
            WHEN src.sold_to_party IS NULL
            OR src.sold_to_party = '' THEN '#'
            ELSE src.sold_to_party
        END AS sold_to_party,
        src.sls_grp,
        src.mysls_brnd_nm,
        src.sls_qty,
        src.sls_amt,
        src.unit_prc_amt,
        src.sls_excl_vat_amt,
        src.stk_rtrn_amt,
        src.stk_recv_amt,
        src.avg_sell_qty,
        src.cum_ship_qty,
        src.cum_rtrn_qty,
        src.web_ordr_takn_qty,
        src.web_ordr_acpt_qty,
        src.dc_invnt_qty,
        src.invnt_qty,
        src.invnt_amt,
        src.invnt_dt,
        src.serial_num,
        src.prod_delv_type,
        src.prod_type,
        src.dept_cd,
        src.dept_nm,
        src.spec_1_desc,
        src.spec_2_desc,
        src.cat_big,
        src.cat_mid,
        src.cat_small,
        src.dc_prod_cd,
        src.cust_dtls,
        src.dist_cd,
        src.crncy_cd,
        src.src_txn_sts,
        src.src_seq_num,
        src.src_sys_cd,
        src.ctry_cd,
        src.mysls_catg,
        src.sap_matl_num AS matl_num,
        e.matl_desc,
        src.prom_sls_amt,
        src.prom_prc_amt,
        'N' AS hist_flg,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS updt_dttm,
        null as channel,
        null as store_type,
        null as sls_grp_cd
    FROM 
        (
            SELECT x.*,
                COALESCE(d.prod_hier_l4, 'Others') AS mysls_brnd_nm,
                COALESCE(d.prod_hier_l3, 'Others') AS mysls_catg,
                d.sap_matl_num,
                COALESCE((x.sls_qty * e.prom_prc), 0) AS prom_sls_amt,
                COALESCE(e.prom_prc, CAST(0 AS NUMERIC(16, 5))) AS prom_prc_amt,
                x.str_cd AS str_num
            FROM (
                    SELECT a.*,
                        qp.sold_to_party,
                        CASE
                            WHEN a.src_sys_cd LIKE '7-11'
                            OR a.src_sys_cd LIKE 'A-Mart%'
                            OR a.src_sys_cd LIKE 'Cosmed%'
                            OR a.src_sys_cd LIKE 'EC'
                            OR a.src_sys_cd LIKE 'Watsons%' THEN CAST(g.barcd AS VARCHAR(100))
                            ELSE CAST(a.ean_num AS VARCHAR(100))
                        END AS barcode,
                        CASE
                            WHEN c.sls_grp IS NULL
                            OR c.sls_grp = '' THEN a.src_sys_cd
                            ELSE c.sls_grp
                        END AS sls_grp
                    FROM (
                            SELECT *
                            FROM itg_pos
                            WHERE LENGTH(str_cd) > 0
                        ) a
                        LEFT JOIN (
                            SELECT DISTINCT sls_grp,
                                cust_store_ref,
                                sold_to_party
                            FROM edw_customer_attr_flat_dim
                            WHERE cust_store_ref IS NOT NULL
                                AND cust_store_ref <> ''
                        ) c ON c.cust_store_ref = a.str_cd
                        AND c.sls_grp = a.src_sys_cd
                        LEFT JOIN (
                            SELECT para.parameter_value AS cust_nm,
                                rtrim(ean_map.cust_prod_cd) as cust_prod_cd,
                                MIN(ean_map.barcd) AS barcd
                            FROM itg_pos_cust_prod_cd_ean_map ean_map
                                Left Join (
                                    select *
                                    from itg_query_parameters
                                    where country_code = 'TW'
                                        and parameter_type = 'cust_nm'
                                ) para on ean_map.cust_nm = para.parameter_name
                            GROUP BY rtrim(ean_map.cust_prod_cd),
                                para.parameter_value
                        ) g ON rtrim(a.vend_prod_cd) = rtrim(g.cust_prod_cd)
                        AND a.src_sys_cd = g.cust_nm
                        left join (
                            select distinct country_code,
                                parameter_name as src_sys_cd,
                                parameter_value as sold_to_party
                            from itg_query_parameters
                            where country_code = 'TW'
                                and parameter_type = 'sold_to_party'
                        ) qp on qp.src_sys_cd = a.src_sys_cd
                ) x
                LEFT JOIN (
                    SELECT DISTINCT ean,
                        sap_matl_num,
                        prod_hier_l4,
                        prod_hier_l3,
                        cntry
                    FROM edw_product_attr_dim
                ) d ON rtrim(CAST (x.barcode AS VARCHAR (40))) = rtrim(CAST (d.ean AS VARCHAR (40)))
                AND x.ctry_cd = d.cntry
                LEFT JOIN (
                    SELECT para.parameter_value AS cust,
                        barcd,
                        cust_prod_cd,
                        prom_prc,
                        prom_strt_dt,
                        prom_end_dt
                    FROM itg_pos_prom_prc_map prc_map
                        Left Join (
                            select *
                            from itg_query_parameters
                            where country_code = 'TW'
                                and parameter_type = 'cust_nm'
                        ) para on prc_map.cust = para.parameter_name
                ) e ON x.pos_dt BETWEEN e.prom_strt_dt AND e.prom_end_dt
                AND rtrim(x.barcode) = rtrim(e.barcd)
                AND x.src_sys_cd = e.cust
                AND rtrim(x.vend_prod_cd) = rtrim(e.cust_prod_cd)
            WHERE x.ctry_cd = 'TW'
                AND x.src_sys_cd = (
                    SELECT parameter_value
                    FROM itg_query_parameters
                    WHERE country_code = 'TW'
                        AND parameter_type = 'cust_nm'
                        AND parameter_name = 'Watsons'
                )
        ) src
        LEFT JOIN (
            SELECT DISTINCT matl_num,
                matl_desc
            FROM edw_material_dim
        ) e ON COALESCE (LTRIM (e.matl_num, 0), '#') = LTRIM (COALESCE (src.sap_matl_num, '#'), 0)
),
final as
(
    select * from tw_pos_1
    union all
    select * from tw_pos_2
    union all
    select * from tw_pos_3
)
select * from final
