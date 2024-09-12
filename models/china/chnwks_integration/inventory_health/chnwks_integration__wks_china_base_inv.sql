with edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_code_descriptions as 
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
itg_parameter_reg_inventory as 
(
    select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
),
v_rpt_inventory_cube_new_inventory_analysis as 
(
    select * from {{ source('chnedw_integration', 'v_rpt_inventory_cube_new_inventory_analysis') }}
),
edw_customer_dim as 
(
    select * from {{ source('chnedw_integration', 'edw_customer_dim') }}
),
t_rpt_inventory_cube_sunny_wholesale_ygpf as 
(
    select * from {{ source('chnedw_integration', 't_rpt_inventory_cube_sunny_wholesale_ygpf') }}
),
edw_sales_mlm_fact as 
(
    select * from {{ source('chnedw_integration', 'edw_sales_mlm_fact') }}
),
v_rpt_pos_sales as 
(
    select * from {{ source('chnedw_integration', 'v_rpt_pos_sales') }}
),
cust_hier as (
  Select 
    distinct cust_num, 
    case when prnt_cust_key is null 
    or prnt_cust_key = '' then 'Not Assigned' else prnt_cust_key end as sap_prnt_cust_key, 
    case when CDDES_PCK.code_desc is null 
    or CDDES_PCK.code_desc = '' then 'Not Assigned' else CDDES_PCK.code_desc end as sap_prnt_cust_desc, 
    case when bnr_key is null 
    or bnr_key = '' then 'Not Assigned' else bnr_key end as sap_bnr_key, 
    case when CDDES_BNRKEY.code_desc is null 
    or CDDES_BNRKEY.code_desc = '' then 'Not Assigned' else CDDES_BNRKEY.code_desc end as bnr_desc, 
    sap_bnr_frmt_key, 
    SUB_CHNL_KEY 
  from 
    (
      Select 
        ltrim(cust_num, 0) cust_num, 
        prnt_cust_key, 
        bnr_key, 
        bnr_frmt_key as sap_bnr_frmt_key, 
        SUB_CHNL_KEY, 
        ROW_NUMBER() OVER (
          PARTITION BY LTRIM(CUST_NUM, 0) 
          ORDER BY 
            prnt_cust_key DESC, 
            DECODE(
              CUST_DEL_FLAG, NULL, 'O', '', 'O', CUST_DEL_FLAG
            ) ASC, 
            sls_org, 
            dstr_chnl
        ) AS rnk 
      from 
        EDW_CUSTOMER_SALES_DIM 
      where 
        SLS_ORG IN ('1500', '8888', '100A') 
        and nullif(prnt_cust_key, '') is not null
    ) b, 
    (
      Select 
        CODE, 
        CODE_DESC 
      from 
        EDW_CODE_DESCRIPTIONS 
      where 
        trim(
          Upper(CODE_TYPE)
        ) = 'PARENT CUSTOMER KEY'
    ) CDDES_PCK, 
    (
      Select 
        CODE, 
        CODE_DESC 
      from 
        EDW_CODE_DESCRIPTIONS 
      where 
        trim(
          Upper(CODE_TYPE)
        ) = 'BANNER KEY'
    ) CDDES_BNRKEY 
  where 
    CDDES_PCK.CODE(+) = b.PRNT_CUST_KEY 
    and CDDES_BNRKEY.CODE(+) = b.bnr_key 
    and b.rnk = 1
),
union1 as
(
    SELECT 
    year_month, 
    country_name AS country_name, 
    bu as bu, 
    sold_to, 
    nvl(
        nullif(product_code, ''), 
        'NA'
    ) AS matl_num, 
    nvl(
        SAP_PRNT_CUST_KEY, 'Not Assigned'
    ) AS SAP_PRNT_CUST_KEY, 
    nvl(SAP_BNR_KEY, 'Not Assigned') AS SAP_BNR_KEY, 
    nvl(SUB_CHNL_KEY, 'Not Assigned') AS SUB_CHNL_KEY, 
    SUM(SI_SLS_QTY) AS SI_SLS_QTY, 
    SUM(SI_GTS_VAL) AS SI_GTS_VAL, 
    SUM(CLOSING_STOCK) AS INVENTORY_QUANTITY, 
    SUM(CLOSING_STOCK_VAL) AS INVENTORY_VAL, 
    SUM(SO_QTY) AS SO_SLS_QTY, 
    SUM(SO_VAL) AS SO_TRD_SLS 
    FROM 
    (
        SELECT 
        CASE WHEN derived_table1.sold_to_code in (
            Select 
            parameter_value 
            from 
            itg_parameter_reg_inventory 
            where 
            country_name = 'China FTZ' 
            and parameter_name = 'FTZ_SOLD_TO'
        ) THEN 'China FTZ' :: CHARACTER VARYING ELSE 'China Domestic' :: CHARACTER VARYING END AS country_name, 
        'UNASSIGNED' AS parent_code, 
        derived_table1.payer_code, 
        derived_table1.sold_to_code AS sold_to, 
        derived_table1.customer_code, 
        derived_table1.product_code, 
        derived_table1.bu, 
        derived_table1.all_channel, 
        derived_table1.region, 
        derived_table1.area AS zone_or_area, 
        derived_table1.cal_year :: CHARACTER VARYING AS cal_year, 
        derived_table1.cal_mnth_id :: CHARACTER VARYING AS year_month, 
        SUM(derived_table1.si_sls_qty):: NUMERIC :: NUMERIC(18, 0):: NUMERIC(38, 2) AS si_sls_qty, 
        SUM(derived_table1.si_gts_val) AS si_gts_val, 
        SUM(derived_table1.closing_stock) AS closing_stock, 
        SUM(
            derived_table1.closing_stock_val
        ) AS closing_stock_val, 
        SUM(derived_table1.so_qty) AS so_qty, 
        SUM(derived_table1.so_val) AS so_val 
        FROM 
        (
            SELECT 
            "left"(
                t_rpt_inventory_cube_new.yyyymm :: CHARACTER VARYING :: TEXT, 
                4
            ) AS cal_year, 
            t_rpt_inventory_cube_new.yyyymm AS cal_mnth_id, 
            t_rpt_inventory_cube_new.payer_code, 
            t_rpt_inventory_cube_new.customer_code, 
            t_rpt_inventory_cube_new.sold_to_code, 
            t_rpt_inventory_cube_new.sku_code AS product_code, 
            t_rpt_inventory_cube_new.bu, 
            t_rpt_inventory_cube_new.all_channel, 
            t_rpt_inventory_cube_new.region, 
            t_rpt_inventory_cube_new.area, 
            t_rpt_inventory_cube_new.current_month_sellin_gts AS si_gts_val, 
            t_rpt_inventory_cube_new.current_month_sellin AS si_sls_qty, 
            t_rpt_inventory_cube_new.current_month_sellout_gts AS so_val, 
            t_rpt_inventory_cube_new.current_month_sellout AS so_qty, 
            t_rpt_inventory_cube_new.current_month_total_inventory_gts AS closing_stock_val, 
            t_rpt_inventory_cube_new.current_month_total_inventory AS closing_stock 
            FROM 
            v_rpt_inventory_cube_new_inventory_analysis as t_rpt_inventory_cube_new 
            WHERE 
            t_rpt_inventory_cube_new.sales_category_level1 NOT IN (
                Select 
                parameter_value 
                from 
                itg_parameter_reg_inventory 
                where 
                country_name = 'China' 
                and parameter_name = 'sales_category_level1'
            )
        ) derived_table1 
        GROUP BY 
        derived_table1.payer_code, 
        derived_table1.sold_to_code, 
        derived_table1.customer_code, 
        derived_table1.product_code, 
        derived_table1.bu, 
        derived_table1.all_channel, 
        derived_table1.region, 
        derived_table1.area, 
        derived_table1.cal_year, 
        derived_table1.cal_mnth_id
    ) a, 
    (
        select 
        distinct cust_num, 
        sap_prnt_cust_key, 
        sap_prnt_cust_desc, 
        sap_bnr_key, 
        bnr_desc, 
        SUB_CHNL_KEY 
        from 
        cust_hier 
        where 
        nvl(sap_prnt_cust_key, '#') not in('PC0004', 'PC0013', 'PC0014')
    ) b 
    where 
    b.cust_num(+) = a.sold_to --and  a.bu  in (Select parameter_value  from itg_parameter_reg_inventory where country_name='China' and parameter_name='BU')
    and a.all_channel not in (
        select 
        parameter_value 
        from 
        itg_parameter_reg_inventory 
        where 
        country_name = 'China' 
        and parameter_name = 'BU'
    ) 
    GROUP BY 
    year_month, 
    country_name, 
    bu, 
    sold_to, 
    nvl(
        nullif(product_code, ''), 
        'NA'
    ), 
    nvl(
        SAP_PRNT_CUST_KEY, 'Not Assigned'
    ), 
    nvl(SAP_BNR_KEY, 'Not Assigned'), 
    nvl(SUB_CHNL_KEY, 'Not Assigned')

),
union2 as
(
    SELECT 
    year_month, 
    country_name AS country_name, 
    bu as bu, 
    sold_to, 
    nvl(
        nullif(product_code, ''), 
        'NA'
    ) AS matl_num, 
    nvl(
        SAP_PRNT_CUST_KEY, 'Not Assigned'
    ) AS SAP_PRNT_CUST_KEY, 
    nvl(SAP_BNR_KEY, 'Not Assigned') AS SAP_BNR_KEY, 
    nvl(SUB_CHNL_KEY, 'Not Assigned') AS SUB_CHNL_KEY, 
    SUM(SI_SLS_QTY) AS SI_SLS_QTY, 
    SUM(SI_GTS_VAL) AS SI_GTS_VAL, 
    SUM(CLOSING_STOCK) AS INVENTORY_QUANTITY, 
    SUM(CLOSING_STOCK_VAL) AS INVENTORY_VAL, 
    SUM(SO_QTY) AS SO_SLS_QTY, 
    SUM(SO_VAL) AS SO_TRD_SLS 
    FROM 
    (
        SELECT 
        'China Domestic' AS country_name, 
        CASE WHEN GROUP_ACCOUNT = '屈臣氏' THEN CAST('PC0004' AS VARCHAR) WHEN GROUP_ACCOUNT = '沃尔玛' THEN CAST('PC0014' AS VARCHAR) WHEN GROUP_ACCOUNT = '乐购' THEN CAST('PC0013' AS VARCHAR) END AS WATSON_PARENT_CODE, 
        'UNASSIGNED' AS payer_code, 
        CASE WHEN GROUP_ACCOUNT = '屈臣氏' THEN 'PC0004' WHEN GROUP_ACCOUNT = '沃尔玛' THEN 'PC0014' WHEN GROUP_ACCOUNT = '乐购' THEN 'PC0013' END AS sold_to, 
        'UNASSIGNED' AS customer_code, 
        derived_table2.product_code, 
        derived_table2.group_account AS bu, 
        'UNASSIGNED' AS "region", 
        'UNASSIGNED' AS zone_or_area, 
        "left"(
            derived_table2.year_month :: CHARACTER VARYING :: TEXT, 
            4
        ):: CHARACTER VARYING AS cal_year, 
        derived_table2.year_month :: CHARACTER VARYING AS year_month, 
        SUM(derived_table2.si_qty) AS si_sls_qty, 
        SUM(derived_table2.si_gts) AS si_gts_val, 
        SUM(derived_table2.pos_inv_qty) AS closing_stock, 
        SUM(derived_table2.pos_inv_value) AS closing_stock_val, 
        SUM(derived_table2.pos_qty) AS so_qty, 
        SUM(derived_table2.pos_sales) AS so_val 
        FROM 
        (
            SELECT 
            a.yyyymm AS year_month, 
            a.product_code, 
            b.group_account, 
            SUM(a.si_gts) AS si_gts, 
            SUM(a.si_qty) AS si_qty, 
            0 AS pos_qty, 
            0 AS pos_sales, 
            0 AS pos_inv_qty, 
            0 AS pos_inv_value 
            FROM 
            edw_sales_mlm_fact a 
            JOIN edw_customer_dim b ON a.customer_code :: TEXT = b.customer_code :: TEXT 
            WHERE 
            NVL(GROUP_ACCOUNT, 'NA') IN ('屈臣氏') 
            GROUP BY 
            a.yyyymm, 
            a.product_code, 
            b.group_account ---- Tesco and Wal-Mart Sellin ------
            UNION ALL 
            SELECT 
            yyyymm, 
            sku_code AS product_code, 
            nka_name, 
            SUM(current_month_sellin_gts) AS si_gts, 
            SUM(current_month_sellin) AS si_qty, 
            0 AS pos_qty, 
            0 AS pos_sales, 
            0 AS pos_inv_qty, 
            0 AS pos_inv_value 
            FROM 
            t_rpt_inventory_cube_sunny_wholesale_ygpf 
            WHERE 
            customer_code NOT LIKE '%FTZ%' 
            AND NVL(nka_name, 'NA') IN ('沃尔玛', '乐购') 
            GROUP BY 
            yyyymm, 
            sku_code, 
            nka_name 
            UNION ALL 
            SELECT 
            v_rpt_pos_sales.month :: INTEGER AS "month", 
            v_rpt_pos_sales.p_code, 
            v_rpt_pos_sales.ka, 
            0 AS si_gts, 
            0 AS si_qty, 
            SUM(v_rpt_pos_sales.pos_qty) AS pos_qty, 
            SUM(v_rpt_pos_sales.pos_cost) AS pos_sales, 
            SUM(v_rpt_pos_sales.pos_inventory) AS pos_inv_qty, 
            SUM(
                v_rpt_pos_sales.pos_inventory_value_lastest
            ) AS pos_inv_value 
            FROM 
            v_rpt_pos_sales 
            WHERE 
            NVL(KA, 'NA') IN (
                '屈臣氏', '沃尔玛', '乐购'
            ) 
            AND LEFT("month", 4)>= (
                DATE_PART(YEAR, current_timestamp()) -6
            ) 
            GROUP BY 
            v_rpt_pos_sales.month, 
            v_rpt_pos_sales.p_code, 
            v_rpt_pos_sales.ka
        ) derived_table2 
        GROUP BY 
        derived_table2.product_code, 
        derived_table2.group_account, 
        "left"(
            derived_table2.year_month :: CHARACTER VARYING :: TEXT, 
            4
        ):: CHARACTER VARYING, 
        derived_table2.year_month
    ) a, 
    (
        select 
        distinct prnt_cust_key as cust_num, 
        prnt_cust_key as sap_prnt_cust_key, 
        sap_prnt_cust_desc, 
        bnr_key as sap_bnr_key, 
        bnr_desc, 
        SUB_CHNL_KEY 
        from 
        EDW_CUSTOMER_SALES_DIM b, 
        (
            Select 
            CODE, 
            CODE_DESC as sap_prnt_cust_desc 
            from 
            EDW_CODE_DESCRIPTIONS 
            where 
            trim(
                Upper(CODE_TYPE)
            ) = 'PARENT CUSTOMER KEY'
        ) CDDES_PCK, 
        (
            Select 
            CODE, 
            CODE_DESC as bnr_desc 
            from 
            EDW_CODE_DESCRIPTIONS 
            where 
            trim(
                Upper(CODE_TYPE)
            ) = 'BANNER KEY'
        ) CDDES_BNRKEY 
        where 
        SLS_ORG IN ('1500', '8888', '100A') 
        and NVL(prnt_cust_key, '#') IN('PC0004', 'PC0013', 'PC0014') --and upper(NVL(bnr_desc,'#')) IN ('WATSON\'S','HYMART','WAL-MART')
        and (
            upper(
            NVL(bnr_desc, '#')
            ) like 'WATSON%' 
            or upper(
            NVL(bnr_desc, '#')
            ) IN ('HYMART', 'WAL-MART')
        ) 
        and CDDES_PCK.CODE(+) = b.PRNT_CUST_KEY 
        and CDDES_BNRKEY.CODE(+) = b.bnr_key
    ) b 
    where 
    b.cust_num = a.sold_to --and  a.bu  in (Select parameter_value  from rg_itg.itg_parameter_reg_inventory where country_name='China' and parameter_name='BU')
    GROUP BY 
    year_month, 
    country_name, 
    bu, 
    sold_to, 
    nvl(
        nullif(product_code, ''), 
        'NA'
    ), 
    nvl(
        SAP_PRNT_CUST_KEY, 'Not Assigned'
    ), 
    nvl(SAP_BNR_KEY, 'Not Assigned'), 
    nvl(SUB_CHNL_KEY, 'Not Assigned')

),
final as (
    select * from union1
    UNION ALL
    select * from union2
)
select * from final