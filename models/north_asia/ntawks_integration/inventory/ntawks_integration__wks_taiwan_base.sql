WITH v_rpt_ims_inventory
AS (
    SELECT *
    FROM {{ref('ntaedw_integration__v_rpt_ims_inventory')}}
    ),
edw_billing_fact
AS (
    SELECT *
    FROM {{ref('aspedw_integration__edw_billing_fact')}}
    ),
edw_material_sales_dim
AS (
    SELECT *
    FROM {{ref('aspedw_integration__edw_material_sales_dim')}}
    ),
v_rpt_dly_plan_ims_enrich
AS (
    SELECT *
    FROM {{ref('ntaedw_integration__v_rpt_dly_plan_ims_enrich')}}
    ),
edw_pos_fact
AS (
    SELECT *
    FROM {{ref('ntaedw_integration__edw_pos_fact')}}
    ),
EDW_CUSTOMER_SALES_DIM
AS (
    SELECT *
    FROM {{ref('aspedw_integration__edw_customer_sales_dim')}}
    ),
itg_parameter_reg_inventory
AS (
    SELECT *
    FROM {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }} 
    ),
edw_vw_tw_pos_inventory
AS (
    SELECT *
    FROM {{ref('mysedw_integration__edw_vw_my_pos_inventory')}}
    ),
EDW_VW_OS_TIME_DIM
AS (
    SELECT *
    FROM {{ref('sgpedw_integration__edw_vw_os_time_dim')}}
    ),
itg_pos_prom_prc_map
AS (
    SELECT *
    FROM {{ref('ntaitg_integration__itg_pos_prom_prc_map')}}
    ),
EDW_CODE_DESCRIPTIONS
AS (
    SELECT *
    FROM {{ref('aspedw_integration__edw_code_descriptions')}}
    ),
inv_max
AS (
    SELECT dstr_cd,
        CASE 
            WHEN dstr_cd = '134478'
                THEN dstr_nm
            ELSE 'NA'
            END dstr_nm,
        TO_CHAR(invnt_dt, 'yyyymm') cal_month,
        MAX(invnt_dt) max_invnt_dt
    FROM v_rpt_ims_inventory
    WHERE ctry_nm = 'Taiwan'
        AND non_sellable_product = 'sellable products'
        AND from_crncy = 'TWD'
        AND to_crncy = 'TWD'
        AND LEFT(invnt_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
    GROUP BY dstr_cd,
        CASE 
            WHEN dstr_cd = '134478'
                THEN dstr_nm
            ELSE 'NA'
            END,
        TO_CHAR(invnt_dt, 'yyyymm')
    ),
inv_join
AS (
    SELECT 'TW' ctry_cd,
        'INV' Data_Type,
        inv.dstr_cd AS dstr_cd,
        CASE 
            WHEN inv.dstr_cd = '134478'
                THEN inv.dstr_nm
            ELSE 'NA'
            END AS dstr_nm,
        inv.prod_cd,
        inv.ean_num AS ean_num,
        TO_CHAR(invnt_dt, 'yyyymm') AS cal_month,
        invnt_qty,
        (invnt_qty * sell_in_price_manual) AS inv_value
    FROM v_rpt_ims_inventory inv
    JOIN inv_max ON rtrim(inv.dstr_cd) = rtrim(inv_max.dstr_cd)
        AND rtrim(inv_max.dstr_nm) = rtrim(CASE 
                WHEN inv.dstr_cd = '134478'
                    THEN inv.dstr_nm
                ELSE 'NA'
                END)
        AND rtrim(TO_CHAR(invnt_dt, 'yyyymm')) = rtrim(inv_max.cal_month)
        AND rtrim(inv.invnt_dt) = rtrim(inv_max.max_invnt_dt)
    ),
t1
AS (
    SELECT invv.ctry_cd,
        invv.Data_Type,
        invv.dstr_cd AS dstr_cd,
        invv.ean_num AS ean_num,
        invv.cal_month,
        SUM(invnt_qty) inv_qty,
        sum(inv_value) inv_value,
        0 AS so_qty,
        0 AS sls_value,
        0 AS si_qty,
        0 AS si_value
    FROM inv_join invv
    GROUP BY invv.ctry_cd,
        invv.Data_Type,
        invv.dstr_cd,
        -- prod_cd,
        invv.ean_num,
        invv.cal_month
    ),
t2
AS (
    SELECT 'TW' cntry_cd,
        'SELLIN',
        ltrim(cust_sls, '0') AS distributor_code,
        msd.ean_num,
        to_char(bill_dt, 'yyyymm') AS cal_month,
        0 AS inv_qty,
        0 AS inv_value,
        0 AS sls_qty,
        0 AS sls_value,
        sum(bill_qty) AS sell_in_qty,
        sum(netval_inv) AS sell_in_value
    FROM edw_billing_fact bill
    LEFT JOIN edw_material_sales_dim msd ON rtrim(bill.sls_org) = rtrim(msd.sls_org)
        AND LTRIM(msd.matl_num, '0') = LTRIM(bill.material, '0')
        AND rtrim(bill.distr_chnl) = rtrim(msd.dstr_chnl)
    WHERE bill.sls_org = '1200'
        AND left(bill.bill_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
        AND bill_type IN ('ZF2T', 'ZG2T')
        AND msd.ean_num != ''
    GROUP BY LTRIM(cust_sls, '0'),
        ean_num,
        to_char(bill_dt, 'yyyymm')
    ),
t3
AS (
    SELECT ctry_cd,
        Data_Type,
        dstr_cd,
        ean_num,
        cal_month,
        0 AS inv_qty,
        0 AS inv_value,
        SUM(sls_qty) so_qty,
        SUM(sls_value) AS sls_value,
        0 AS sell_in_qty,
        0 AS sell_in_value
    FROM (
        SELECT 'TW' AS ctry_cd,
            'SELLOUT' AS Data_Type,
            txn.dstr_cd,
            prod_cd,
            ean_num,
            TO_CHAR(ims_txn_dt, 'yyyymm') cal_month,
            (sls_qty) - (rtrn_qty) sls_qty,
            ((sls_qty - rtrn_qty) * sell_in_price_manual) sls_value
        FROM v_rpt_dly_plan_ims_enrich txn
        WHERE (
                CASE 
                    WHEN txn.prod_cd LIKE '1U%'
                        OR txn.prod_cd LIKE 'COUNTER TOP%'
                        OR txn.prod_cd IS NULL
                        OR txn.prod_cd = ''
                        OR txn.prod_cd LIKE 'DUMPBIN%'
                        THEN 'non sellable products'
                    ELSE 'sellable products'
                    END = 'sellable products'
                )
            AND ctry_cd = 'TW'
            AND txn.dstr_cd <> '134478' ---- changed as PX data needs to be taken from POS
            AND LEFT(ims_txn_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
            AND to_crncy = 'TWD'
        )
    GROUP BY ctry_cd,
        Data_Type,
        dstr_cd,
        ean_num,
        cal_month
    ),
t4
AS (
    SELECT ctry_cd,
        Data_Type,
        cust_num AS dstr_cd,
        ean_num,
        cal_month,
        0 AS inv_qty,
        0 AS inv_value,
        cast(sum(sls_qty) AS NUMERIC(38, 4)) AS so_qty,
        cast(SUM(sls_value) AS NUMERIC(38, 4)) AS sls_value,
        0 AS sell_in_qty,
        0 AS sell_in_value
    FROM (
        SELECT 'TW' AS ctry_cd,
            'OFFTAKE' AS Data_Type,
            ean_num,
            TO_CHAR(pos_dt, 'yyyymm') cal_month,
            sls_qty AS sls_qty,
            (sls_qty * price.prom_prc) AS sls_value
        FROM edw_pos_fact txn,
            (
                SELECT CASE 
                        WHEN cust = 'ibonMart'
                            THEN 'ibonMart'
                        WHEN cust = 'EC'
                            THEN 'EC'
                        WHEN cust = '7-11'
                            THEN '7-11'
                        WHEN cust = 'Carrefour'
                            THEN 'Carrefour 家樂福'
                        WHEN cust = 'Cosmed'
                            THEN 'Cosmed 康是美'
                        WHEN cust = 'PX-Civilian'
                            THEN 'PX 全聯'
                        WHEN cust = 'A-Mart'
                            THEN 'A-Mart 愛買'
                        WHEN cust = 'Poya'
                            THEN 'Poya 寶雅'
                        WHEN cust = 'Watsons'
                            THEN 'Watsons 屈臣氏'
                        WHEN cust = 'RT-Mart'
                            THEN 'RT-Mart 大潤發'
                        END AS cust,
                    barcd,
                    cust_prod_cd,
                    prom_prc,
                    prom_strt_dt,
                    prom_end_dt
                FROM itg_pos_prom_prc_map
                ) price
        WHERE src_sys_cd = 'Watsons 屈臣氏'
            AND ctry_cd = 'TW'
            AND LEFT(pos_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
            AND crncy_cd = 'TWD'
            AND txn.pos_dt BETWEEN price.prom_strt_dt
                AND price.prom_end_dt
            AND txn.ean_num = price.barcd(+)
            AND txn.src_sys_cd = price.cust(+)
            AND txn.vend_prod_cd = price.cust_prod_cd(+)
        ) AS pos,
        (
            SELECT DISTINCT MIN(nvl(LTRIM(cust_num, '0'), '#')) AS cust_num
            FROM EDW_CUSTOMER_SALES_DIM
            WHERE sls_org = '1200'
                AND prnt_cust_key = (
                    SELECT DISTINCT parameter_value
                    FROM itg_parameter_reg_inventory
                    WHERE country_name = 'TAIWAN'
                        AND parameter_name = 'AS_WATSONS_PRNT_CUST_KEY'
                    )
            ) AS CUST
    GROUP BY ctry_cd,
        Data_Type,
        cust_num,
        ean_num,
        cal_month
    ),
t5
AS (
    SELECT 'TW' AS ctry_cd,
        'OFFTAKE' AS Data_Type,
        cust_num AS dstr_cd,
        ean_num,
        TO_CHAR(pos_dt, 'yyyymm') AS cal_month,
        0 AS inv_qty,
        0 AS inv_value,
        cast(sum(sls_qty) AS NUMERIC(38, 4)) AS so_qty,
        cast(SUM(prom_sls_amt) AS NUMERIC(38, 4)) AS sls_value,
        0 AS sell_in_qty,
        0 AS sell_in_value
    FROM edw_pos_fact txn,
        (
            SELECT DISTINCT max(nvl(LTRIM(cust_num, '0'), '#')) AS cust_num
            FROM EDW_CUSTOMER_SALES_DIM
            WHERE sls_org = '1200'
                AND prnt_cust_key = (
                    SELECT DISTINCT parameter_value
                    FROM itg_parameter_reg_inventory
                    WHERE country_name = 'TAIWAN'
                        AND parameter_name = 'PX_PRNT_CUST_KEY'
                    )
            ) AS CUST
    WHERE src_sys_cd = 'PX 全聯'
        AND ctry_cd = 'TW'
        AND LEFT(pos_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
        AND crncy_cd = 'TWD'
    GROUP BY cust_num,
        ean_num,
        cal_month
    ),
t6
AS (
    SELECT ctry_cd,
        Data_Type,
        LTRIM(CUST.cust_num, '0') AS dstr_cd,
        --null as dstr_cd,
        ean_num AS ean_num,
        CAST(cal_mnth_id AS VARCHAR) AS cal_month,
        CAST(SUM(total_stock_qty) AS NUMERIC(38, 4)) AS inv_qty,
        CAST(SUM(total_stock_qty * price.prom_prc) AS NUMERIC(38, 4)) AS inv_value,
        0 AS so_qty,
        0 AS sls_value,
        0 AS si_qty,
        0 AS si_value
    FROM (
        SELECT 'TW' ctry_cd,
            'INV' Data_Type,
            ean_num,
            item_cd,
            YEAR,
            INV.mnth_id,
            inv.inv_week,
            YEAR || '-' || SUBSTRING(INV.mnth_id, 5, 2) || '-01' AS inv_dt,
            end_stock_qty AS total_stock_qty
        FROM edw_vw_tw_pos_inventory inv,
            (
                SELECT mnth_id,
                    MAX(inv_week) AS max_week_no
                FROM edw_vw_tw_pos_inventory
                GROUP BY mnth_id
                ) max_week
        WHERE LTRIM(inv.inv_week, '0') = LTRIM(max_week.max_week_no, '0')
            AND inv.mnth_id = max_week.mnth_id
        ) inv,
        (
            SELECT cal_year,
                cal_mnth_id,
                wk,
                cal_date_id
            FROM EDW_VW_OS_TIME_DIM
            GROUP BY cal_year,
                cal_mnth_id,
                wk,
                cal_date_id
            ) AS td,
        (
            SELECT MIN(nvl(LTRIM(cust_num, '0'), '#')) AS cust_num
            FROM EDW_CUSTOMER_SALES_DIM
            WHERE sls_org = '1200'
                AND prnt_cust_key = (
                    SELECT DISTINCT parameter_value
                    FROM itg_parameter_reg_inventory
                    WHERE country_name = 'TAIWAN'
                        AND parameter_name = 'AS_WATSONS_PRNT_CUST_KEY'
                    )
            ) AS CUST,
        (
            SELECT *
            FROM itg_pos_prom_prc_map
            ) price
    WHERE inv.year >= (DATE_PART(YEAR, SYSDATE()) - 6)
        AND INV.mnth_id + 15 = TD.cal_date_id
        --AND   LTRIM(inv.inv_week,'0') = LTRIM(td.cal_date_id,'0')
        --AND   inv.year = td.cal_year
        AND inv.inv_dt::DATE >= price.prom_strt_dt
        AND inv.inv_dt::DATE <= price.prom_end_dt
        AND inv.ean_num = price.barcd(+)
        AND inv.item_cd = price.cust_prod_cd(+)
    GROUP BY ctry_cd,
        Data_Type,
        LTRIM(CUST.cust_num, '0'),
        ean_num,
        cal_mnth_id
    ),
t_joined
AS (
    SELECT *
    FROM t1
    
    UNION ALL
    
    SELECT *
    FROM t2
    
    UNION ALL
    
    SELECT *
    FROM t3
    
    UNION ALL
    
    SELECT *
    FROM t4
    
    UNION ALL
    
    SELECT *
    FROM t5
    
    UNION ALL
    
    SELECT *
    FROM t6
    ),
t_select
AS (
    SELECT ctry_cd,
        data_type,
        dstr_cd,
        ean_num,
        cal_month,
        sum(inv_qty) AS inv_qty,
        sum(inv_value) AS inv_value,
        sum(so_qty) AS so_qty,
        sum(sls_value) AS so_value,
        sum(si_qty) AS si_qty,
        sum(si_value) AS si_value
    FROM t_joined
    WHERE dstr_cd <> (
            SELECT DISTINCT parameter_value
            FROM itg_parameter_reg_inventory
            WHERE country_name = 'Taiwan'
                AND parameter_name = 'inv_analysis_distributor_id'
            )
    GROUP BY ctry_cd,
        data_type,
        dstr_cd,
        ean_num,
        cal_month
    ),
t7
AS (
    SELECT cust_num,
        CASE 
            WHEN prnt_cust_key IS NULL
                OR prnt_cust_key = ''
                THEN 'Not Assigned'
            ELSE prnt_cust_key
            END AS prnt_cust_key,
        CASE 
            WHEN CDDES_PCK.code_desc IS NULL
                OR CDDES_PCK.code_desc = ''
                THEN 'Not Assigned'
            ELSE CDDES_PCK.code_desc
            END AS prnt_cust_desc,
        CASE 
            WHEN bnr_key IS NULL
                OR bnr_key = ''
                THEN 'Not Assigned'
            ELSE bnr_key
            END AS bnr_key,
        CASE 
            WHEN CDDES_BNRKEY.code_desc IS NULL
                OR CDDES_BNRKEY.code_desc = ''
                THEN 'Not Assigned'
            ELSE CDDES_BNRKEY.code_desc
            END AS bnr_desc
    FROM (
        SELECT ltrim(cust_num, 0) cust_num,
            prnt_cust_key,
            bnr_key
        FROM EDW_CUSTOMER_SALES_DIM
        WHERE sls_org = '1200'
        ) b,
        (
            SELECT CODE,
                CODE_DESC
            FROM EDW_CODE_DESCRIPTIONS
            WHERE trim(Upper(CODE_TYPE)) = 'PARENT CUSTOMER KEY'
            ) CDDES_PCK,
        (
            SELECT CODE,
                CODE_DESC
            FROM EDW_CODE_DESCRIPTIONS
            WHERE trim(Upper(CODE_TYPE)) = 'BANNER KEY'
            ) CDDES_BNRKEY
    WHERE CDDES_PCK.CODE(+) = b.PRNT_CUST_KEY
        AND CDDES_BNRKEY.CODE(+) = b.bnr_key
    ),
final
AS (
    SELECT ctry_cd,
        PRNT_CUST_KEY AS sap_parent_customer_key,
        prnt_cust_desc AS sap_parent_customer_desc,
        bnr_key,
        bnr_desc,
        ean_num,
        cal_month,
        sum(inv_qty) AS inv_qty,
        sum(inv_value) AS inv_value,
        sum(so_qty) AS so_qty,
        sum(so_value) AS so_value,
        sum(si_qty) AS si_qty,
        sum(si_value) AS si_value
    FROM t_select a,
        t7 b
    WHERE b.cust_num(+) = a.dstr_cd
    GROUP BY ctry_cd,
        PRNT_CUST_KEY,
        prnt_cust_desc,
        bnr_key,
        bnr_desc,
        ean_num,
        cal_month
    )
SELECT *
FROM final
