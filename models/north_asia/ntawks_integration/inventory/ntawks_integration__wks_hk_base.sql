WITH v_rpt_ims_inventory
AS (
    SELECT *
    FROM {{ ref('ntaedw_integration__v_rpt_ims_inventory') }}
    ),
itg_parameter_reg_inventory
AS (
    SELECT *
    FROM {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
    ),
EDW_VW_OS_TIME_DIM
AS (
    SELECT *
    FROM {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
    ),
v_rpt_ims_inventory_analysis
AS (
    SELECT *
    FROM {{ ref('ntaedw_integration__v_rpt_ims_inventory_analysis') }}
    ),
edw_billing_fact
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_billing_fact') }}
    ),
edw_list_price
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_list_price') }}
    ),
edw_material_sales_dim
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_material_sales_dim') }}
    ),
t1
AS (
    SELECT dstr_cd,
        fisc_per, --fisc_per,
        MAX(invnt_dt) max_invnt_dt
    FROM v_rpt_ims_inventory
    WHERE ctry_nm = 'Hong Kong'
        AND ltrim(dstr_cd, 0) IN (
            SELECT parameter_value
            FROM itg_parameter_reg_inventory
            WHERE country_name = 'HK'
                AND parameter_name = 'inv_analysis_distributor_id'
            )
        AND non_sellable_product = 'sellable products'
        AND from_crncy = 'HKD'
        AND to_crncy = 'HKD'
        AND left(invnt_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
    GROUP BY dstr_cd,
        fisc_per
    ),
t2
AS (
    SELECT 'HK' ctry_cd,
        'INV' Data_Type,
        inv.dstr_cd AS dstr_cd,
        prod_cd,
        substring(inv.fisc_per, 1, 4) || substring(inv.fisc_per, 6, 2) AS fisc_per,
        SUM(invnt_qty) inv_qty,
        0 AS inv_value,
        0 AS so_qty,
        0 AS so_value,
        0 AS si_qty,
        0 AS si_value
    FROM v_rpt_ims_inventory inv
    JOIN t1 inv_max ON inv.dstr_cd = inv_max.dstr_cd
        AND ltrim(inv.dstr_cd, 0) IN (
            SELECT parameter_value
            FROM itg_parameter_reg_inventory
            WHERE country_name = 'HK'
                AND parameter_name = 'inv_analysis_distributor_id'
            )
        AND inv.fisc_per = inv_max.fisc_per
        AND inv.invnt_dt = inv_max.max_invnt_dt
    WHERE ctry_nm = 'Hong Kong'
        AND non_sellable_product = 'sellable products'
        AND from_crncy = 'HKD'
        AND to_crncy = 'HKD'
        AND left(inv.invnt_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
    GROUP BY ctry_nm,
        inv.dstr_cd,
        prod_cd,
        substring(inv.fisc_per, 1, 4) || substring(inv.fisc_per, 6, 2)
    ),
t3
AS (
    SELECT 'HK' AS ctry_cd,
        txn.dstr_cd,
        prod_cd,
        ean_num,
        ims_txn_dt,
        fisc_per,
        (sls_qty) - (rtrn_qty) sls_qty,
        0 AS sls_amt
    FROM v_rpt_ims_inventory_analysis txn
    WHERE (
            CASE 
                WHEN txn.prod_cd like '1U%'
                    OR txn.prod_cd like 'COUNTER TOP%'
                    OR txn.prod_cd IS NULL
                    OR txn.prod_cd = ''
                    OR txn.prod_cd like 'DUMPBIN%'
                    THEN 'non sellable products'
                ELSE 'sellable products'
                END = 'sellable products'
            )
        AND ctry_nm = 'Hong Kong'
        AND left(ims_txn_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
        AND ltrim(dstr_cd, 0) IN (
            SELECT parameter_value
            FROM itg_parameter_reg_inventory
            WHERE country_name = 'HK'
                AND parameter_name = 'inv_analysis_distributor_id'
            )
        AND from_crncy = 'HKD'
        AND to_crncy = 'HKD'
    ),
t4
AS (
    SELECT ctry_cd,
        'SELLOUT' AS data_type,
        dstr_cd,
        prod_cd,
        substring(fisc_per, 1, 4) || substring(fisc_per, 6, 2) AS fisc_per,
        0 AS inv_qty,
        0 AS inv_amt,
        sum(sls_qty) so_qty,
        0 AS so_value,
        0 AS sell_in_qty,
        0 AS sell_in_value
    FROM t3
    GROUP BY ctry_cd,
        dstr_cd,
        prod_cd,
        substring(fisc_per, 1, 4) || substring(fisc_per, 6, 2)
    ),
t5
AS (
    SELECT 'HK' cntry_cd,
        'SELLIN' AS data_type,
        LTRIM(sold_to, '0') AS distributor_code,
        LTRIM(MATERIAL, '0') AS product_code,
        bill_dt,
        0 AS inv_qty,
        0 AS inv_amt,
        0 AS sls_qty,
        0 AS sls_amt,
        sum(bill_qty) sell_in_qty,
        sum(subtotal_1) sell_in_value
    FROM edw_billing_fact
    WHERE left(bill_dt, 4) >= (DATE_PART(YEAR, SYSDATE()) - 6)
        AND sls_org = '1110'
        AND ltrim(sold_to, 0) IN (
            SELECT parameter_value
            FROM itg_parameter_reg_inventory
            WHERE country_name = 'HK'
                AND parameter_name = 'inv_analysis_distributor_id'
            )
        AND BILL_TYPE IN ('ZF2H', 'ZG2H')
    GROUP BY LTRIM(sold_to, '0'),
        LTRIM(MATERIAL, '0'),
        bill_dt
    ),
t6
AS (
    SELECT cntry_cd,
        data_type,
        distributor_code,
        product_code,
        mnth_id AS fisc_per,
        sum(inv_qty) inv_qty,
        sum(inv_amt) inv_value,
        sum(sls_qty) so_qty,
        sum(sls_amt) so_value,
        sum(sell_in_qty) si_qty,
        sum(sell_in_value) si_value
    FROM t5 
    LEFT JOIN EDW_VW_OS_TIME_DIM  ON T5.bill_dt = to_date(EDW_VW_OS_TIME_DIM.cal_date)
    GROUP BY cntry_cd,
        data_type,
        distributor_code,
        product_code,
        mnth_id
    ),
union_of_ts
AS (
    SELECT *
    FROM t2
    
    UNION ALL

    SELECT *
    FROM t4
    
    UNION ALL
    
    SELECT *
    FROM t6
    ),
final
AS (
    SELECT ctry_cd,
        dstr_cd,
        nvl(nullif(prod_cd, ''), 'NA') AS matl_num,
        fisc_per AS month,
        SUM(T1.si_qty) AS SI_SLS_QTY,
        SUM(T1.si_value) AS SI_GTS_VAL,
        SUM(T1.inv_qty) AS INVENTORY_QUANTITY,
        SUM(T1.inv_qty * nvl(t5.amount, t6.amount)) AS INVENTORY_VAL,
        SUM(T1.SO_QTY) AS SO_SLS_QTY,
        SUM(T1.SO_QTY * nvl(t5.amount, t6.amount)) AS SO_TRD_SLS
    FROM union_of_ts T1,
        (
            SELECT ltrim(material, 0) material,
                MAX(amount) amount
            FROM (
                SELECT T1.*,
                    (
                        rank() OVER (
                            PARTITION BY ltrim(T1.material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC,
                                to_date(dt_from, 'YYYYMMDD') DESC
                            )
                        ) AS rn
                FROM edw_list_price T1
                WHERE sls_org = '1110'
                )
            WHERE rn = 1
            GROUP BY material
            ) T5,
        (
            SELECT ean_num,
                amount,
                matl_num
            FROM (
                SELECT t_flp.*,
                    (
                        row_number() OVER (
                            PARTITION BY matl_num ORDER BY amount DESC
                            )
                        ) AS rn
                FROM (
                    SELECT DISTINCT T_LP.ean_num,
                        T_LP.amount,
                        matl_num
                    FROM (
                        SELECT ean_num,
                            MAX(amount) amount
                        FROM (
                            SELECT T_LP1.*,
                                (
                                    RANK() OVER (
                                        PARTITION BY ean_num,
                                        LTRIM(T_LP1.material, 0) ORDER BY TO_DATE(valid_to, 'YYYYMMDD') DESC,
                                            TO_DATE(dt_from, 'YYYYMMDD') DESC
                                        )
                                    ) AS rn
                            FROM (
                                SELECT DISTINCT ms.matl_num,
                                    ms.ean_num,
                                    LP.*
                                FROM edw_material_sales_dim ms,
                                    edw_list_price LP
                                WHERE lp.sls_org = '1110'
                                    AND lp.sls_org = ms.sls_org
                                    AND LTRIM(ms.matl_num, '0') = LTRIM(LP.material, '0')
                                ) T_LP1
                            ) ean_lp
                        WHERE rn = 1
                        GROUP BY ean_num
                        ) T_LP
                    JOIN edw_material_sales_dim ms1 ON ms1.ean_num = T_LP.ean_num
                    WHERE ms1.sls_org = '1110'
                        AND ms1.ean_num != ''
                    ) t_flp
                )
            WHERE rn = 1
            ) T6
    WHERE ltrim(T1.dstr_cd, 0) IN (
            SELECT parameter_value
            FROM itg_parameter_reg_inventory
            WHERE country_name = 'HK'
                AND parameter_name = 'inv_analysis_distributor_id'
            )
        AND Ltrim(T1.prod_cd, 0) = ltrim(T5.material(+), 0)
        AND Ltrim(T1.prod_cd, 0) = ltrim(T6.matl_num(+), 0)
    GROUP BY ctry_cd,
        dstr_cd,
        prod_cd,
        fisc_per
    )
SELECT *
FROM
final