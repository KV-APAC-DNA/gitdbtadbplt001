with edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_sales_order_fact as
(
    select * from {{ ref('aspedw_integration__edw_sales_order_fact') }}
),
edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_material_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
v_intrm_crncy_exch as
(
    select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
),
edw_product_attr_dim as
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
edw_customer_attr_flat_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
edw_customer_attr_hier_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_attr_hier_dim') }}
),
edw_ims_fact as
(
    select * from {{ ref('ntaedw_integration__edw_ims_fact') }}
),
edw_pos_fact as
(
    select * from {{ ref('ntaedw_integration__edw_pos_fact') }}
),
edw_tw_bu_forecast_sku as
(
    select * from {{ ref('ntaedw_integration__edw_tw_bu_forecast_sku') }}
),
tw_except_rtn as
(
    SELECT
        'SELLIN_CM'::character varying AS sellin_data_set,
        fct.matl_num,
        fct.div,
        fct.sls_org,
        fct.dstr_chnl,
        fct.ctry_key,
        try_to_date
        (
            concat_ws
                (
                    '-',
                    left(fct.fisc_yr_per,4),
                    right(fct.fisc_yr_per,2),
                    '01'
                )
        ) AS fisc_date,
        fct.fisc_yr_per AS fisc_per,
        fct.obj_crncy_co_obj,
        fct.cust_num,
        fct.co_cd,
        0 AS return_val,
        0 AS return_qty,
        sum(
            CASE
                WHEN (
                    (fct.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                ) THEN fct.amt_obj_crncy
                ELSE ((0)::numeric)::numeric(18, 0)
            END
        ) AS copa_amt,
        sum(
            CASE
                WHEN (
                    (fct.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                ) THEN fct.amt_obj_crncy
                ELSE ((0)::numeric)::numeric(18, 0)
            END
        ) AS copa_ret_amt,
        sum(
            CASE
                WHEN (
                    (
                        (
                            (fct.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                        )
                        OR (
                            (fct.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                        )
                    )
                    OR (
                        (fct.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                    )
                ) THEN fct.amt_obj_crncy
                ELSE ((0)::numeric)::numeric(18, 0)
            END
        ) AS hidden_amt,
        sum(fct.qty) AS qty,
        sum(fct.amt_obj_crncy) AS amt_obj_crncy
    FROM edw_copa_trans_fact fct
    WHERE
    (
        fct.acct_hier_shrt_desc = 'GTS'
        OR fct.acct_hier_shrt_desc = 'HDPM'
        OR fct.acct_hier_shrt_desc = 'HDMD'
        OR fct.acct_hier_shrt_desc = 'HDPR'
    )
    AND fct.ctry_key = 'TW'
    GROUP BY
    1,
    fct.matl_num,
    fct.div,
    fct.sls_org,
    fct.dstr_chnl,
    fct.ctry_key,
    try_to_date
    (
        concat_ws
            (
                '-',
                left(fct.fisc_yr_per,4),
                right(fct.fisc_yr_per,2),
                '01'
            )
    ),
    fct.fisc_yr_per,
    fct.obj_crncy_co_obj,
    fct.cust_num,
    fct.co_cd
),
tw_rtn as
(
    SELECT
        'SELLIN_CM'::character varying AS sellin_data_set,
        fct.matl_num,
        fct.div,
        fct.sls_org,
        fct.dstr_chnl,
        fct.ctry_key,
        concat_ws
        (
            '-',
            left(fisc_yr_per,4),
            right(fisc_yr_per,2),
            '01'
        ) AS fisc_date,
        fct.fisc_yr_per AS fisc_per,
        fct.obj_crncy_co_obj,
        fct.cust_num,
        fct.co_cd,
        0 AS return_val,
        0 AS return_qty,
        sum(
            CASE
                WHEN (
                    (fct.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                ) THEN fct.amt_obj_crncy
                ELSE ((0)::numeric)::numeric(18, 0)
            END
        ) AS copa_amt,
        sum(
            CASE
                WHEN (
                    (fct.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                ) THEN fct.amt_obj_crncy
                ELSE ((0)::numeric)::numeric(18, 0)
            END
        ) AS copa_ret_amt,
        sum(
            CASE
                WHEN (
                    (
                        (
                            (fct.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                        )
                        OR (
                            (fct.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                        )
                    )
                    OR (
                        (fct.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                    )
                ) THEN fct.amt_obj_crncy
                ELSE ((0)::numeric)::numeric(18, 0)
            END
        ) AS hidden_amt,
        sum(fct.qty) AS qty,
        sum(fct.amt_obj_crncy) AS amt_obj_crncy
    FROM edw_copa_trans_fact fct
    WHERE (fct.acct_hier_shrt_desc = 'RTN')
    AND (fct.ctry_key = 'TW')
    GROUP BY
        1,
        fct.matl_num,
        fct.div,
        fct.sls_org,
        fct.dstr_chnl,
        fct.ctry_key,
        concat_ws
        (
            '-',
            left(fisc_yr_per,4),
            right(fisc_yr_per,2),
            '01'
        ),
        fct.fisc_yr_per,
        fct.obj_crncy_co_obj,
        fct.cust_num,
        fct.co_cd
),
derived_table1 as
(
    SELECT
        derived_table1.sellin_data_set,
        derived_table1.matl_num,
        derived_table1.div,
        derived_table1.sls_org,
        derived_table1.dstr_chnl,
        derived_table1.ctry_key,
        derived_table1.fisc_date,
        derived_table1.fisc_per,
        derived_table1.obj_crncy_co_obj,
        derived_table1.cust_num,
        derived_table1.co_cd,
        sum(derived_table1.copa_amt) AS copa_amt,
        sum(derived_table1.copa_ret_amt) AS copa_ret_amt,
        sum(derived_table1.hidden_amt) AS hidden_amt,
        sum(derived_table1.return_val) AS return_val,
        sum(derived_table1.return_qty) AS return_qty,
        sum(derived_table1.qty) AS qty,
        0 AS copa_amt_l3m,
        0 AS copa_return_amt_l3m,
        0 AS qty_l3m,
        0 AS copa_amt_l6m,
        0 AS copa_return_amt_l6m,
        0 AS qty_l6m
    FROM
        (
            select * from tw_except_rtn
            UNION ALL
            select * from tw_rtn
        ) derived_table1
    GROUP BY derived_table1.sellin_data_set,
        derived_table1.matl_num,
        derived_table1.div,
        derived_table1.sls_org,
        derived_table1.dstr_chnl,
        derived_table1.ctry_key,
        derived_table1.fisc_date,
        derived_table1.fisc_per,
        derived_table1.obj_crncy_co_obj,
        derived_table1.cust_num,
        derived_table1.co_cd
),
derived_table3 as
(
    SELECT
        'SELLIN_CM'::character varying AS sellin_data_set,
        so.material AS matl_num,
        so.division AS div,
        so.sls_org,
        so.distr_chan AS dstr_chnl,
        'TW'::character varying AS ctry_key,
        try_to_date
        (
            concat_ws
                (
                    '-',
                    left(cal.fisc_per,4),
                    right(cal.fisc_per,2),
                    '01'
                )
        ) AS fisc_date,
        cal.fisc_per,
        'TWD'::character varying AS obj_crncy_co_obj,
        so.sold_to AS cust_num,
        so.comp_cd AS co_cd,
        0 AS return_val,
        sum(so.cml_cf_qty) AS return_qty,
        0 AS copa_amt,
        0 AS copa_ret_amt,
        0 AS hidden_amt,
        0 AS qty,
        0 AS amt_obj_crncy
    FROM
        (
            edw_sales_order_fact so
            JOIN edw_calendar_dim cal ON ((so.bill_dt = cal.cal_day))
        )
    WHERE
        (so.doc_type = 'ZRET')
        AND (so.ord_reason LIKE 'X%')
        AND (so.sls_org = '1200' OR so.sls_org = '120S')
    GROUP BY 1,
        so.material,
        so.division,
        so.sls_org,
        so.distr_chan,
        6,
        try_to_date
        (
            concat_ws
                (
                    '-',
                    left(cal.fisc_per,4),
                    right(cal.fisc_per,2),
                    '01'
                )
        ),
        cal.fisc_per,
        9,
        so.sold_to,
        so.comp_cd
    UNION ALL
    SELECT
        derived_table2.sellin_data_set,
        derived_table2.matl_num,
        derived_table2.div,
        derived_table2.sls_org,
        derived_table2.dstr_chnl,
        derived_table2.ctry_key,
        derived_table2.fisc_date,
        derived_table2.fisc_per,
        derived_table2.obj_crncy_co_obj,
        derived_table2.cust_num,
        derived_table2.co_cd,
        (
            sum(
                CASE
                    WHEN (
                        derived_table2.zret = ((0)::numeric)::numeric(18, 0)
                    ) THEN ((0)::numeric)::numeric(18, 0)
                    ELSE COALESCE(derived_table2.zdrt,null)
                END
            ) + sum(derived_table2.zret)
        ) AS return_val,
        sum(derived_table2.return_qty) AS return_qty,
        0 AS copa_amt,
        0 AS copa_ret_amt,
        0 AS hidden_amt,
        0 AS qty,
        0 AS amt_obj_crncy
    FROM
        (
            SELECT 'SELLIN_CM'::character varying AS sellin_data_set,
                so.material AS matl_num,
                so.division AS div,
                so.sls_org,
                so.distr_chan AS dstr_chnl,
                'TW'::character varying AS ctry_key,
                try_to_date
                (
                    concat_ws
                        (
                            '-',
                            left(cal.fisc_per,4),
                            right(cal.fisc_per,2),
                            '01'
                        )
                ) AS fisc_date,
                cal.fisc_per,
                'TWD'::character varying AS obj_crncy_co_obj,
                so.sold_to AS cust_num,
                so.comp_cd AS co_cd,
                sum(
                    CASE
                        WHEN (
                            (
                                (so.doc_type)::text = ('ZRET'::character varying)::text
                            )
                            AND (
                                (so.ord_reason)::text like ('X%'::character varying)::text
                            )
                        ) THEN so.net_value
                        ELSE ((0)::numeric)::numeric(18, 0)
                    END
                ) AS zret,
                sum(
                    CASE
                        WHEN (
                            (so.doc_type)::text = ('ZDRT'::character varying)::text
                        ) THEN so.net_value
                        ELSE ((0)::numeric)::numeric(18, 0)
                    END
                ) AS zdrt,
                0 AS return_qty,
                0 AS copa_amt,
                0 AS copa_ret_amt,
                0 AS hidden_amt,
                0 AS qty,
                0 AS amt_obj_crncy
            FROM (
                    edw_sales_order_fact so
                    JOIN edw_calendar_dim cal ON ((so.bill_dt = cal.cal_day))
                )
            WHERE
            (so.doc_type = 'ZDRT' OR so.doc_type = 'ZRET')
            AND (so.sls_org = '1200' OR so.sls_org = '120S')
            GROUP BY 1,
                so.material,
                so.division,
                so.sls_org,
                so.distr_chan,
                6,
                try_to_date
                (
                    concat_ws
                        (
                            '-',
                            left(cal.fisc_per,4),
                            right(cal.fisc_per,2),
                            '01'
                        )
                ),
                cal.fisc_per,
                9,
                so.sold_to,
                so.comp_cd
        ) derived_table2
    GROUP BY
        derived_table2.sellin_data_set,
        derived_table2.matl_num,
        derived_table2.div,
        derived_table2.sls_org,
        derived_table2.dstr_chnl,
        derived_table2.ctry_key,
        derived_table2.fisc_date,
        derived_table2.fisc_per,
        derived_table2.obj_crncy_co_obj,
        derived_table2.cust_num,
        derived_table2.co_cd
),
sellin_cm as
(
    SELECT
        derived_table4.sellin_data_set,
        derived_table4.matl_num,
        derived_table4.div,
        derived_table4.sls_org,
        derived_table4.dstr_chnl,
        derived_table4.ctry_key,
        derived_table4.fisc_date,
        derived_table4.fisc_per,
        derived_table4.obj_crncy_co_obj,
        derived_table4.cust_num,
        derived_table4.co_cd,
        (
            (
                sum(derived_table4.copa_amt) - sum(derived_table4.hidden_amt)
            ) + sum(derived_table4.return_val)
        ) AS copa_amt,
        (
            sum(derived_table4.copa_ret_amt) + sum(derived_table4.return_val)
        ) AS copa_ret_amt,
        (
            sum(derived_table4.qty) + sum(derived_table4.return_qty)
        ) AS qty,
        sum(derived_table4.copa_amt_l3m) AS copa_amt_l3m,
        sum(derived_table4.copa_return_amt_l3m) AS copa_return_amt_l3m,
        sum(derived_table4.qty_l3m) AS qty_l3m,
        sum(derived_table4.copa_amt_l6m) AS copa_amt_l6m,
        sum(derived_table4.copa_return_amt_l6m) AS copa_return_amt_l6m,
        sum(derived_table4.qty_l6m) AS qty_l6m
    FROM
        (
            select * from derived_table1
            UNION ALL
            SELECT
                derived_table3.sellin_data_set,
                derived_table3.matl_num,
                derived_table3.div,
                derived_table3.sls_org,
                derived_table3.dstr_chnl,
                derived_table3.ctry_key,
                derived_table3.fisc_date,
                derived_table3.fisc_per,
                derived_table3.obj_crncy_co_obj,
                derived_table3.cust_num,
                derived_table3.co_cd,
                sum(derived_table3.copa_amt) AS copa_amt,
                sum(derived_table3.copa_ret_amt) AS copa_ret_amt,
                sum(derived_table3.hidden_amt) AS hidden_amt,
                sum(derived_table3.return_val) AS return_val,
                sum(derived_table3.return_qty) AS return_qty,
                sum(derived_table3.qty) AS qty,
                0 AS copa_amt_l3m,
                0 AS copa_return_amt_l3m,
                0 AS qty_l3m,
                0 AS copa_amt_l6m,
                0 AS copa_return_amt_l6m,
                0 AS qty_l6m
            FROM derived_table3
            GROUP BY
                derived_table3.sellin_data_set,
                derived_table3.matl_num,
                derived_table3.div,
                derived_table3.sls_org,
                derived_table3.dstr_chnl,
                derived_table3.ctry_key,
                derived_table3.fisc_date,
                derived_table3.fisc_per,
                derived_table3.obj_crncy_co_obj,
                derived_table3.cust_num,
                derived_table3.co_cd
        ) derived_table4
    GROUP BY derived_table4.sellin_data_set,
        derived_table4.matl_num,
        derived_table4.div,
        derived_table4.sls_org,
        derived_table4.dstr_chnl,
        derived_table4.ctry_key,
        derived_table4.fisc_date,
        derived_table4.fisc_per,
        derived_table4.obj_crncy_co_obj,
        derived_table4.cust_num,
        derived_table4.co_cd
),
ref_date_fiscal as
(
    SELECT
        DISTINCT 
        try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) AS fisc_date,
        dateadd(month, -3, try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01'))) AS threemnths_fisc_date,
        dateadd(month, -6, try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01'))) AS sixmnths_fisc_date,
        dateadd(month, -2, try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01'))) AS twomnths_fisc_date
    FROM edw_calendar_dim cal

),
sellin_l3m as
(
    SELECT
        derived_table8.sellin_data_set,
        derived_table8.matl_num,
        derived_table8.div,
        derived_table8.sls_org,
        derived_table8.dstr_chnl,
        derived_table8.ctry_key,
        derived_table8.fisc_date,
        derived_table8.fisc_per,
        derived_table8.obj_crncy_co_obj,
        derived_table8.cust_num,
        derived_table8.co_cd,
        0 AS copa_amt,
        0 AS copa_ret_amt,
        sum(derived_table8.qty) AS qty,
        (
            (
                sum(derived_table8.copa_amt_l3m) - sum(derived_table8.hidden_amt)
            ) + sum(derived_table8.return_val)
        ) AS copa_amt_l3m,
        (
            sum(derived_table8.copa_return_amt_l3m) + sum(derived_table8.return_val)
        ) AS copa_return_amt_l3m,
        (
            sum(derived_table8.qty_l3m) + sum(derived_table8.return_qty)
        ) AS qty_l3m,
        sum(derived_table8.copa_amt_l6m) AS copa_amt_l6m,
        sum(derived_table8.copa_return_amt_l6m) AS copa_return_amt_l6m,
        sum(derived_table8.qty_l6m) AS qty_l6m
    FROM
        (
            SELECT
                derived_table5.sellin_data_set,
                derived_table5.matl_num,
                derived_table5.div,
                derived_table5.sls_org,
                derived_table5.dstr_chnl,
                derived_table5.ctry_key,
                derived_table5.fisc_date,
                derived_table5.fisc_per,
                derived_table5.obj_crncy_co_obj,
                derived_table5.cust_num,
                derived_table5.co_cd,
                0 AS copa_amt,
                0 AS copa_ret_amt,
                sum(derived_table5.hidden_amt) AS hidden_amt,
                sum(derived_table5.return_val) AS return_val,
                sum(derived_table5.return_qty) AS return_qty,
                0 AS qty,
                sum(derived_table5.copa_amt) AS copa_amt_l3m,
                sum(derived_table5.copa_ret_amt) AS copa_return_amt_l3m,
                sum(derived_table5.qty) AS qty_l3m,
                0 AS copa_amt_l6m,
                0 AS copa_return_amt_l6m,
                0 AS qty_l6m
            FROM
                (
                    SELECT
                        'SELLIN_L3M'::character varying AS sellin_data_set,
                        fct.matl_num,
                        fct.div,
                        fct.sls_org,
                        fct.dstr_chnl,
                        fct.ctry_key,
                        ref_date_fiscal.fisc_date,
                        fct.fisc_yr_per AS fisc_per,
                        fct.obj_crncy_co_obj,
                        fct.cust_num,
                        fct.co_cd,
                        0 AS return_val,
                        0 AS return_qty,
                        sum(
                            CASE
                                WHEN (
                                    (fct.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS copa_amt,
                        sum(
                            CASE
                                WHEN (
                                    (fct.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS copa_ret_amt,
                        sum(
                            CASE
                                WHEN (
                                    (
                                        (
                                            (fct.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                                        )
                                        OR (
                                            (fct.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                                        )
                                    )
                                    OR (
                                        (fct.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                                    )
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS hidden_amt,
                        sum(fct.qty) AS qty,
                        sum(fct.amt_obj_crncy) AS amt_obj_crncy
                    FROM edw_copa_trans_fact fct,
                    ref_date_fiscal
                    WHERE
                        (
                            (
                                fct.acct_hier_shrt_desc = 'GTS'
                                OR fct.acct_hier_shrt_desc = 'HDPM'
                                OR fct.acct_hier_shrt_desc = 'HDMD'
                                OR fct.acct_hier_shrt_desc = 'HDPR')
                            AND fct.ctry_key = 'TW'
                            AND try_to_date(concat_ws('-',left(fct.fisc_yr_per,4),right(fct.fisc_yr_per,2),'01')) > ref_date_fiscal.threemnths_fisc_date
                            AND try_to_date(concat_ws('-',left(fct.fisc_yr_per,4),right(fct.fisc_yr_per,2),'01')) <= ref_date_fiscal.fisc_date
                            AND fct.ctry_key = 'TW'
                        )

                    GROUP BY 1,
                        fct.matl_num,
                        fct.div,
                        fct.sls_org,
                        fct.dstr_chnl,
                        fct.ctry_key,
                        ref_date_fiscal.fisc_date,
                        fct.fisc_yr_per,
                        fct.obj_crncy_co_obj,
                        fct.cust_num,
                        fct.co_cd
                    UNION ALL
                    SELECT
                        'SELLIN_L3M'::character varying AS sellin_data_set,
                        fct.matl_num,
                        fct.div,
                        fct.sls_org,
                        fct.dstr_chnl,
                        fct.ctry_key,
                        ref_date_fiscal.fisc_date,
                        fct.fisc_yr_per AS fisc_per,
                        fct.obj_crncy_co_obj,
                        fct.cust_num,
                        fct.co_cd,
                        0 AS return_val,
                        0 AS return_qty,
                        sum(
                            CASE
                                WHEN (
                                    (fct.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS copa_amt,
                        sum(
                            CASE
                                WHEN (
                                    (fct.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS copa_ret_amt,
                        sum(
                            CASE
                                WHEN (
                                    (
                                        (
                                            (fct.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                                        )
                                        OR (
                                            (fct.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                                        )
                                    )
                                    OR (
                                        (fct.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                                    )
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS hidden_amt,
                        sum(fct.qty) AS qty,
                        sum(fct.amt_obj_crncy) AS amt_obj_crncy
                    FROM edw_copa_trans_fact fct,
                    ref_date_fiscal
                    WHERE
                    (
                        fct.acct_hier_shrt_desc = 'RTN'
                        AND fct.ctry_key = 'TW'
                        AND try_to_date(concat_ws('-',left(fct.fisc_yr_per,4),right(fct.fisc_yr_per,2),'01')) > ref_date_fiscal.threemnths_fisc_date
                        AND try_to_date(concat_ws('-',left(fct.fisc_yr_per,4),right(fct.fisc_yr_per,2),'01')) <= ref_date_fiscal.fisc_date
                        AND fct.ctry_key = 'TW'
                    )
                    GROUP BY 1,
                        fct.matl_num,
                        fct.div,
                        fct.sls_org,
                        fct.dstr_chnl,
                        fct.ctry_key,
                        ref_date_fiscal.fisc_date,
                        fct.fisc_yr_per,
                        fct.obj_crncy_co_obj,
                        fct.cust_num,
                        fct.co_cd
                ) derived_table5
            GROUP BY
                derived_table5.sellin_data_set,
                derived_table5.matl_num,
                derived_table5.div,
                derived_table5.sls_org,
                derived_table5.dstr_chnl,
                derived_table5.ctry_key,
                derived_table5.fisc_date,
                derived_table5.fisc_per,
                derived_table5.obj_crncy_co_obj,
                derived_table5.cust_num,
                derived_table5.co_cd
            UNION ALL
            SELECT
                derived_table7.sellin_data_set,
                derived_table7.matl_num,
                derived_table7.div,
                derived_table7.sls_org,
                derived_table7.dstr_chnl,
                derived_table7.ctry_key,
                derived_table7.fisc_date,
                derived_table7.fisc_per,
                derived_table7.obj_crncy_co_obj,
                derived_table7.cust_num,
                derived_table7.co_cd,
                sum(derived_table7.copa_amt) AS copa_amt,
                sum(derived_table7.copa_return_amt) AS copa_ret_amt,
                sum(derived_table7.hidden_amt) AS hidden_amt,
                sum(derived_table7.return_val) AS return_val,
                sum(derived_table7.return_qty) AS return_qty,
                sum(derived_table7.qty) AS qty,
                0 AS copa_amt_l3m,
                0 AS copa_return_amt_l3m,
                0 AS qty_l3m,
                0 AS copa_amt_l6m,
                0 AS copa_return_amt_l6m,
                0 AS qty_l6m
            FROM
                (
                    SELECT
                        'SELLIN_L3M'::character varying AS sellin_data_set,
                        so.material AS matl_num,
                        so.division AS div,
                        so.sls_org,
                        so.distr_chan AS dstr_chnl,
                        'TW'::character varying AS ctry_key,
                        ref_date_fiscal.fisc_date,
                        cal.fisc_per,
                        'TWD'::character varying AS obj_crncy_co_obj,
                        so.sold_to AS cust_num,
                        so.comp_cd AS co_cd,
                        0 AS return_val,
                        sum(so.cml_cf_qty) AS return_qty,
                        0 AS copa_amt,
                        0 AS copa_return_amt,
                        0 AS hidden_amt,
                        0 AS qty,
                        0 AS amt_obj_crncy
                    FROM edw_sales_order_fact so
                    JOIN edw_calendar_dim cal ON ((so.bill_dt = cal.cal_day))
                    JOIN ref_date_fiscal ON
                    (
                        (
                            (
                                try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) > ref_date_fiscal.threemnths_fisc_date
                            )
                            AND (
                                try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) <= ref_date_fiscal.fisc_date
                            )
                        )
                    )
                    WHERE (so.doc_type = 'ZRET' AND so.ord_reason LIKE 'X%')
                    AND (so.sls_org = '1200' OR so.sls_org = '120S')
                    GROUP BY 1,
                        so.material,
                        so.division,
                        so.sls_org,
                        so.distr_chan,
                        6,
                        ref_date_fiscal.fisc_date,
                        cal.fisc_per,
                        9,
                        so.sold_to,
                        so.comp_cd
                    UNION ALL
                    SELECT
                        derived_table6.sellin_data_set,
                        derived_table6.matl_num,
                        derived_table6.div,
                        derived_table6.sls_org,
                        derived_table6.dstr_chnl,
                        derived_table6.ctry_key,
                        derived_table6.fisc_date,
                        derived_table6.fisc_per,
                        derived_table6.obj_crncy_co_obj,
                        derived_table6.cust_num,
                        derived_table6.co_cd,
                        (
                            sum(
                                CASE
                                    WHEN (
                                        derived_table6.zret = ((0)::numeric)::numeric(18, 0)
                                    ) THEN ((0)::numeric)::numeric(18, 0)
                                    ELSE COALESCE(derived_table6.zdrt,null)
                                END
                            ) + sum(derived_table6.zret)
                        ) AS return_val,
                        sum(derived_table6.return_qty) AS return_qty,
                        0 AS copa_amt,
                        0 AS copa_return_amt,
                        0 AS hidden_amt,
                        0 AS qty,
                        0 AS amt_obj_crncy
                    FROM
                        (
                            SELECT
                                'SELLIN_L3M'::character varying AS sellin_data_set,
                                so.material AS matl_num,
                                so.division AS div,
                                so.sls_org,
                                so.distr_chan AS dstr_chnl,
                                'TW'::character varying AS ctry_key,
                                ref_date_fiscal.fisc_date,
                                cal.fisc_per,
                                'TWD'::character varying AS obj_crncy_co_obj,
                                so.sold_to AS cust_num,
                                so.comp_cd AS co_cd,
                                sum(
                                    CASE
                                        WHEN (
                                            (
                                                (so.doc_type)::text = ('ZRET'::character varying)::text
                                            )
                                            AND (
                                                (so.ord_reason)::text like ('X%'::character varying)::text
                                            )
                                        ) THEN so.net_value
                                        ELSE ((0)::numeric)::numeric(18, 0)
                                    END
                                ) AS zret,
                                sum(
                                    CASE
                                        WHEN (
                                            (so.doc_type)::text = ('ZDRT'::character varying)::text
                                        ) THEN so.net_value
                                        ELSE ((0)::numeric)::numeric(18, 0)
                                    END
                                ) AS zdrt,
                                0 AS return_qty,
                                0 AS copa_amt,
                                0 AS hidden_amt,
                                0 AS qty,
                                0 AS amt_obj_crncy
                            FROM
                                (
                                    (
                                        edw_sales_order_fact so
                                        JOIN edw_calendar_dim cal ON ((so.bill_dt = cal.cal_day))
                                    )
                                    JOIN
                                    ref_date_fiscal ON
                                    (
                                        (
                                            (
                                                try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) > ref_date_fiscal.threemnths_fisc_date
                                            )
                                            AND (
                                                try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) <= ref_date_fiscal.fisc_date
                                            )
                                        )
                                    )
                                )
                            WHERE (so.doc_type = 'ZDRT' OR so.doc_type = 'ZRET')
                            AND (so.sls_org = '1200' OR so.sls_org = '120S')
                            GROUP BY 1,
                                so.material,
                                so.division,
                                so.sls_org,
                                so.distr_chan,
                                6,
                                ref_date_fiscal.fisc_date,
                                cal.fisc_per,
                                9,
                                so.sold_to,
                                so.comp_cd
                        ) derived_table6
                    GROUP BY
                        derived_table6.sellin_data_set,
                        derived_table6.matl_num,
                        derived_table6.div,
                        derived_table6.sls_org,
                        derived_table6.dstr_chnl,
                        derived_table6.ctry_key,
                        derived_table6.fisc_date,
                        derived_table6.fisc_per,
                        derived_table6.obj_crncy_co_obj,
                        derived_table6.cust_num,
                        derived_table6.co_cd
                ) derived_table7
            GROUP BY derived_table7.sellin_data_set,
                derived_table7.matl_num,
                derived_table7.div,
                derived_table7.sls_org,
                derived_table7.dstr_chnl,
                derived_table7.ctry_key,
                derived_table7.fisc_date,
                derived_table7.fisc_per,
                derived_table7.obj_crncy_co_obj,
                derived_table7.cust_num,
                derived_table7.co_cd
        ) derived_table8
    GROUP BY derived_table8.sellin_data_set,
        derived_table8.matl_num,
        derived_table8.div,
        derived_table8.sls_org,
        derived_table8.dstr_chnl,
        derived_table8.ctry_key,
        derived_table8.fisc_date,
        derived_table8.fisc_per,
        derived_table8.obj_crncy_co_obj,
        derived_table8.cust_num,
        derived_table8.co_cd
),
sellin_l6m as
(
    SELECT
        derived_table12.sellin_data_set,
        derived_table12.matl_num,
        derived_table12.div,
        derived_table12.sls_org,
        derived_table12.dstr_chnl,
        derived_table12.ctry_key,
        derived_table12.fisc_date,
        derived_table12.fisc_per,
        derived_table12.obj_crncy_co_obj,
        derived_table12.cust_num,
        derived_table12.co_cd,
        0 AS copa_amt,
        0 AS copa_ret_amt,
        sum(derived_table12.qty) AS qty,
        0 AS copa_amt_l3m,
        0 AS copa_return_amt_l3m,
        0 AS qty_l3m,
        (
            (
                sum(derived_table12.copa_amt_l6m) - sum(derived_table12.hidden_amt)
            ) + sum(derived_table12.return_val)
        ) AS copa_amt_l6m,
        (
            sum(derived_table12.copa_return_amt_l6m) + sum(derived_table12.return_val)
        ) AS copa_return_amt_l6m,
        (
            sum(derived_table12.qty_l6m) + sum(derived_table12.return_qty)
        ) AS qty_l6m
    FROM
        (
            SELECT
                derived_table9.sellin_data_set,
                derived_table9.matl_num,
                derived_table9.div,
                derived_table9.sls_org,
                derived_table9.dstr_chnl,
                derived_table9.ctry_key,
                derived_table9.fisc_date,
                derived_table9.fisc_per,
                derived_table9.obj_crncy_co_obj,
                derived_table9.cust_num,
                derived_table9.co_cd,
                0 AS copa_amt,
                0 AS copa_ret_amt,
                sum(derived_table9.hidden_amt) AS hidden_amt,
                sum(derived_table9.return_val) AS return_val,
                sum(derived_table9.return_qty) AS return_qty,
                0 AS qty,
                0 AS copa_amt_l3m,
                0 AS copa_return_amt_l3m,
                0 AS qty_l3m,
                sum(derived_table9.copa_amt) AS copa_amt_l6m,
                sum(derived_table9.copa_ret_amt) AS copa_return_amt_l6m,
                sum(derived_table9.qty) AS qty_l6m
            FROM
                (
                    SELECT
                        'SELLIN_L6M'::character varying AS sellin_data_set,
                        fct.matl_num,
                        fct.div,
                        fct.sls_org,
                        fct.dstr_chnl,
                        fct.ctry_key,
                        ref_date_fiscal.fisc_date,
                        fct.fisc_yr_per AS fisc_per,
                        fct.obj_crncy_co_obj,
                        fct.cust_num,
                        fct.co_cd,
                        0 AS return_val,
                        0 AS return_qty,
                        sum(
                            CASE
                                WHEN (
                                    (fct.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS copa_amt,
                        sum(
                            CASE
                                WHEN (
                                    (fct.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS copa_ret_amt,
                        sum(
                            CASE
                                WHEN (
                                    (
                                        (
                                            (fct.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                                        )
                                        OR (
                                            (fct.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                                        )
                                    )
                                    OR (
                                        (fct.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                                    )
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS hidden_amt,
                        sum(fct.qty) AS qty,
                        sum(fct.amt_obj_crncy) AS amt_obj_crncy
                    FROM edw_copa_trans_fact fct,
                            ref_date_fiscal
                    WHERE
                        (
                            (
                                (
                                    fct.acct_hier_shrt_desc = 'GTS'
                                    OR fct.acct_hier_shrt_desc = 'HDPM'
                                    OR fct.acct_hier_shrt_desc = 'HDMD'
                                    OR fct.acct_hier_shrt_desc = 'HDPR'
                                )
                                AND fct.ctry_key = 'TW'
                                AND try_to_date(concat_ws('-',left(fct.fisc_yr_per,4),right(fct.fisc_yr_per,2),'01')) > ref_date_fiscal.sixmnths_fisc_date
                            )
                            AND try_to_date(concat_ws('-',left(fct.fisc_yr_per,4),right(fct.fisc_yr_per,2),'01')) <= ref_date_fiscal.fisc_date
                            AND fct.ctry_key = 'TW'
                        )

                    GROUP BY 1,
                        fct.matl_num,
                        fct.div,
                        fct.sls_org,
                        fct.dstr_chnl,
                        fct.ctry_key,
                        ref_date_fiscal.fisc_date,
                        fct.fisc_yr_per,
                        fct.obj_crncy_co_obj,
                        fct.cust_num,
                        fct.co_cd
                    UNION ALL
                    SELECT
                        'SELLIN_L6M'::character varying AS sellin_data_set,
                        fct.matl_num,
                        fct.div,
                        fct.sls_org,
                        fct.dstr_chnl,
                        fct.ctry_key,
                        ref_date_fiscal.fisc_date,
                        fct.fisc_yr_per AS fisc_per,
                        fct.obj_crncy_co_obj,
                        fct.cust_num,
                        fct.co_cd,
                        0 AS return_val,
                        0 AS return_qty,
                        sum(
                            CASE
                                WHEN (
                                    (fct.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS copa_amt,
                        sum(
                            CASE
                                WHEN (
                                    (fct.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS copa_ret_amt,
                        sum(
                            CASE
                                WHEN (
                                    (
                                        (
                                            (fct.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                                        )
                                        OR (
                                            (fct.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                                        )
                                    )
                                    OR (
                                        (fct.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                                    )
                                ) THEN fct.amt_obj_crncy
                                ELSE ((0)::numeric)::numeric(18, 0)
                            END
                        ) AS hidden_amt,
                        sum(fct.qty) AS qty,
                        sum(fct.amt_obj_crncy) AS amt_obj_crncy
                    FROM edw_copa_trans_fact fct,
                    ref_date_fiscal
                    WHERE
                        (
                            (fct.acct_hier_shrt_desc = 'RTN' AND fct.ctry_key = 'TW')
                            AND try_to_date(concat_ws('-',left(fct.fisc_yr_per,4),right(fct.fisc_yr_per,2),'01')) > ref_date_fiscal.sixmnths_fisc_date
                        )
                        AND try_to_date(concat_ws('-',left(fct.fisc_yr_per,4),right(fct.fisc_yr_per,2),'01')) <= ref_date_fiscal.fisc_date
                        AND fct.ctry_key = 'TW'

                    GROUP BY 1,
                        fct.matl_num,
                        fct.div,
                        fct.sls_org,
                        fct.dstr_chnl,
                        fct.ctry_key,
                        ref_date_fiscal.fisc_date,
                        fct.fisc_yr_per,
                        fct.obj_crncy_co_obj,
                        fct.cust_num,
                        fct.co_cd
                ) derived_table9
            GROUP BY derived_table9.sellin_data_set,
                derived_table9.matl_num,
                derived_table9.div,
                derived_table9.sls_org,
                derived_table9.dstr_chnl,
                derived_table9.ctry_key,
                derived_table9.fisc_date,
                derived_table9.fisc_per,
                derived_table9.obj_crncy_co_obj,
                derived_table9.cust_num,
                derived_table9.co_cd
            UNION ALL
            SELECT
                derived_table11.sellin_data_set,
                derived_table11.matl_num,
                derived_table11.div,
                derived_table11.sls_org,
                derived_table11.dstr_chnl,
                derived_table11.ctry_key,
                derived_table11.fisc_date,
                derived_table11.fisc_per,
                derived_table11.obj_crncy_co_obj,
                derived_table11.cust_num,
                derived_table11.co_cd,
                sum(derived_table11.copa_amt) AS copa_amt,
                sum(derived_table11.copa_return_amt) AS copa_return_amt,
                sum(derived_table11.hidden_amt) AS hidden_amt,
                sum(derived_table11.return_val) AS return_val,
                sum(derived_table11.return_qty) AS return_qty,
                sum(derived_table11.qty) AS qty,
                0 AS copa_amt_l3m,
                0 AS copa_return_amt_l3m,
                0 AS qty_l3m,
                0 AS copa_amt_l6m,
                0 AS copa_return_amt_l6m,
                0 AS qty_l6m
            FROM
                (
                    SELECT
                        'SELLIN_L6M'::character varying AS sellin_data_set,
                        so.material AS matl_num,
                        so.division AS div,
                        so.sls_org,
                        so.distr_chan AS dstr_chnl,
                        'TW'::character varying AS ctry_key,
                        ref_date_fiscal.fisc_date,
                        cal.fisc_per,
                        'TWD'::character varying AS obj_crncy_co_obj,
                        so.sold_to AS cust_num,
                        so.comp_cd AS co_cd,
                        0 AS return_val,
                        sum(so.cml_cf_qty) AS return_qty,
                        0 AS copa_amt,
                        0 AS copa_return_amt,
                        0 AS hidden_amt,
                        0 AS qty,
                        0 AS amt_obj_crncy
                    FROM edw_sales_order_fact so
                    JOIN edw_calendar_dim cal ON ((so.bill_dt = cal.cal_day))
                    JOIN ref_date_fiscal ON
                    (
                        (
                            (
                                try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) > ref_date_fiscal.sixmnths_fisc_date
                            )
                            AND (
                                try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) <= ref_date_fiscal.fisc_date
                            )
                        )
                    )
                    WHERE
                        (so.doc_type = 'ZRET' AND so.ord_reason LIKE 'X%')
                        AND (so.sls_org = '1200' OR so.sls_org = '120S')

                    GROUP BY 1,
                        so.material,
                        so.division,
                        so.sls_org,
                        so.distr_chan,
                        6,
                        ref_date_fiscal.fisc_date,
                        cal.fisc_per,
                        9,
                        so.sold_to,
                        so.comp_cd
                    UNION ALL
                    SELECT
                        derived_table10.sellin_data_set,
                        derived_table10.matl_num,
                        derived_table10.div,
                        derived_table10.sls_org,
                        derived_table10.dstr_chnl,
                        derived_table10.ctry_key,
                        derived_table10.fisc_date,
                        derived_table10.fisc_per,
                        derived_table10.obj_crncy_co_obj,
                        derived_table10.cust_num,
                        derived_table10.co_cd,
                        (
                            sum(
                                CASE
                                    WHEN (
                                        derived_table10.zret = ((0)::numeric)::numeric(18, 0)
                                    ) THEN ((0)::numeric)::numeric(18, 0)
                                    ELSE COALESCE(derived_table10.zdrt,null)
                                END
                            ) + sum(derived_table10.zret)
                        ) AS return_val,
                        sum(derived_table10.return_qty) AS return_qty,
                        0 AS copa_amt,
                        0 AS copa_return_amt,
                        0 AS hidden_amt,
                        0 AS qty,
                        0 AS amt_obj_crncy
                    FROM
                        (
                            SELECT
                                'SELLIN_L6M'::character varying AS sellin_data_set,
                                so.material AS matl_num,
                                so.division AS div,
                                so.sls_org,
                                so.distr_chan AS dstr_chnl,
                                'TW'::character varying AS ctry_key,
                                ref_date_fiscal.fisc_date,
                                cal.fisc_per,
                                'TWD'::character varying AS obj_crncy_co_obj,
                                so.sold_to AS cust_num,
                                so.comp_cd AS co_cd,
                                sum(
                                    CASE
                                        WHEN (
                                            (
                                                (so.doc_type)::text = ('ZRET'::character varying)::text
                                            )
                                            AND (
                                                (so.ord_reason)::text like ('X%'::character varying)::text
                                            )
                                        ) THEN so.net_value
                                        ELSE ((0)::numeric)::numeric(18, 0)
                                    END
                                ) AS zret,
                                sum(
                                    CASE
                                        WHEN (
                                            (so.doc_type)::text = ('ZDRT'::character varying)::text
                                        ) THEN so.net_value
                                        ELSE ((0)::numeric)::numeric(18, 0)
                                    END
                                ) AS zdrt,
                                0 AS return_qty,
                                0 AS copa_amt,
                                0 AS hidden_amt,
                                0 AS qty,
                                0 AS amt_obj_crncy
                            FROM
                            edw_sales_order_fact so
                            JOIN edw_calendar_dim cal ON ((so.bill_dt = cal.cal_day))
                                JOIN ref_date_fiscal ON
                                (
                                    (
                                        (
                                            try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) > ref_date_fiscal.sixmnths_fisc_date
                                        )
                                        AND (
                                            try_to_date(concat_ws('-',left(cal.fisc_per,4),right(cal.fisc_per,2),'01')) <= ref_date_fiscal.fisc_date
                                        )
                                    )
                                )
                            WHERE
                                (so.doc_type = 'ZDRT' OR so.doc_type = 'ZRET')
                                AND (so.sls_org = '1200' OR so.sls_org = '120S')
                            GROUP BY 1,
                                so.material,
                                so.division,
                                so.sls_org,
                                so.distr_chan,
                                6,
                                ref_date_fiscal.fisc_date,
                                cal.fisc_per,
                                9,
                                so.sold_to,
                                so.comp_cd
                        ) derived_table10
                    GROUP BY derived_table10.sellin_data_set,
                        derived_table10.matl_num,
                        derived_table10.div,
                        derived_table10.sls_org,
                        derived_table10.dstr_chnl,
                        derived_table10.ctry_key,
                        derived_table10.fisc_date,
                        derived_table10.fisc_per,
                        derived_table10.obj_crncy_co_obj,
                        derived_table10.cust_num,
                        derived_table10.co_cd
                ) derived_table11
            GROUP BY derived_table11.sellin_data_set,
                derived_table11.matl_num,
                derived_table11.div,
                derived_table11.sls_org,
                derived_table11.dstr_chnl,
                derived_table11.ctry_key,
                derived_table11.fisc_date,
                derived_table11.fisc_per,
                derived_table11.obj_crncy_co_obj,
                derived_table11.cust_num,
                derived_table11.co_cd
        ) derived_table12
    GROUP BY derived_table12.sellin_data_set,
        derived_table12.matl_num,
        derived_table12.div,
        derived_table12.sls_org,
        derived_table12.dstr_chnl,
        derived_table12.ctry_key,
        derived_table12.fisc_date,
        derived_table12.fisc_per,
        derived_table12.obj_crncy_co_obj,
        derived_table12.cust_num,
        derived_table12.co_cd
),
ectf as
(

    select * from sellin_cm
    UNION ALL
    select * from sellin_l3m
    UNION ALL
    select * from sellin_l6m
),
sellin as
(
    SELECT
            'SELLIN'::character varying AS data_set,
            (
                "substring"(((ectf.fisc_date)::character varying)::text,1,4)
                || "substring"(((ectf.fisc_date)::character varying)::text,6,2)
            )::character varying AS fisc_per,
            (
                "substring"(((ectf.fisc_date)::character varying)::text,1,4)
            )::character varying AS fisc_yr,
            CASE
                WHEN (
                    (
                        upper((ectf.ctry_key)::text) = ('TW'::character varying)::text
                    )
                    AND (
                        (epad.prod_hier_l1 IS NULL)
                        OR (
                            trim((epad.prod_hier_l1)::text) = (''::character varying)::text
                        )
                    )
                ) THEN 'Taiwan'::character varying
                ELSE COALESCE(epad.prod_hier_l1, '#'::character varying)
            END AS prod_hier_l1,
            COALESCE(epad.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
            COALESCE(epad.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
            COALESCE(epad.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
            COALESCE(epad.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
            COALESCE(epad.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
            COALESCE(epad.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
            COALESCE(epad.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
            COALESCE(epad.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
            (
                ltrim(
                    (ectf.matl_num)::text,
                    ((0)::character varying)::text
                )
            )::character varying AS sap_matl_num,
            CASE
                WHEN (
                    (ecafd.channel IS NULL)
                    OR (
                        trim((ecafd.channel)::text) = (''::character varying)::text
                    )
                ) THEN 'Not Available'::character varying
                ELSE ecafd.channel
            END AS channel,
            CASE
                WHEN (
                    (ecafd.sls_grp IS NULL)
                    OR (
                        trim((ecafd.sls_grp)::text) = (''::character varying)::text
                    )
                ) THEN 'Not Available'::character varying
                ELSE ecafd.sls_grp
            END AS sls_grp,
            CASE
                WHEN (
                    (ecahd.strategy_customer_hierachy_name IS NULL)
                    OR (
                        trim((ecahd.strategy_customer_hierachy_name)::text) = (''::character varying)::text
                    )
                ) THEN 'Not Available'::character varying
                ELSE ecahd.strategy_customer_hierachy_name
            END AS strategy_customer_hierachy_name,
            vice.to_crncy,
            vice.ex_rt,
            emsd.ean_num,
            sum(ectf.copa_amt) AS sls_amt,
            (abs(sum(ectf.copa_ret_amt)))::numeric(38, 5) AS sls_return_amt,
            sum(ectf.qty) AS sls_qty,
            sum(ectf.copa_amt_l3m) AS sls_amt_l3m,
            (abs(sum(ectf.copa_return_amt_l3m)))::numeric(38, 5) AS sls_return_amt_13m,
            sum(ectf.qty_l3m) AS sls_qty_l3m,
            sum(ectf.copa_amt_l6m) AS sls_amt_l6m,
            (abs(sum(ectf.copa_return_amt_l6m)))::numeric(38, 5) AS sls_return_amt_l6m,
            sum(ectf.qty_l6m) AS sls_qty_l6m,
            0 AS rf_amount_si,
            0 AS rf_qty_si,
            0 AS rf_amount_so,
            0 AS rf_qty_so,
            0 AS rf_amount_si_l2m,
            0 AS rf_qty_si_l2m,
            0 AS rf_amount_so_l2m,
            0 AS rf_qty_so_l2m
        FROM ectf
        LEFT JOIN v_intrm_crncy_exch vice ON
        (ectf.obj_crncy_co_obj)::text = (vice.from_crncy)::text
        LEFT JOIN
        (
        SELECT edw_material_sales_dim.sls_org,
            edw_material_sales_dim.dstr_chnl,
            edw_material_sales_dim.matl_num,
            edw_material_sales_dim.ean_num
        FROM edw_material_sales_dim
        WHERE (
                (edw_material_sales_dim.ean_num)::text <> (''::character varying)::text
            )
        ) emsd ON
        ltrim(ectf.matl_num::text, '0') = ltrim(emsd.matl_num::text, '0')
        AND COALESCE(ectf.sls_org, '#')::text = COALESCE(emsd.sls_org, '#')::text
        AND COALESCE(ectf.dstr_chnl, '#')::text = COALESCE(emsd.dstr_chnl, '#')::text
        LEFT JOIN edw_product_attr_dim epad
        ON emsd.ean_num::text = epad.aw_remote_key::text
        AND ectf.ctry_key::text = epad.cntry::text
        LEFT JOIN
        (
        SELECT DISTINCT edw_customer_attr_flat_dim.aw_remote_key AS sold_to_party,
        edw_customer_attr_flat_dim.channel,
        edw_customer_attr_flat_dim.sls_grp,
        edw_customer_attr_flat_dim.sls_ofc_desc,
        edw_customer_attr_flat_dim.store_typ
        FROM edw_customer_attr_flat_dim
        ) ecafd ON
        ltrim(ectf.cust_num::text, '0') = ltrim(ecafd.sold_to_party::text, '0')
        LEFT JOIN
        (
        SELECT DISTINCT edw_customer_attr_hier_dim.sold_to_party,
        edw_customer_attr_hier_dim.strategy_customer_hierachy_name
        FROM edw_customer_attr_hier_dim
        ) ecahd ON ltrim(ectf.cust_num::text, '0') = ltrim(ecahd.sold_to_party::text, '0')
        WHERE
            ectf.fisc_date IN
        (
            SELECT DISTINCT
                try_to_date(concat_ws('-',left(edw_copa_trans_fact.fisc_yr_per,4),right(edw_copa_trans_fact.fisc_yr_per,2),'01')) AS to_date
            FROM
                edw_copa_trans_fact
            WHERE
                edw_copa_trans_fact.ctry_key = 'TW'::character varying
                AND edw_copa_trans_fact.acct_hier_shrt_desc = 'GTS'::character varying
        )
        AND substring(ectf.fisc_date::character varying, 1, 4) >
        (date_part(year, convert_timezone('UTC', current_timestamp())::timestamp without time zone) - 3)::text
        GROUP BY 1,
            (
                "substring"(
                    ((ectf.fisc_date)::character varying)::text,
                    1,
                    4
                ) || "substring"(
                    ((ectf.fisc_date)::character varying)::text,
                    6,
                    2
                )
            ),
            "substring"(
                ((ectf.fisc_date)::character varying)::text,
                1,
                4
            ),
            ectf.ctry_key,
            epad.prod_hier_l1,
            epad.prod_hier_l2,
            epad.prod_hier_l3,
            epad.prod_hier_l4,
            epad.prod_hier_l5,
            epad.prod_hier_l6,
            epad.prod_hier_l7,
            epad.prod_hier_l8,
            epad.prod_hier_l9,
            ectf.matl_num,
            ecafd.channel,
            ecafd.sls_grp,
            ecahd.strategy_customer_hierachy_name,
            vice.to_crncy,
            vice.ex_rt,
            emsd.ean_num
        HAVING
        (
            (
                sum(COALESCE(ectf.copa_amt, 0)) <> 0
                OR sum(COALESCE(ectf.copa_amt_l3m, 0)) <> 0
            )
            OR sum(COALESCE(ectf.qty_l3m, 0)) <> 0
        )
        OR sum(COALESCE(ectf.copa_amt_l6m, 0)) <> 0
        OR sum(COALESCE(ectf.qty_l6m, 0)) <> 0
),
sellout_cm as
(
    SELECT
        'SELLOUT_CM'::character varying AS sellout_data_set,
        derived_table13.ims_txn_dt AS fisc_date,
        derived_table13.dstr_cd,
        derived_table13.ean_num,
        derived_table13.crncy_cd,
        derived_table13.ctry_cd,
        (
            sum(derived_table13.sls_amt) - sum(derived_table13.rtrn_amt)
        ) AS sls_amt,
        sum(derived_table13.rtrn_amt) AS sls_ret_amt,
        (
            sum(derived_table13.sls_qty) - sum(derived_table13.rtrn_qty)
        ) AS sls_qty,
        0 AS sls_amt_l3m,
        0 AS sls_return_amt_13m,
        0 AS sls_qty_l3m,
        0 AS sls_amt_l6m,
        0 AS sls_return_amt_l6m,
        0 AS sls_qty_l6m
    FROM
        (
            SELECT
                b.fisc_per,
                a.ims_txn_dt,
                a.dstr_cd,
                a.dstr_nm,
                a.cust_cd,
                a.cust_nm,
                a.prod_cd,
                a.prod_nm,
                a.ean_num,
                a.ship_cust_nm,
                a.cust_cls_grp,
                a.cust_sub_cls,
                a.sls_ofc_cd,
                a.sls_grp_cd,
                a.sls_ofc_nm,
                a.sls_grp_nm,
                a.acc_type,
                a.co_cd,
                a.sls_rep_cd,
                a.sls_rep_nm,
                a.crncy_cd,
                a.ctry_cd,
                a.sls_amt,
                a.sls_qty,
                a.rtrn_amt,
                a.rtrn_qty
            FROM edw_ims_fact a
            LEFT JOIN edw_calendar_dim b ON ((b.cal_day = a.ims_txn_dt))
            WHERE (
                    (a.ctry_cd)::text = ('TW'::character varying)::text
                )
        ) derived_table13
    GROUP BY 1,
        derived_table13.ims_txn_dt,
        derived_table13.dstr_cd,
        derived_table13.ean_num,
        derived_table13.crncy_cd,
        derived_table13.ctry_cd
),
ref_date as
(
    SELECT DISTINCT to_date(
            "left"(
                ((edw_calendar_dim.cal_day)::character varying)::text,
                7
            ),
            ('YYYY-MM'::character varying)::text
        ) AS cal_date,
        dateadd(
            month,
            (- (3)::bigint),
            (
                to_date(
                    "left"(
                        ((edw_calendar_dim.cal_day)::character varying)::text,
                        7
                    ),
                    ('YYYY-MM'::character varying)::text
                )
            )::timestamp without time zone
        ) AS threemnths_cal_date,
        dateadd(
            month,
            (- (6)::bigint),
            (
                to_date(
                    "left"(
                        ((edw_calendar_dim.cal_day)::character varying)::text,
                        7
                    ),
                    ('YYYY-MM'::character varying)::text
                )
            )::timestamp without time zone
        ) AS sixmnths_cal_date,
        dateadd(
            month,
            (- (2)::bigint),
            (
                to_date(
                    "left"(
                        ((edw_calendar_dim.cal_day)::character varying)::text,
                        7
                    ),
                    ('YYYY-MM'::character varying)::text
                )
            )::timestamp without time zone
        ) AS twomnths_cal_date
    FROM edw_calendar_dim
),
sellout_l3m as
(
    SELECT
        'SELLOUT_L3M'::character varying AS sellout_data_set,
        ref_date.cal_date,
        so_fct.dstr_cd,
        so_fct.ean_num,
        so_fct.crncy_cd,
        so_fct.ctry_cd,
        0 AS sls_amt,
        0 AS sls_qty,
        0 AS sls_ret_amt,
        (sum(so_fct.sls_amt) - sum(so_fct.rtrn_amt)) AS sls_amt_l3m,
        (sum(so_fct.rtrn_amt))::numeric(38, 5) AS sls_return_amt_13m,
        (sum(so_fct.sls_qty) - sum(so_fct.rtrn_qty)) AS sls_qty_l3m,
        0 AS sls_amt_l6m,
        0 AS sls_return_amt_l6m,
        0 AS sls_qty_l6m
    FROM
        (
            SELECT
                b.fisc_per,
                to_date(
                    "left"(((a.ims_txn_dt)::character varying)::text, 7),
                    ('YYYY-MM'::character varying)::text
                ) AS ims_txn_dt,
                a.dstr_cd,
                a.dstr_nm,
                a.cust_cd,
                a.cust_nm,
                a.prod_cd,
                a.prod_nm,
                a.ean_num,
                a.ship_cust_nm,
                a.cust_cls_grp,
                a.cust_sub_cls,
                a.sls_ofc_cd,
                a.sls_grp_cd,
                a.sls_ofc_nm,
                a.sls_grp_nm,
                a.acc_type,
                a.co_cd,
                a.sls_rep_cd,
                a.sls_rep_nm,
                a.crncy_cd,
                a.ctry_cd,
                a.sls_amt,
                a.sls_qty,
                a.rtrn_amt,
                a.rtrn_qty
            FROM edw_ims_fact a
            LEFT JOIN edw_calendar_dim b ON (
                (
                    b.cal_day = to_date(
                        "left"(((a.ims_txn_dt)::character varying)::text, 7),
                        ('YYYY-MM'::character varying)::text
                    )
                )
            )
            WHERE (
                    (a.ctry_cd)::text = ('TW'::character varying)::text
                )
        ) so_fct,
        ref_date
    WHERE
        (
            (so_fct.ims_txn_dt > ref_date.threemnths_cal_date)
            AND (so_fct.ims_txn_dt <= ref_date.cal_date)
        )
    GROUP BY 1,
        ref_date.cal_date,
        so_fct.dstr_cd,
        so_fct.ean_num,
        so_fct.crncy_cd,
        so_fct.ctry_cd
),
sellout_l6m as
(
    SELECT
        'SELLOUT_L6M'::character varying AS sellout_data_set,
        ref_date.cal_date,
        so_fct.dstr_cd,
        so_fct.ean_num,
        so_fct.crncy_cd,
        so_fct.ctry_cd,
        0 AS sls_amt,
        0 AS sls_qty,
        0 AS sls_ret_amt,
        0 AS sls_amt_l3m,
        0 AS sls_return_amt_13m,
        0 AS sls_qty_l3m,
        (sum(so_fct.sls_amt) - sum(so_fct.rtrn_amt)) AS sls_amt_l6m,
        (sum(so_fct.rtrn_amt))::numeric(38, 5) AS sls_return_amt_l6m,
        (sum(so_fct.sls_qty) - sum(so_fct.rtrn_qty)) AS sls_qty_l6m
    FROM
        (
            SELECT b.fisc_per,
                to_date(
                    "left"(((a.ims_txn_dt)::character varying)::text, 7),
                    ('YYYY-MM'::character varying)::text
                ) AS ims_txn_dt,
                a.dstr_cd,
                a.dstr_nm,
                a.cust_cd,
                a.cust_nm,
                a.prod_cd,
                a.prod_nm,
                a.ean_num,
                a.ship_cust_nm,
                a.cust_cls_grp,
                a.cust_sub_cls,
                a.sls_ofc_cd,
                a.sls_grp_cd,
                a.sls_ofc_nm,
                a.sls_grp_nm,
                a.acc_type,
                a.co_cd,
                a.sls_rep_cd,
                a.sls_rep_nm,
                a.crncy_cd,
                a.ctry_cd,
                a.sls_amt,
                a.sls_qty,
                a.rtrn_amt,
                a.rtrn_qty
            FROM edw_ims_fact a
            LEFT JOIN edw_calendar_dim b ON (
                (
                    b.cal_day = to_date(
                        "left"(((a.ims_txn_dt)::character varying)::text, 7),
                        ('YYYY-MM'::character varying)::text
                    )
                )
            )
            WHERE (
                    (a.ctry_cd)::text = ('TW'::character varying)::text
                )
        ) so_fct,
        ref_date
    WHERE (
            (so_fct.ims_txn_dt > ref_date.sixmnths_cal_date)
            AND (so_fct.ims_txn_dt <= ref_date.cal_date)
        )
    GROUP BY 1,
        ref_date.cal_date,
        so_fct.dstr_cd,
        so_fct.ean_num,
        so_fct.crncy_cd,
        so_fct.ctry_cd
),
eif as
(
    select * from sellout_cm
    UNION ALL
    select * from sellout_l3m
    UNION ALL
    select * from sellout_l6m

),
sellout as
(
    SELECT
        'SELLOUT'::character varying AS data_set,
        (
            (
                "substring"(((eif.fisc_date)::character varying)::text, 1, 4) || "substring"(((eif.fisc_date)::character varying)::text, 6, 2)
            )
        )::character varying AS fisc_per,
        (
            "substring"(((eif.fisc_date)::character varying)::text, 1, 4)
        )::character varying AS fisc_yr,
        CASE
            WHEN (
                (
                    upper((eif.ctry_cd)::text) = ('TW'::character varying)::text
                )
                AND (
                    (epad.prod_hier_l1 IS NULL)
                    OR (
                        trim((epad.prod_hier_l1)::text) = (''::character varying)::text
                    )
                )
            ) THEN 'Taiwan'::character varying
            ELSE COALESCE(epad.prod_hier_l1, '#'::character varying)
        END AS prod_hier_l1,
        COALESCE(epad.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
        COALESCE(epad.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
        COALESCE(epad.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
        COALESCE(epad.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
        COALESCE(epad.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
        COALESCE(epad.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
        COALESCE(epad.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
        COALESCE(epad.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
        epad.sap_matl_num,
        (
            CASE
                WHEN (
                    (ecafd.channel IS NULL)
                    OR (
                        trim(ecafd.channel) = (''::character varying)::text
                    )
                ) THEN ('Not Available'::character varying)::text
                ELSE ecafd.channel
            END
        )::character varying AS channel,
        (
            CASE
                WHEN (
                    (ecafd.sls_grp IS NULL)
                    OR (
                        trim(ecafd.sls_grp) = (''::character varying)::text
                    )
                ) THEN ('Not Available'::character varying)::text
                ELSE ecafd.sls_grp
            END
        )::character varying AS sls_grp,
        CASE
            WHEN (
                (ecahd.strategy_customer_hierachy_name IS NULL)
                OR (
                    trim((ecahd.strategy_customer_hierachy_name)::text) = (''::character varying)::text
                )
            ) THEN 'Not Available'::character varying
            ELSE ecahd.strategy_customer_hierachy_name
        END AS strategy_customer_hierachy_name,
        vice.to_crncy,
        vice.ex_rt,
        eif.ean_num,
        sum(eif.sls_amt) AS sls_amt,
        abs(sum(eif.sls_ret_amt)) AS sls_return_amt,
        sum(eif.sls_qty) AS sls_qty,
        sum(eif.sls_amt_l3m) AS sls_amt_l3m,
        (abs(sum(eif.sls_return_amt_13m)))::numeric(38, 5) AS sls_return_amt_13m,
        sum(eif.sls_qty_l3m) AS sls_qty_l3m,
        sum(eif.sls_amt_l6m) AS sls_amt_l6m,
        (abs(sum(eif.sls_return_amt_l6m)))::numeric(38, 5) AS sls_return_amt_l6m,
        sum(eif.sls_qty_l6m) AS sls_qty_l6m,
        0 AS rf_amount_si,
        0 AS rf_qty_si,
        0 AS rf_amount_so,
        0 AS rf_qty_so,
        0 AS rf_amount_si_l2m,
        0 AS rf_qty_si_l2m,
        0 AS rf_amount_so_l2m,
        0 AS rf_qty_so_l2m
    FROM eif
    LEFT JOIN
    (
        SELECT DISTINCT edw_product_attr_dim.ean,
            edw_product_attr_dim.cntry,
            edw_product_attr_dim.sap_matl_num,
            edw_product_attr_dim.prod_hier_l1,
            edw_product_attr_dim.prod_hier_l2,
            edw_product_attr_dim.prod_hier_l3,
            edw_product_attr_dim.prod_hier_l4,
            edw_product_attr_dim.prod_hier_l5,
            edw_product_attr_dim.prod_hier_l6,
            edw_product_attr_dim.prod_hier_l7,
            edw_product_attr_dim.prod_hier_l8,
            edw_product_attr_dim.prod_hier_l9,
            edw_product_attr_dim.lcl_prod_nm
        FROM edw_product_attr_dim
    ) epad ON
    (
        (
            ((eif.ean_num)::text = (epad.ean)::text)
            AND ((epad.cntry)::text = (eif.ctry_cd)::text)
        )
    )
    LEFT JOIN
    (
        SELECT edw_customer_attr_flat_dim.sold_to_party,
            "max"((edw_customer_attr_flat_dim.channel)::text) AS channel,
            "max"((edw_customer_attr_flat_dim.sls_grp)::text) AS sls_grp,
            "max"((edw_customer_attr_flat_dim.store_typ)::text) AS store_typ,
            "max"((edw_customer_attr_flat_dim.sls_ofc_desc)::text) AS sls_ofc_desc
        FROM edw_customer_attr_flat_dim
        GROUP BY edw_customer_attr_flat_dim.sold_to_party
    ) ecafd ON
    (
        (
            (eif.dstr_cd)::text = (ecafd.sold_to_party)::text
        )
    )
    LEFT JOIN
    (
        SELECT DISTINCT edw_customer_attr_hier_dim.sold_to_party,
            edw_customer_attr_hier_dim.strategy_customer_hierachy_name
        FROM edw_customer_attr_hier_dim
    ) ecahd ON
    (
        (
            ltrim(
                (eif.dstr_cd)::text,
                ((0)::character varying)::text
            ) = ltrim(
                (ecahd.sold_to_party)::text,
                ((0)::character varying)::text
            )
        )
    )
    LEFT JOIN v_intrm_crncy_exch vice ON (((eif.crncy_cd)::text = (vice.from_crncy)::text))
    WHERE
        (
            "substring"(((eif.fisc_date)::character varying)::text, 1, 4) > (
                date_part(year,convert_timezone('UTC', current_timestamp())::timestamp) - 3
            )::text
        )
        AND
        (
            eif.fisc_date IN (
                SELECT DISTINCT a.cal_day
                FROM edw_calendar_dim a
                JOIN edw_ims_fact b ON a.cal_day = b.ims_txn_dt
                WHERE b.ctry_cd = 'TW'
            )
        )

    GROUP BY 1,
        (
            "substring"(((eif.fisc_date)::character varying)::text, 1, 4) || "substring"(((eif.fisc_date)::character varying)::text, 6, 2)
        ),
        "substring"(((eif.fisc_date)::character varying)::text, 1, 4),
        eif.ctry_cd,
        epad.prod_hier_l1,
        epad.prod_hier_l2,
        epad.prod_hier_l3,
        epad.prod_hier_l4,
        epad.prod_hier_l5,
        epad.prod_hier_l6,
        epad.prod_hier_l7,
        epad.prod_hier_l8,
        epad.prod_hier_l9,
        epad.sap_matl_num,
        ecafd.channel,
        ecafd.sls_grp,
        ecahd.strategy_customer_hierachy_name,
        vice.to_crncy,
        vice.ex_rt,
        eif.ean_num
    HAVING
    (
        (
            sum(COALESCE(eif.sls_amt, 0)) <> 0
            OR sum(COALESCE(eif.sls_qty, 0)) <> 0
            OR sum(COALESCE(eif.sls_amt_l3m, 0)) <> 0
            OR sum(COALESCE(eif.sls_qty_l3m, 0)) <> 0
            OR sum(COALESCE(eif.sls_amt_l6m, 0)) <> 0
            OR sum(COALESCE(eif.sls_qty_l6m, 0)) <> 0
        )
    )
),
pos_cm as
(
    SELECT
        ' POS_CM '::character varying AS pos_data_set,
        derived_table14.pos_dt AS fisc_date,
        derived_table14.sls_grp,
        derived_table14.sold_to_party,
        derived_table14.str_cd,
        derived_table14.ean_num,
        derived_table14.crncy_cd,
        derived_table14.ctry_cd,
        sum(derived_table14.prom_sls_amt) AS sls_amt,
        sum(derived_table14.prom_sls_return_amt) AS sls_return_amt,
        sum(derived_table14.sls_qty) AS sls_qty,
        0 AS sls_amt_l3m,
        0 AS sls_return_amt_l3m,
        0 AS sls_qty_l3m,
        0 AS sls_amt_l6m,
        0 AS sls_return_amt_l6m,
        0 AS sls_qty_l6m
    FROM
        (
            SELECT
                b.fisc_per,
                a.pos_dt,
                a.vend_cd,
                a.vend_nm,
                a.prod_nm,
                a.vend_prod_cd,
                a.vend_prod_nm,
                a.brnd_nm,
                a.ean_num,
                a.str_cd,
                a.str_nm,
                a.sls_qty,
                a.sls_amt,
                a.prom_sls_amt,
                CASE
                    WHEN (
                        a.prom_sls_amt < (((0)::numeric)::numeric(18, 0))::numeric(16, 5)
                    ) THEN a.prom_sls_amt
                    ELSE ((0)::numeric)::numeric(18, 0)
                END AS prom_sls_return_amt,
                a.unit_prc_amt,
                a.invnt_qty,
                a.invnt_amt,
                a.invnt_dt,
                a.crncy_cd,
                a.src_sys_cd,
                a.ctry_cd,
                a.sold_to_party,
                a.sls_grp,
                a.mysls_brnd_nm,
                a.mysls_catg,
                a.matl_num,
                a.matl_desc,
                a.prom_prc_amt
            FROM edw_pos_fact a
            LEFT JOIN edw_calendar_dim b ON ((b.cal_day = a.pos_dt))
            WHERE (
                    (a.ctry_cd)::text = ('TW'::character varying)::text
                )
        ) derived_table14
    GROUP BY 1,
        derived_table14.pos_dt,
        derived_table14.sls_grp,
        derived_table14.sold_to_party,
        derived_table14.str_cd,
        derived_table14.ean_num,
        derived_table14.crncy_cd,
        derived_table14.ctry_cd
),
pos_l3m as
(
    SELECT
        ' POS_L3M '::character varying AS pos_data_set,
        ref_date.cal_date,
        pos_fct.sls_grp,
        pos_fct.sold_to_party,
        pos_fct.str_cd,
        pos_fct.ean_num,
        pos_fct.crncy_cd,
        pos_fct.ctry_cd,
        0 AS sls_amt,
        0 AS sls_return_amt,
        0 AS sls_qty,
        sum(pos_fct.prom_sls_amt) AS sls_amt_l3m,
        (sum(pos_fct.prom_sls_return_amt))::numeric(38, 5) AS sls_return_amt_l3m,
        sum(pos_fct.sls_qty) AS sls_qty_l3m,
        0 AS sls_amt_l6m,
        0 AS sls_return_amt_l6m,
        0 AS sls_qty_l6m
    FROM
        (
            SELECT
                b.fisc_per,
                to_date(
                    "left"(((a.pos_dt)::character varying)::text, 7),
                    ('YYYY-MM'::character varying)::text
                ) AS pos_dt,
                a.vend_cd,
                a.vend_nm,
                a.prod_nm,
                a.vend_prod_cd,
                a.vend_prod_nm,
                a.brnd_nm,
                a.ean_num,
                a.str_cd,
                a.str_nm,
                a.sls_qty,
                a.sls_amt,
                a.prom_sls_amt,
                CASE
                    WHEN (
                        a.prom_sls_amt < (((0)::numeric)::numeric(18, 0))::numeric(16, 5)
                    ) THEN a.prom_sls_amt
                    ELSE ((0)::numeric)::numeric(18, 0)
                END AS prom_sls_return_amt,
                a.unit_prc_amt,
                a.invnt_qty,
                a.invnt_amt,
                a.invnt_dt,
                a.crncy_cd,
                a.src_sys_cd,
                a.ctry_cd,
                a.sold_to_party,
                a.sls_grp,
                a.mysls_brnd_nm,
                a.mysls_catg,
                a.matl_num,
                a.matl_desc,
                a.prom_prc_amt
            FROM edw_pos_fact a
            LEFT JOIN edw_calendar_dim b ON (
                (
                    b.cal_day = to_date(
                        "left"(((a.pos_dt)::character varying)::text, 7),
                        ('YYYY-MM'::character varying)::text
                    )
                )
            )
            WHERE (
                    (a.ctry_cd)::text = ('TW'::character varying)::text
                )
        ) pos_fct,
        ref_date
    WHERE (
            (pos_fct.pos_dt > ref_date.threemnths_cal_date)
            AND (pos_fct.pos_dt <= ref_date.cal_date)
        )
    GROUP BY 1,
        ref_date.cal_date,
        pos_fct.sls_grp,
        pos_fct.sold_to_party,
        pos_fct.str_cd,
        pos_fct.ean_num,
        pos_fct.crncy_cd,
        pos_fct.ctry_cd
),
pos_l6m as
(
    SELECT
        ' POS_L6M '::character varying AS pos_data_set,
        ref_date.cal_date,
        pos_fct.sls_grp,
        pos_fct.sold_to_party,
        pos_fct.str_cd,
        pos_fct.ean_num,
        pos_fct.crncy_cd,
        pos_fct.ctry_cd,
        0 AS sls_amt,
        0 AS sls_return_amt,
        0 AS sls_qty,
        0 AS sls_amt_l3m,
        0 AS sls_qty_l3m,
        0 AS sls_return_amt_l3m,
        sum(pos_fct.prom_sls_amt) AS sls_amt_l6m,
        (sum(pos_fct.prom_sls_return_amt))::numeric(38, 5) AS sls_return_amt_l6m,
        sum(pos_fct.sls_qty) AS sls_qty_l6m
    FROM
        (
            SELECT
                b.fisc_per,
                to_date(
                    "left"(((a.pos_dt)::character varying)::text, 7),
                    ('YYYY-MM'::character varying)::text
                ) AS pos_dt,
                a.vend_cd,
                a.vend_nm,
                a.prod_nm,
                a.vend_prod_cd,
                a.vend_prod_nm,
                a.brnd_nm,
                a.ean_num,
                a.str_cd,
                a.str_nm,
                a.sls_qty,
                a.sls_amt,
                a.prom_sls_amt,
                CASE
                    WHEN (
                        a.prom_sls_amt < (((0)::numeric)::numeric(18, 0))::numeric(16, 5)
                    ) THEN a.prom_sls_amt
                    ELSE ((0)::numeric)::numeric(18, 0)
                END AS prom_sls_return_amt,
                a.unit_prc_amt,
                a.invnt_qty,
                a.invnt_amt,
                a.invnt_dt,
                a.crncy_cd,
                a.src_sys_cd,
                a.ctry_cd,
                a.sold_to_party,
                a.sls_grp,
                a.mysls_brnd_nm,
                a.mysls_catg,
                a.matl_num,
                a.matl_desc,
                a.prom_prc_amt
            FROM
            edw_pos_fact a
            LEFT JOIN edw_calendar_dim b ON (
                (
                    b.cal_day = to_date(
                        "left"(((a.pos_dt)::character varying)::text, 7),
                        ('YYYY-MM'::character varying)::text
                    )
                )
            )
            WHERE (
                    (a.ctry_cd)::text = (' TW '::character varying)::text
                )
        ) pos_fct,
        ref_date
    WHERE (
            (pos_fct.pos_dt > ref_date.sixmnths_cal_date)
            AND (pos_fct.pos_dt <= ref_date.cal_date)
        )
    GROUP BY 1,
        ref_date.cal_date,
        pos_fct.sls_grp,
        pos_fct.sold_to_party,
        pos_fct.str_cd,
        pos_fct.ean_num,
        pos_fct.crncy_cd,
        pos_fct.ctry_cd
),
epf as
(
    select * from pos_cm
    union all
    select * from pos_l3m
    union all
    select * from pos_l6m
),
pos as
(
    SELECT
        'POS'::character varying AS data_set,
        (
            (
                "substring"(((epf.fisc_date)::character varying)::text, 1, 4) || "substring"(((epf.fisc_date)::character varying)::text, 6, 2)
            )
        )::character varying AS fisc_per,
        (
            "substring"(((epf.fisc_date)::character varying)::text, 1, 4)
        )::character varying AS fisc_yr,
        CASE
            WHEN (
                (
                    upper((epf.ctry_cd)::text) = ('TW'::character varying)::text
                )
                AND (
                    (epad.prod_hier_l1 IS NULL)
                    OR (
                        trim((epad.prod_hier_l1)::text) = (''::character varying)::text
                    )
                )
            ) THEN 'Taiwan'::character varying
            ELSE COALESCE(epad.prod_hier_l1, '#'::character varying)
        END AS prod_hier_l1,
        COALESCE(epad.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
        COALESCE(epad.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
        COALESCE(epad.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
        COALESCE(epad.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
        COALESCE(epad.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
        COALESCE(epad.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
        COALESCE(epad.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
        COALESCE(epad.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
        epad.sap_matl_num,
        CASE
            WHEN (
                (ecafd.channel IS NULL)
                OR (
                    trim((ecafd.channel)::text) = (''::character varying)::text
                )
            ) THEN 'Not Available'::character varying
            ELSE ecafd.channel
        END AS channel,
        CASE
            WHEN (
                (epf.sls_grp IS NULL)
                OR (
                    trim((epf.sls_grp)::text) = (''::character varying)::text
                )
            ) THEN 'Not Available'::character varying
            WHEN (
                trim((epf.sls_grp)::text) = ('7-11'::character varying)::text
            ) THEN '7-ELEVEN'::character varying
            WHEN (
                trim((epf.sls_grp)::text) = ('Carrefour 家樂福'::character varying)::text
            ) THEN 'Carrefour'::character varying
            WHEN (
                trim((epf.sls_grp)::text) = ('Cosmed 康是美'::character varying)::text
            ) THEN 'Cosmed'::character varying
            WHEN (
                trim((epf.sls_grp)::text) = ('EC'::character varying)::text
            ) THEN 'EC'::character varying
            WHEN (
                trim((epf.sls_grp)::text) = ('Poya 寶雅'::character varying)::text
            ) THEN 'Pao Ya Ltd'::character varying
            WHEN (
                trim((epf.sls_grp)::text) = ('PX 全聯'::character varying)::text
            ) THEN 'PX'::character varying
            WHEN (
                trim((epf.sls_grp)::text) = ('RT-Mart 大潤發'::character varying)::text
            ) THEN 'Auchan(ASIA/Pacific)'::character varying
            WHEN (
                trim((epf.sls_grp)::text) = ('Watsons 屈臣氏'::character varying)::text

            ) THEN 'AS WATSON\' S '::character varying
            ELSE epf.sls_grp
        END AS sls_grp,
        CASE
            WHEN (
                (epf.sls_grp IS NULL)
                OR (
                    trim((epf.sls_grp)::text) = (''::character varying)::text
                )
            ) THEN ' NOT Available '::character varying
            WHEN (
                trim((epf.sls_grp)::text) = (' 7 -11 '::character varying)::text
            ) THEN ' 7 - ELEVEN (TW) '::character varying
            WHEN (
                trim((epf.sls_grp)::text) = (' Carrefour 家樂福 '::character varying)::text
            ) THEN ' Carrefour (ASIA / Pacific) '::character varying
            WHEN (
                trim((epf.sls_grp)::text) = (' Cosmed 康是美 '::character varying)::text
            ) THEN ' Cosmed (TW) '::character varying
            WHEN (
                trim((epf.sls_grp)::text) = (' EC '::character varying)::text
            ) THEN ' Web Galaxy (TW - E) '::character varying
            WHEN (
                trim((epf.sls_grp)::text) = (' Poya 寶雅 '::character varying)::text
            ) THEN ' Pao Ya Ltd (TW) '::character varying
            WHEN (
                trim((epf.sls_grp)::text) = (' PX 全聯 '::character varying)::text
            ) THEN ' JIA Wang (TW - D) '::character varying
            WHEN (
                trim((epf.sls_grp)::text) = (' RT - Mart 大潤發 '::character varying)::text
            ) THEN ' Auchan (ASIA / Pacific) '::character varying
            WHEN (
                trim((epf.sls_grp)::text) = (' Watsons 屈臣氏 '::character varying)::text
            ) THEN ' Hutchison (Asia / Pacific) '::character varying
            ELSE epf.sls_grp
        END AS strategy_customer_hierachy_name,
        vice.to_crncy,
        vice.ex_rt,
        epf.ean_num,
        sum(epf.sls_amt) AS sls_amt,
        abs(sum(epf.sls_return_amt)) AS sls_return_amt,
        sum(epf.sls_qty) AS sls_qty,
        sum(epf.sls_amt_l3m) AS sls_amt_l3m,
        (abs(sum(epf.sls_return_amt_l3m)))::numeric(38, 5) AS sls_return_amt_13m,
        sum(epf.sls_qty_l3m) AS sls_qty_l3m,
        sum(epf.sls_amt_l6m) AS sls_amt_l6m,
        (abs(sum(epf.sls_return_amt_l6m)))::numeric(38, 5) AS sls_return_amt_l6m,
        sum(epf.sls_qty_l6m) AS sls_qty_l6m,
        0 AS rf_amount_si,
        0 AS rf_qty_si,
        0 AS rf_amount_so,
        0 AS rf_qty_so,
        0 AS rf_amount_si_l2m,
        0 AS rf_qty_si_l2m,
        0 AS rf_amount_so_l2m,
        0 AS rf_qty_so_l2m
    FROM epf
    LEFT JOIN
    (
        SELECT DISTINCT edw_product_attr_dim.ean,
            edw_product_attr_dim.cntry,
            edw_product_attr_dim.sap_matl_num,
            edw_product_attr_dim.prod_hier_l1,
            edw_product_attr_dim.prod_hier_l2,
            edw_product_attr_dim.prod_hier_l3,
            edw_product_attr_dim.prod_hier_l4,
            edw_product_attr_dim.prod_hier_l5,
            edw_product_attr_dim.prod_hier_l6,
            edw_product_attr_dim.prod_hier_l7,
            edw_product_attr_dim.prod_hier_l8,
            edw_product_attr_dim.prod_hier_l9,
            edw_product_attr_dim.lcl_prod_nm
        FROM edw_product_attr_dim
    ) epad ON
    (
        (
            ((epf.ean_num)::text = (epad.ean)::text)
            AND ((epad.cntry)::text = (epf.ctry_cd)::text)
        )
    )
    LEFT JOIN
    (
        SELECT edw_customer_attr_flat_dim.sold_to_party,
            edw_customer_attr_flat_dim.channel,
            edw_customer_attr_flat_dim.sls_grp,
            edw_customer_attr_flat_dim.cust_store_ref,
            edw_customer_attr_flat_dim.sls_ofc_desc,
            edw_customer_attr_flat_dim.store_typ
        FROM edw_customer_attr_flat_dim
    ) ecafd ON
    (
        (
            (
                (epf.sold_to_party)::text = (ecafd.sold_to_party)::text
            )
            AND (
                (epf.str_cd)::text = (ecafd.cust_store_ref)::text
            )
        )
    )
    LEFT JOIN (
        SELECT DISTINCT edw_customer_attr_hier_dim.sold_to_party,
            edw_customer_attr_hier_dim.strategy_customer_hierachy_name
        FROM edw_customer_attr_hier_dim
    ) ecahd ON
    (
        (
            ltrim(
                (epf.sold_to_party)::text,
                ((0)::character varying)::text
            ) = ltrim(
                (ecahd.sold_to_party)::text,
                ((0)::character varying)::text
            )
        )
    )
    LEFT JOIN v_intrm_crncy_exch vice ON (((epf.crncy_cd)::text = (vice.from_crncy)::text))
    WHERE
        (
            "substring"(((epf.fisc_date)::character varying)::text, 1, 4) > (
                (
                    date_part(year,convert_timezone('UTC', current_timestamp())) - 3
                )::character varying
            )::text
        )
        AND (
            epf.fisc_date IN (
                SELECT DISTINCT a.cal_day
                FROM edw_calendar_dim a
                JOIN edw_pos_fact b ON a.cal_day = b.pos_dt
                WHERE b.ctry_cd = 'TW'
            )
        )

    GROUP BY 1,
        (
            "substring"(((epf.fisc_date)::character varying)::text, 1, 4) || "substring"(((epf.fisc_date)::character varying)::text, 6, 2)
        ),
        "substring"(((epf.fisc_date)::character varying)::text, 1, 4),
        epf.ctry_cd,
        epad.prod_hier_l1,
        epad.prod_hier_l2,
        epad.prod_hier_l3,
        epad.prod_hier_l4,
        epad.prod_hier_l5,
        epad.prod_hier_l6,
        epad.prod_hier_l7,
        epad.prod_hier_l8,
        epad.prod_hier_l9,
        epad.sap_matl_num,
        ecafd.channel,
        epf.sls_grp,
        ecahd.strategy_customer_hierachy_name,
        vice.to_crncy,
        vice.ex_rt,
        epf.ean_num
    HAVING
        (
        sum(COALESCE(epf.sls_amt, 0)) <> 0
        OR sum(COALESCE(epf.sls_qty, 0)) <> 0
        OR sum(COALESCE(epf.sls_amt_l3m, 0)) <> 0
        OR sum(COALESCE(epf.sls_qty_l3m, 0)) <> 0
        OR sum(COALESCE(epf.sls_amt_l6m, 0)) <> 0
        )
    OR sum(COALESCE(epf.sls_qty_l6m, 0)) <> 0
),
etbfs as
(
    SELECT
        'TWD'::character varying AS crncy_cd,
        edw_tw_bu_forecast_sku.bu_version,
        to_date(
            (
                (edw_tw_bu_forecast_sku.forecast_for_year)::text || (edw_tw_bu_forecast_sku.forecast_for_mnth)::text
            ),
            ('YYYYMON'::character varying)::text
        ) AS forecast_date,
        edw_tw_bu_forecast_sku.sls_grp,
        edw_tw_bu_forecast_sku.channel,
        edw_tw_bu_forecast_sku.strategy_customer_hierachy_name,
        edw_tw_bu_forecast_sku.sap_code,
        dense_rank() OVER(
            PARTITION BY to_date(
                (
                    (edw_tw_bu_forecast_sku.forecast_on_year)::text || (edw_tw_bu_forecast_sku.forecast_on_month)::text
                ),
                ('YYYYMM'::character varying)::text
            )
            ORDER BY edw_tw_bu_forecast_sku.bu_version DESC
        ) AS version,
        sum(edw_tw_bu_forecast_sku.pre_sales_before_returns) AS rf_amount_si,
        sum(edw_tw_bu_forecast_sku.rf_sellin_qty) AS rf_qty_si,
        0 AS rf_amount_so,
        sum(edw_tw_bu_forecast_sku.rf_sellout_qty) AS rf_qty_so,
        0 AS rf_amount_si_l2m,
        0 AS rf_qty_si_l2m,
        0 AS rf_amount_so_l2m,
        0 AS rf_qty_so_l2m
    FROM edw_tw_bu_forecast_sku
    WHERE
        to_date
            (
                edw_tw_bu_forecast_sku.forecast_on_year || edw_tw_bu_forecast_sku.forecast_on_month,
                'YYYYMM'
            ) = (
                SELECT MAX(
                    to_date(
                        edw_tw_bu_forecast_sku.forecast_on_year || edw_tw_bu_forecast_sku.forecast_on_month,
                        'YYYYMM'
                    )
                ) AS max_date
                FROM edw_tw_bu_forecast_sku
            )
    GROUP BY 1,
        edw_tw_bu_forecast_sku.bu_version,
        to_date(
            (
                (edw_tw_bu_forecast_sku.forecast_for_year)::text || (edw_tw_bu_forecast_sku.forecast_for_mnth)::text
            ),
            ('YYYYMON'::character varying)::text
        ),
        edw_tw_bu_forecast_sku.sls_grp,
        edw_tw_bu_forecast_sku.channel,
        edw_tw_bu_forecast_sku.strategy_customer_hierachy_name,
        edw_tw_bu_forecast_sku.sap_code,
        edw_tw_bu_forecast_sku.forecast_on_year,
        edw_tw_bu_forecast_sku.forecast_on_month
    UNION ALL
    SELECT 'TWD'::character varying AS crncy_cd,
        edw_tw_bu_forecast_sku.bu_version,
        to_date(
            (
                (edw_tw_bu_forecast_sku.forecast_for_year)::text || (edw_tw_bu_forecast_sku.forecast_for_mnth)::text
            ),
            ('YYYYMON'::character varying)::text
        ) AS forecast_date,
        edw_tw_bu_forecast_sku.sls_grp,
        edw_tw_bu_forecast_sku.channel,
        edw_tw_bu_forecast_sku.strategy_customer_hierachy_name,
        edw_tw_bu_forecast_sku.sap_code,
        dense_rank() OVER(
            PARTITION BY to_date(
                (
                    (edw_tw_bu_forecast_sku.forecast_on_year)::text || (edw_tw_bu_forecast_sku.forecast_on_month)::text
                ),
                ('YYYYMM'::character varying)::text
            )
            ORDER BY edw_tw_bu_forecast_sku.bu_version DESC
        ) AS version,
        0 AS rf_amount_si,
        0 AS rf_qty_si,
        0 AS rf_amount_so,
        0 AS rf_qty_so,
        sum(edw_tw_bu_forecast_sku.pre_sales_before_returns) AS rf_amount_si_l2m,
        sum(edw_tw_bu_forecast_sku.rf_sellin_qty) AS rf_qty_si_l2m,
        0 AS rf_amount_so_l2m,
        sum(edw_tw_bu_forecast_sku.rf_sellout_qty) AS rf_qty_so_l2m
    FROM edw_tw_bu_forecast_sku
    WHERE
        dateadd(
            month,
            -2,
            to_date(
                edw_tw_bu_forecast_sku.forecast_for_year || edw_tw_bu_forecast_sku.forecast_for_mnth,
                'YYYYMON'
            )
        ) = to_date(
            edw_tw_bu_forecast_sku.forecast_on_year || edw_tw_bu_forecast_sku.forecast_on_month,
            'YYYYMM'
        )
    GROUP BY 1,
        edw_tw_bu_forecast_sku.bu_version,
        to_date(
            (
                (edw_tw_bu_forecast_sku.forecast_for_year)::text || (edw_tw_bu_forecast_sku.forecast_for_mnth)::text
            ),
            ('YYYYMON'::character varying)::text
        ),
        edw_tw_bu_forecast_sku.sls_grp,
        edw_tw_bu_forecast_sku.channel,
        edw_tw_bu_forecast_sku.strategy_customer_hierachy_name,
        edw_tw_bu_forecast_sku.sap_code,
        edw_tw_bu_forecast_sku.forecast_on_year,
        edw_tw_bu_forecast_sku.forecast_on_month
    UNION ALL
    SELECT
        'TWD'::character varying AS crncy_cd,
        edw_tw_bu_forecast_sku.bu_version,
        to_date(
            (
                (edw_tw_bu_forecast_sku.forecast_for_year)::text || (edw_tw_bu_forecast_sku.forecast_for_mnth)::text
            ),
            ('YYYYMON'::character varying)::text
        ) AS forecast_date,
        edw_tw_bu_forecast_sku.sls_grp,
        edw_tw_bu_forecast_sku.channel,
        edw_tw_bu_forecast_sku.strategy_customer_hierachy_name,
        edw_tw_bu_forecast_sku.sap_code,
        dense_rank() OVER(
            PARTITION BY to_date(
                (
                    (edw_tw_bu_forecast_sku.forecast_on_year)::text || (edw_tw_bu_forecast_sku.forecast_on_month)::text
                ),
                ('YYYYMM'::character varying)::text
            )
            ORDER BY edw_tw_bu_forecast_sku.bu_version DESC
        ) AS version,
        0 AS rf_amount_si,
        0 AS rf_qty_si,
        0 AS rf_amount_so,
        0 AS rf_qty_so,
        sum(edw_tw_bu_forecast_sku.pre_sales_before_returns) AS rf_amount_si_l2m,
        sum(edw_tw_bu_forecast_sku.rf_sellin_qty) AS rf_qty_si_l2m,
        0 AS rf_amount_so_l2m,
        sum(edw_tw_bu_forecast_sku.rf_sellout_qty) AS rf_qty_so_l2m
    FROM edw_tw_bu_forecast_sku
    WHERE
        (
            (
                dateadd(
                    month,
                    (- (2)::bigint),
                    (
                        to_date(
                            (
                                (edw_tw_bu_forecast_sku.forecast_for_year)::text || (edw_tw_bu_forecast_sku.forecast_for_mnth)::text
                            ),
                            ('YYYYMON'::character varying)::text
                        )
                    )::timestamp without time zone
                ) > (
                    SELECT "max"(
                            to_date(
                                (
                                    (edw_tw_bu_forecast_sku.forecast_on_year)::text || (edw_tw_bu_forecast_sku.forecast_on_month)::text
                                ),
                                ('YYYYMM'::character varying)::text
                            )
                        ) AS "max"
                    FROM edw_tw_bu_forecast_sku
                )
            )
            AND (
                to_date(
                    (
                        (edw_tw_bu_forecast_sku.forecast_on_year)::text || (edw_tw_bu_forecast_sku.forecast_on_month)::text
                    ),
                    ('YYYYMM'::character varying)::text
                ) = (
                    SELECT "max"(
                            to_date(
                                (
                                    (edw_tw_bu_forecast_sku.forecast_on_year)::text || (edw_tw_bu_forecast_sku.forecast_on_month)::text
                                ),
                                ('YYYYMM'::character varying)::text
                            )
                        ) AS "max"
                    FROM edw_tw_bu_forecast_sku
                )
            )
        )
    GROUP BY 1,
        edw_tw_bu_forecast_sku.bu_version,
        to_date(
            (
                (edw_tw_bu_forecast_sku.forecast_for_year)::text || (edw_tw_bu_forecast_sku.forecast_for_mnth)::text
            ),
            ('YYYYMON'::character varying)::text
        ),
        edw_tw_bu_forecast_sku.sls_grp,
        edw_tw_bu_forecast_sku.channel,
        edw_tw_bu_forecast_sku.strategy_customer_hierachy_name,
        edw_tw_bu_forecast_sku.sap_code,
        edw_tw_bu_forecast_sku.forecast_on_year,
        edw_tw_bu_forecast_sku.forecast_on_month
),
forecast as
(
    SELECT
        'FORECAST'::character varying AS data_set,
        (
            (
                "substring"(
                    ((etbfs.forecast_date)::character varying)::text,
                    1,
                    4
                ) || "substring"(
                    ((etbfs.forecast_date)::character varying)::text,
                    6,
                    2
                )
            )
        )::character varying AS fisc_per,
        (
            "substring"(
                ((etbfs.forecast_date)::character varying)::text,
                1,
                4
            )
        )::character varying AS fisc_yr,
        CASE
            WHEN (
                (prod_hier.prod_hier_l1 IS NULL)
                OR (
                    trim((prod_hier.prod_hier_l1)::text) = (''::character varying)::text
                )
            ) THEN 'Taiwan'::character varying
            ELSE COALESCE(prod_hier.prod_hier_l1, '#'::character varying)
        END AS prod_hier_l1,
        COALESCE(prod_hier.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
        COALESCE(prod_hier.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
        COALESCE(prod_hier.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
        COALESCE(prod_hier.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
        COALESCE(prod_hier.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
        COALESCE(prod_hier.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
        COALESCE(prod_hier.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
        COALESCE(prod_hier.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
        etbfs.sap_code AS sap_matl_num,
        CASE
            WHEN (
                (etbfs.channel IS NULL)
                OR (
                    trim((etbfs.channel)::text) = (''::character varying)::text
                )
            ) THEN 'NOT Available'::character varying
            ELSE etbfs.channel
        END AS channel,
        CASE
            WHEN (
                (etbfs.sls_grp IS NULL)
                OR (
                    trim((etbfs.sls_grp)::text) = (''::character varying)::text
                )
            ) THEN 'NOT Available'::character varying
            ELSE etbfs.sls_grp
        END AS sls_grp,
        CASE
            WHEN (
                (etbfs.strategy_customer_hierachy_name IS NULL)
                OR (
                    trim((etbfs.strategy_customer_hierachy_name)::text) = (''::character varying)::text
                )
            ) THEN 'NOT Available'::character varying
            ELSE etbfs.strategy_customer_hierachy_name
        END AS strategy_customer_hierachy_name,
        vice.to_crncy,
        vice.ex_rt,
        (prod_hier.ean_num)::character varying AS ean_num,
        0 AS sls_amt,
        0 AS sls_return_amt,
        0 AS sls_qty,
        0 AS sls_amt_l3m,
        0 AS sls_return_amt_13m,
        0 AS sls_qty_l3m,
        0 AS sls_amt_l6m,
        0 AS sls_return_amt_l6m,
        0 AS sls_qty_l6m,
        sum(etbfs.rf_amount_si) AS rf_amount_si,
        sum(etbfs.rf_qty_si) AS rf_qty_si,
        sum(etbfs.rf_amount_so) AS rf_amount_so,
        sum(etbfs.rf_qty_so) AS rf_qty_so,
        sum(etbfs.rf_amount_si_l2m) AS rf_amount_si_l2m,
        sum(etbfs.rf_qty_si_l2m) AS rf_qty_si_l2m,
        sum(etbfs.rf_amount_so_l2m) AS rf_amount_so_l2m,
        sum(etbfs.rf_qty_so_l2m) AS rf_qty_so_l2m
    FROM etbfs
    LEFT JOIN
    (
        SELECT matl.sap_code,
            matl.ean_num,
            prod.prod_hier_l1,
            prod.prod_hier_l2,
            prod.prod_hier_l3,
            prod.prod_hier_l4,
            prod.prod_hier_l5,
            prod.prod_hier_l6,
            prod.prod_hier_l7,
            prod.prod_hier_l8,
            prod.prod_hier_l9
        FROM (
                (
                    SELECT sap.sap_code,
                        "max"(ean.ean_num) AS ean_num
                    FROM (
                            (
                                SELECT DISTINCT ltrim(
                                        (edw_tw_bu_forecast_sku.sap_code)::text,
                                        (' 0 '::character varying)::text
                                    ) AS sap_code
                                FROM edw_tw_bu_forecast_sku
                            ) sap
                            LEFT JOIN (
                                SELECT DISTINCT ltrim(
                                        (edw_material_sales_dim.matl_num)::text,
                                        (' 0 '::character varying)::text
                                    ) AS matl_num,
                                    ltrim(
                                        (edw_material_sales_dim.ean_num)::text,
                                        (' 0 '::character varying)::text
                                    ) AS ean_num
                                FROM edw_material_sales_dim
                                WHERE (
                                        (
                                            (edw_material_sales_dim.sls_org)::text = ('1200'::character varying)::text
                                        )
                                        OR (
                                            (edw_material_sales_dim.sls_org)::text = ('120S'::character varying)::text
                                        )
                                    )
                            ) ean ON ((sap.sap_code = ean.matl_num))
                        )
                    GROUP BY sap.sap_code
                ) matl
                LEFT JOIN (
                    SELECT DISTINCT edw_product_attr_dim.ean,
                        edw_product_attr_dim.sap_matl_num,
                        edw_product_attr_dim.prod_hier_l1,
                        edw_product_attr_dim.prod_hier_l2,
                        edw_product_attr_dim.prod_hier_l3,
                        edw_product_attr_dim.prod_hier_l4,
                        edw_product_attr_dim.prod_hier_l5,
                        edw_product_attr_dim.prod_hier_l6,
                        edw_product_attr_dim.prod_hier_l7,
                        edw_product_attr_dim.prod_hier_l8,
                        edw_product_attr_dim.prod_hier_l9
                    FROM edw_product_attr_dim
                    WHERE (
                            (edw_product_attr_dim.cntry)::text = ('TW'::character varying)::text
                        )
                ) prod ON ((matl.ean_num = (prod.ean)::text))
            )
    ) prod_hier ON ((prod_hier.sap_code = (etbfs.sap_code)::text))
    LEFT JOIN v_intrm_crncy_exch vice ON (
        ((etbfs.crncy_cd)::text = (vice.from_crncy)::text)
    )
    WHERE (etbfs.version = 1)
    GROUP BY 1,
        (
            "substring"(
                ((etbfs.forecast_date)::character varying)::text,
                1,
                4
            ) || "substring"(
                ((etbfs.forecast_date)::character varying)::text,
                6,
                2
            )
        ),
        "substring"(
            ((etbfs.forecast_date)::character varying)::text,
            1,
            4
        ),
        prod_hier.prod_hier_l1,
        prod_hier.prod_hier_l2,
        prod_hier.prod_hier_l3,
        prod_hier.prod_hier_l4,
        prod_hier.prod_hier_l5,
        prod_hier.prod_hier_l6,
        prod_hier.prod_hier_l7,
        prod_hier.prod_hier_l8,
        prod_hier.prod_hier_l9,
        etbfs.sap_code,
        etbfs.channel,
        etbfs.sls_grp,
        etbfs.strategy_customer_hierachy_name,
        vice.to_crncy,
        vice.ex_rt,
        prod_hier.ean_num
    HAVING
    (
        sum(COALESCE(etbfs.rf_amount_si, 0.0)) <> 0.0
        OR sum(COALESCE(etbfs.rf_qty_si, 0.0)) <> 0.0
        OR sum(COALESCE(etbfs.rf_qty_so, 0.0)) <> 0.0
        OR sum(COALESCE(etbfs.rf_amount_si_l2m, 0.0)) <> 0.0
        OR sum(COALESCE(etbfs.rf_qty_si_l2m, 0.0)) <> 0.0
        OR sum(COALESCE(etbfs.rf_qty_so_l2m, 0.0)) <> 0.0
    )
),
final as
(
    select * from sellin
    UNION ALL
    select * from sellout
    UNION ALL
    select * from pos
    UNION ALL
    select * from forecast
)
select * from final