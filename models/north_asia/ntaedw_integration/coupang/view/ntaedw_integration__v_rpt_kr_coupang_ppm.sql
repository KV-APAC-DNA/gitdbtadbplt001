with itg_kr_coupang_productsalereport as (
    select * from {{ ref('ntaitg_integration__itg_kr_coupang_productsalereport') }} 
),
itg_query_parameters as (
    select * from {{ source('ntaitg_integration', 'itg_query_parameters') }}
),
edw_intrm_calendar as (
    select * from {{ ref('ntaedw_integration__edw_intrm_calendar') }} 
),
edw_product_attr_dim as (
    select * from {{ source('aspedw_integration', 'edw_product_attr_dim') }}
),
kr_coupang_ppm as 
(
    SELECT 
        'KR'::character varying AS ctry_cd,
        'SOUTH KOREA'::character varying AS ctry_nm,
        'KRW'::character varying AS crncy_cd,
        itg_kr_coupang_productsalereport.transaction_date,
        itg_kr_coupang_productsalereport.sku_id,
        itg_kr_coupang_productsalereport.barcode,
        itg_kr_coupang_productsalereport.brand,
        itg_kr_coupang_productsalereport.sku_people,
        itg_kr_coupang_productsalereport.product_id,
        itg_kr_coupang_productsalereport.sales_gmv,
        itg_kr_coupang_productsalereport.cost_of_purchase,
        itg_kr_coupang_productsalereport.units_sold,
        itg_kr_coupang_productsalereport.transaction_currency,
        CASE
            WHEN (
                (
                    itg_kr_coupang_productsalereport.sales_gmv / CASE
                        WHEN (
                            itg_kr_coupang_productsalereport.units_sold = ((0)::numeric)::numeric(18, 0)
                        ) THEN (NULL::numeric)::numeric(18, 0)
                        ELSE itg_kr_coupang_productsalereport.units_sold
                    END
                ) = ((0)::numeric)::numeric(18, 0)
            ) THEN (NULL::numeric)::numeric(18, 0)
            ELSE (
                itg_kr_coupang_productsalereport.sales_gmv / CASE
                    WHEN (
                        itg_kr_coupang_productsalereport.units_sold = ((0)::numeric)::numeric(18, 0)
                    ) THEN (NULL::numeric)::numeric(18, 0)
                    ELSE itg_kr_coupang_productsalereport.units_sold
                END
            )
        END AS price,
        CASE
            WHEN (
                (
                    itg_kr_coupang_productsalereport.sales_gmv / CASE
                        WHEN (
                            ((mv.parameter_value)::numeric(18, 0))::numeric(5, 1) = ((0)::numeric)::numeric(5, 1)
                        ) THEN (NULL::numeric)::numeric(18, 0)
                        ELSE ((mv.parameter_value)::numeric(18, 0))::numeric(5, 1)
                    END
                ) = ((0)::numeric)::numeric(18, 0)
            ) THEN (NULL::numeric)::numeric(18, 0)
            ELSE (
                itg_kr_coupang_productsalereport.sales_gmv / CASE
                    WHEN (
                        ((mv.parameter_value)::numeric(18, 0))::numeric(5, 1) = ((0)::numeric)::numeric(5, 1)
                    ) THEN (NULL::numeric)::numeric(18, 0)
                    ELSE ((mv.parameter_value)::numeric(18, 0))::numeric(5, 1)
                END
            )
        END AS revenue,
        (
            CASE
                WHEN (
                    (
                        itg_kr_coupang_productsalereport.sales_gmv / CASE
                            WHEN (
                                ((mv.parameter_value)::numeric(18, 0))::numeric(5, 1) = ((0)::numeric)::numeric(5, 1)
                            ) THEN (NULL::numeric)::numeric(18, 0)
                            ELSE ((mv.parameter_value)::numeric(18, 0))::numeric(5, 1)
                        END
                    ) = ((0)::numeric)::numeric(18, 0)
                ) THEN (NULL::numeric)::numeric(18, 0)
                ELSE (
                    itg_kr_coupang_productsalereport.sales_gmv / CASE
                        WHEN (
                            ((mv.parameter_value)::numeric(18, 0))::numeric(5, 1) = ((0)::numeric)::numeric(5, 1)
                        ) THEN (NULL::numeric)::numeric(18, 0)
                        ELSE ((mv.parameter_value)::numeric(18, 0))::numeric(5, 1)
                    END
                )
            END - CASE
                WHEN (
                    itg_kr_coupang_productsalereport.cost_of_purchase = ((0)::numeric)::numeric(18, 6)
                ) THEN (NULL::numeric)::numeric(18, 0)
                ELSE itg_kr_coupang_productsalereport.cost_of_purchase
            END
        ) AS ppm
    FROM 
        (
            itg_kr_coupang_productsalereport
            LEFT JOIN 
            (
                SELECT DISTINCT itg_query_parameters.country_code,
                    itg_query_parameters.parameter_name,
                    itg_query_parameters.parameter_value,
                    itg_query_parameters.parameter_type
                FROM itg_query_parameters
                WHERE 
                    (
                        (
                            (itg_query_parameters.country_code)::text = ('KR'::character varying)::text
                        )
                        AND (
                            (itg_query_parameters.parameter_name)::text = ('coupang_ppm_margin_value'::character varying)::text
                        )
                    )
            ) mv ON 
            (
                (
                    upper(
                        (itg_kr_coupang_productsalereport.country_cd)::text
                    ) = upper((mv.country_code)::text)
                )
            )
        )
),
final as
(
    SELECT 
        kr_coupang_ppm.ctry_cd,
        kr_coupang_ppm.ctry_nm,
        kr_coupang_ppm.crncy_cd,
        kr_coupang_ppm.transaction_date,
        kr_coupang_ppm.sku_id,
        kr_coupang_ppm.barcode,
        kr_coupang_ppm.brand,
        kr_coupang_ppm.sku_people,
        kr_coupang_ppm.product_id,
        sum(kr_coupang_ppm.ppm) AS ppp,
        sum(kr_coupang_ppm.sales_gmv) AS sales_gmv,
        sum(kr_coupang_ppm.revenue) AS revenue,
        (
            (
                kr_coupang_ppm.ppm / CASE
                    WHEN (
                        kr_coupang_ppm.revenue = ((0)::numeric)::numeric(18, 0)
                    ) THEN (NULL::numeric)::numeric(18, 0)
                    ELSE kr_coupang_ppm.revenue
                END
            ) * ((100)::numeric)::numeric(18, 0)
        ) AS ppm_percent,
        sum(kr_coupang_ppm.cost_of_purchase) AS cost_of_purchase,
        sum(kr_coupang_ppm.units_sold) AS units_sold,
        sum(kr_coupang_ppm.price) AS price,
        cal."year" AS fisc_year,
        cal.qrtr_no AS fisc_qrtr,
        cal.mnth_id AS fisc_month,
        cal.mnth_no AS fisc_month_num,
        cal.mnth_long AS fisc_month_name,
        cal.wk AS fisc_wk_num,
        cal.mnth_wk_no AS fisc_month_wk_num,
        cal.mnth_day AS fisc_month_day,
        cal.cal_year,
        cal.cal_qrtr_no AS cal_qrtr,
        cal.cal_mnth_id AS cal_month,
        cal.cal_mnth_no AS cal_month_num,
        cal.cal_mnth_nm AS cal_month_name,
        cal.cal_yr_wk_num AS cal_wk_num,
        cal.cal_mnth_wk_num,
        cal.cal_mnth_day,
        cal.cal_day,
        null AS ex_rt_typ,
        null AS to_crncy,
        null AS ex_rt,
        prod.sap_matl_num,
        prod.prod_hier_l1,
        prod.prod_hier_l2,
        prod.prod_hier_l3,
        COALESCE(prod.prod_hier_l4, 'N/A'::character varying) AS prod_hier_l4,
        prod.prod_hier_l5,
        prod.prod_hier_l6,
        prod.prod_hier_l7,
        prod.prod_hier_l8,
        prod.prod_hier_l9,
        prod.lcl_prod_nm
    FROM kr_coupang_ppm
        LEFT JOIN 
        (
            SELECT 
                ecd.fisc_yr AS "year",
                ecd.fisc_per,
                CASE
                    WHEN (ecd.pstng_per = 1) THEN 1
                    WHEN (ecd.pstng_per = 2) THEN 1
                    WHEN (ecd.pstng_per = 3) THEN 1
                    WHEN (ecd.pstng_per = 4) THEN 2
                    WHEN (ecd.pstng_per = 5) THEN 2
                    WHEN (ecd.pstng_per = 6) THEN 2
                    WHEN (ecd.pstng_per = 7) THEN 3
                    WHEN (ecd.pstng_per = 8) THEN 3
                    WHEN (ecd.pstng_per = 9) THEN 3
                    WHEN (ecd.pstng_per = 10) THEN 4
                    WHEN (ecd.pstng_per = 11) THEN 4
                    WHEN (ecd.pstng_per = 12) THEN 4
                    ELSE NULL::integer
                END AS qrtr_no,
                (
                    (
                        ((ecd.fisc_yr)::character varying)::text || ('/'::character varying)::text
                    ) || (
                        CASE
                            WHEN (ecd.pstng_per = 1) THEN 'Q1'::character varying
                            WHEN (ecd.pstng_per = 2) THEN 'Q1'::character varying
                            WHEN (ecd.pstng_per = 3) THEN 'Q1'::character varying
                            WHEN (ecd.pstng_per = 4) THEN 'Q2'::character varying
                            WHEN (ecd.pstng_per = 5) THEN 'Q2'::character varying
                            WHEN (ecd.pstng_per = 6) THEN 'Q2'::character varying
                            WHEN (ecd.pstng_per = 7) THEN 'Q3'::character varying
                            WHEN (ecd.pstng_per = 8) THEN 'Q3'::character varying
                            WHEN (ecd.pstng_per = 9) THEN 'Q3'::character varying
                            WHEN (ecd.pstng_per = 10) THEN 'Q4'::character varying
                            WHEN (ecd.pstng_per = 11) THEN 'Q4'::character varying
                            WHEN (ecd.pstng_per = 12) THEN 'Q4'::character varying
                            ELSE NULL::character varying
                        END
                    )::text
                ) AS qrtr,
                (
                    ((ecd.fisc_yr)::character varying)::text || trim(
                        to_char(ecd.pstng_per, ('00'::character varying)::text)
                    )
                ) AS mnth_id,
                (
                    (
                        ((ecd.fisc_yr)::character varying)::text || ('/'::character varying)::text
                    ) || (
                        CASE
                            WHEN (ecd.pstng_per = 1) THEN 'JAN'::character varying
                            WHEN (ecd.pstng_per = 2) THEN 'FEB'::character varying
                            WHEN (ecd.pstng_per = 3) THEN 'MAR'::character varying
                            WHEN (ecd.pstng_per = 4) THEN 'APR'::character varying
                            WHEN (ecd.pstng_per = 5) THEN 'MAY'::character varying
                            WHEN (ecd.pstng_per = 6) THEN 'JUN'::character varying
                            WHEN (ecd.pstng_per = 7) THEN 'JUL'::character varying
                            WHEN (ecd.pstng_per = 8) THEN 'AUG'::character varying
                            WHEN (ecd.pstng_per = 9) THEN 'SEP'::character varying
                            WHEN (ecd.pstng_per = 10) THEN 'OCT'::character varying
                            WHEN (ecd.pstng_per = 11) THEN 'NOV'::character varying
                            WHEN (ecd.pstng_per = 12) THEN 'DEC'::character varying
                            ELSE NULL::character varying
                        END
                    )::text
                ) AS mnth_desc,
                ecd.pstng_per AS mnth_no,
                CASE
                    WHEN (ecd.pstng_per = 1) THEN 'JAN'::character varying
                    WHEN (ecd.pstng_per = 2) THEN 'FEB'::character varying
                    WHEN (ecd.pstng_per = 3) THEN 'MAR'::character varying
                    WHEN (ecd.pstng_per = 4) THEN 'APR'::character varying
                    WHEN (ecd.pstng_per = 5) THEN 'MAY'::character varying
                    WHEN (ecd.pstng_per = 6) THEN 'JUN'::character varying
                    WHEN (ecd.pstng_per = 7) THEN 'JUL'::character varying
                    WHEN (ecd.pstng_per = 8) THEN 'AUG'::character varying
                    WHEN (ecd.pstng_per = 9) THEN 'SEP'::character varying
                    WHEN (ecd.pstng_per = 10) THEN 'OCT'::character varying
                    WHEN (ecd.pstng_per = 11) THEN 'NOV'::character varying
                    WHEN (ecd.pstng_per = 12) THEN 'DEC'::character varying
                    ELSE NULL::character varying
                END AS mnth_shrt,
                CASE
                    WHEN (ecd.pstng_per = 1) THEN 'JANUARY'::character varying
                    WHEN (ecd.pstng_per = 2) THEN 'FEBRUARY'::character varying
                    WHEN (ecd.pstng_per = 3) THEN 'MARCH'::character varying
                    WHEN (ecd.pstng_per = 4) THEN 'APRIL'::character varying
                    WHEN (ecd.pstng_per = 5) THEN 'MAY'::character varying
                    WHEN (ecd.pstng_per = 6) THEN 'JUNE'::character varying
                    WHEN (ecd.pstng_per = 7) THEN 'JULY'::character varying
                    WHEN (ecd.pstng_per = 8) THEN 'AUGUST'::character varying
                    WHEN (ecd.pstng_per = 9) THEN 'SEPTEMBER'::character varying
                    WHEN (ecd.pstng_per = 10) THEN 'OCTOBER'::character varying
                    WHEN (ecd.pstng_per = 11) THEN 'NOVEMBER'::character varying
                    WHEN (ecd.pstng_per = 12) THEN 'DECEMBER'::character varying
                    ELSE NULL::character varying
                END AS mnth_long,
                cmfmwk.yr_wk_num AS wk,
                cmfmwk.fisc_mnth_wk AS mnth_wk_no,
                row_number() OVER(
                    PARTITION BY ecd.fisc_per
                    ORDER BY ecd.cal_day
                ) AS mnth_day,
                ecd.cal_yr AS cal_year,
                ecd.cal_qtr_1 AS cal_qrtr_no,
                ecd.cal_mo_1 AS cal_mnth_id,
                ecd.cal_mo_2 AS cal_mnth_no,
                CASE
                    WHEN (ecd.cal_mo_2 = 1) THEN 'JANUARY'::character varying
                    WHEN (ecd.cal_mo_2 = 2) THEN 'FEBRUARY'::character varying
                    WHEN (ecd.cal_mo_2 = 3) THEN 'MARCH'::character varying
                    WHEN (ecd.cal_mo_2 = 4) THEN 'APRIL'::character varying
                    WHEN (ecd.cal_mo_2 = 5) THEN 'MAY'::character varying
                    WHEN (ecd.cal_mo_2 = 6) THEN 'JUNE'::character varying
                    WHEN (ecd.cal_mo_2 = 7) THEN 'JULY'::character varying
                    WHEN (ecd.cal_mo_2 = 8) THEN 'AUGUST'::character varying
                    WHEN (ecd.cal_mo_2 = 9) THEN 'SEPTEMBER'::character varying
                    WHEN (ecd.cal_mo_2 = 10) THEN 'OCTOBER'::character varying
                    WHEN (ecd.cal_mo_2 = 11) THEN 'NOVEMBER'::character varying
                    WHEN (ecd.cal_mo_2 = 12) THEN 'DECEMBER'::character varying
                    ELSE NULL::character varying
                END AS cal_mnth_nm,
                row_number() OVER(
                    PARTITION BY ecd.cal_mo_1
                    ORDER BY ecd.cal_day
                ) AS cal_mnth_day,
                cmfmwk.cal_mnth_wk_num,
                cmfmwk.cal_yr_wk_num,
                to_date((ecd.cal_day)::timestamp without time zone) AS cal_day,
                replace(
                    ((ecd.cal_day)::character varying)::text,
                    ('-'::character varying)::text,
                    (''::character varying)::text
                ) AS cal_date_id
            FROM edw_intrm_calendar ecd,
            (
                SELECT derived_table2.cal_day,
                    derived_table2.cal_mo_1,
                    derived_table2.cal_yr,
                    derived_table2.week_start,
                    derived_table2.week_end,
                    derived_table2.fisc_mnth_wk,
                    derived_table2.yr_wk_num,
                    dense_rank() OVER(
                        PARTITION BY derived_table2.cal_mo_1
                        ORDER BY derived_table2.week_start
                    ) AS cal_mnth_wk_num,
                    derived_table2.cal_yr_wk_num
                FROM (
                        SELECT derived_table1.cal_day,
                            derived_table1.cal_mo_1,
                            derived_table1.cal_wk,
                            derived_table1.cal_yr,
                            derived_table1.min_day,
                            derived_table1.max_day,
                            derived_table1.fisc_mnth_wk,
                            derived_table1.yr_wk_num,
                            derived_table1.cal_yr_wk_num,
                            CASE
                                WHEN (derived_table1.cal_day = derived_table1.min_day) THEN (derived_table1.cal_day)::timestamp without time zone
                                WHEN (derived_table1.wkday = 1) THEN (derived_table1.cal_day)::timestamp without time zone
                                ELSE CASE
                                    WHEN (
                                        date_trunc(
                                            week,
                                            (derived_table1.cal_day)::timestamp without time zone
                                        ) <= derived_table1.min_day
                                    ) THEN (derived_table1.min_day)::timestamp without time zone
                                    ELSE date_trunc(
                                        week,
                                        (derived_table1.cal_day)::timestamp without time zone
                                    )
                                END
                            END AS week_start,
                            CASE
                                WHEN (derived_table1.cal_day = derived_table1.max_day) THEN (derived_table1.cal_day)::timestamp without time zone
                                WHEN (derived_table1.wkday = 7) THEN (derived_table1.cal_day)::timestamp without time zone
                                ELSE CASE
                                    WHEN (
                                        date_trunc(
                                            week,
                                            ((derived_table1.cal_day + 7))::timestamp without time zone
                                        ) >= derived_table1.max_day
                                    ) THEN (derived_table1.max_day)::timestamp without time zone
                                    ELSE (
                                        dateadd(day,-1,
                                        date_trunc(
                                            week,((derived_table1.cal_day + 7))::timestamp without time zone
                                        )
                                        )
                                    )
                                END
                            END AS week_end
                        FROM (
                                SELECT edw_intrm_calendar.cal_day,
                                    edw_intrm_calendar.cal_mo_1,
                                    edw_intrm_calendar.cal_wk,
                                    edw_intrm_calendar.wkday,
                                    edw_intrm_calendar.cal_yr,
                                    dense_rank() OVER(
                                        PARTITION BY edw_intrm_calendar.fisc_per
                                        ORDER BY edw_intrm_calendar.cal_wk
                                    ) AS fisc_mnth_wk,
                                    dense_rank() OVER(
                                        PARTITION BY edw_intrm_calendar.fisc_yr
                                        ORDER BY edw_intrm_calendar.cal_wk
                                    ) AS yr_wk_num,
                                    dense_rank() OVER(
                                        PARTITION BY edw_intrm_calendar.cal_yr
                                        ORDER BY edw_intrm_calendar.cal_wk
                                    ) AS cal_yr_wk_num,
                                    min(edw_intrm_calendar.cal_day) OVER(
                                        PARTITION BY edw_intrm_calendar.cal_mo_1 order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
                                        FOLLOWING 
                                    ) AS min_day,
                                    "max"(edw_intrm_calendar.cal_day) OVER(
                                        PARTITION BY edw_intrm_calendar.cal_mo_1 order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                    ) AS max_day
                                FROM ntaedw_integration.edw_intrm_calendar
                            ) derived_table1
                    ) derived_table2
            ) cmfmwk
            WHERE (ecd.cal_day = cmfmwk.cal_day)
        ) cal ON ((kr_coupang_ppm.transaction_date = cal.cal_day)) 
        LEFT JOIN 
        (
            SELECT DISTINCT edw_product_attr_dim.ean AS ean_num,
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
        ) prod ON 
        (
            (
                (
                    (kr_coupang_ppm.ctry_cd)::text = (prod.cntry)::text
                )
                AND (
                    rtrim(kr_coupang_ppm.barcode)::text = rtrim(prod.ean_num)::text
                )
            )
        )  
    GROUP BY kr_coupang_ppm.ctry_cd,
    kr_coupang_ppm.ctry_nm,
    kr_coupang_ppm.crncy_cd,
    kr_coupang_ppm.transaction_date,
    kr_coupang_ppm.sku_id,
    kr_coupang_ppm.sku_people,
    kr_coupang_ppm.product_id,
    kr_coupang_ppm.barcode,
    kr_coupang_ppm.brand,
    kr_coupang_ppm.ppm,
    kr_coupang_ppm.revenue,
    prod.sap_matl_num,
    prod.prod_hier_l1,
    prod.prod_hier_l2,
    prod.prod_hier_l3,
    prod.prod_hier_l4,
    prod.prod_hier_l5,
    prod.prod_hier_l6,
    prod.prod_hier_l7,
    prod.prod_hier_l8,
    prod.prod_hier_l9,
    prod.lcl_prod_nm,
    cal.cal_day,
    cal."year",
    cal.qrtr_no,
    cal.mnth_id,
    cal.mnth_no,
    cal.mnth_long,
    cal.wk,
    cal.mnth_wk_no,
    cal.mnth_day,
    cal.cal_year,
    cal.cal_qrtr_no,
    cal.cal_mnth_id,
    cal.cal_mnth_no,
    cal.cal_mnth_nm,
    cal.cal_yr_wk_num,
    cal.cal_mnth_wk_num,
    cal.cal_mnth_day
)
select * from final