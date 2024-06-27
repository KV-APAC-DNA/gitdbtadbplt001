with vw_edw_coupang_premium_intrm as (
    select * from {{ ref('ntaedw_integration__vw_edw_coupang_premium_intrm') }} 
),
edw_intrm_calendar as 
(
    select * from {{ ref('ntaedw_integration__edw_intrm_calendar') }} 
),
edw_product_attr_dim as 
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
final as 
(   
    SELECT 
        premium.ctry_cd,
        premium.ctry_nm,
        premium.data_source,
        premium.reference_date,
        premium.data_granularity,
        premium.category_depth1,
        premium.category_depth2,
        premium.category_depth3,
        premium.ranking,
        premium.prev_day_ranking,
        premium.prev_week_ranking,
        premium.prev_mon_ranking,
        premium.sku_id,
        premium.sku_name,
        premium.jnj_brand,
        premium.rank_change,
        premium.vendoritemid,
        premium.ean,
        premium.all_brand,
        premium.jnj_product_flag,
        premium.coupang_product_name,
        premium.review_score_star,
        premium.review_contents,
        premium.coupang_id,
        premium.new_user_count,
        premium.curr_user_count,
        premium.tot_user_count,
        premium.new_user_sales_amt,
        premium.curr_user_sales_amt,
        premium.new_user_avg_product_sales_price,
        premium.curr_user_avg_product_sales_price,
        premium.tot_user_avg_product_sales_price,
        premium.search_keyword,
        premium.product_name,
        premium.by_search_keyword,
        premium.by_product_ranking,
        premium.product_ranking,
        premium.click_rate,
        premium.cart_transition_rate,
        premium.purchase_conversion_rate,
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
        prod.prod_hier_l1,
        prod.prod_hier_l2,
        prod.prod_hier_l3,
        prod.prod_hier_l4,
        prod.prod_hier_l5,
        prod.prod_hier_l6,
        prod.prod_hier_l7,
        prod.prod_hier_l8,
        prod.prod_hier_l9,
        prod.lcl_prod_nm
    FROM 
        (
            (
                vw_edw_coupang_premium_intrm premium
                LEFT JOIN (
                    SELECT ecd.fisc_yr AS "year",
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
                        cyrwkno.yr_wk_num AS wk,
                        ecd.fisc_wk_num AS mnth_wk_no,
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
                        ceil(day((ecd.cal_day ))/7) AS cal_mnth_wk_num,
                        date_part
                        (
                            week,
                            (ecd.cal_day)::timestamp without time zone
                        ) AS cal_yr_wk_num,
                        to_date((ecd.cal_day)::timestamp without time zone) AS cal_day,
                        "replace"(
                            ((ecd.cal_day)::character varying)::text,
                            ('-'::character varying)::text,
                            (''::character varying)::text
                        ) AS cal_date_id
                    FROM edw_intrm_calendar ecd,
                        (
                            SELECT row_number() OVER(
                                    PARTITION BY a.fisc_yr
                                    ORDER BY a.cal_wk
                                ) AS yr_wk_num,
                                row_number() OVER(
                                    PARTITION BY a.cal_yr
                                    ORDER BY a.cal_wk
                                ) AS cal_yr_wk_num,
                                to_date(
                                    dateadd(
                                        day,
                                        (- (6)::bigint),
                                        (a.cal_day)::timestamp without time zone
                                    )
                                ) AS cal_day_first,
                                a.cal_day AS cal_day_last
                            FROM edw_intrm_calendar a
                            WHERE (
                                    a.cal_day IN (
                                        SELECT edw_intrm_calendar.cal_day
                                        FROM edw_intrm_calendar
                                        WHERE (edw_intrm_calendar.wkday = 7)
                                    )
                                )
                            ORDER BY a.cal_wk
                        ) cyrwkno
                    WHERE (
                            (ecd.cal_day >= cyrwkno.cal_day_first)
                            AND (ecd.cal_day <= cyrwkno.cal_day_last)
                        )
                ) cal ON (
                    (
                        ((premium.reference_date)::character varying)::text = ((cal.cal_day)::character varying)::text
                    )
                )
            )
            LEFT JOIN (
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
                FROM edw_product_attr_dim edw_product_attr_dim
                WHERE (
                        (edw_product_attr_dim.cntry)::text = ('KR'::character varying)::text
                    )
            ) prod ON (((premium.ean)::text = (prod.ean_num)::text))
        )
)
select * from final