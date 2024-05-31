with itg_kr_coupang_pa_report as (
    select * from ntaitg_integration.itg_kr_coupang_pa_report
),
itg_mds_kr_brand_campaign_promotion as (
    select * from ntaitg_integration.itg_mds_kr_brand_campaign_promotion
),
itg_kr_coupang_productsalereport as (
    select * from ntaitg_integration.itg_kr_coupang_productsalereport
),
itg_mds_kr_keyword_classifications as (
    select * from ntaitg_integration.itg_mds_kr_keyword_classifications
),
itg_kr_coupang_bpa_report as (
    select * from ntaitg_integration.itg_kr_coupang_bpa_report
),
itg_kr_dads_coupang_price as (
    select * from ntaitg_integration.itg_kr_dads_coupang_price
),
itg_kr_dads_linkprice as (
    select * from ntaitg_integration.itg_kr_dads_linkprice
),
itg_mds_kr_naver_product_master as (
    select * from ntaitg_integration.itg_mds_kr_naver_product_master
),
itg_kr_dads_naver_gmv as (
    select * from ntaitg_integration.itg_kr_dads_naver_gmv
),
edw_intrm_calendar as (
    select * from ntaedw_integration.edw_intrm_calendar
),
itg_kr_dads_coupang_search_keyword as (
    select * from ntaitg_integration.itg_kr_dads_coupang_search_keyword
),
itg_kr_dads_naver_keyword_search_volume as (
    select * from ntaitg_integration.itg_kr_dads_naver_keyword_search_volume
),
itg_kr_dads_naver_search_channel as (
    select * from ntaitg_integration.itg_kr_dads_naver_search_channel
),
itg_query_parameters as (
    select * from ntaitg_integration.itg_query_parameters
),
final as (
SELECT final.brand,
    final.keyword,
    final.search_area,
    final.ad_types,
    final.retailer,
    final.sub_retailer,
    final.product_name,
    final.barcode,
    final.keyword_group,
    final.fisc_wk_num,
    final.fisc_mnth,
    final.fisc_per,
    final.fisc_day,
    final.category_1,
    final.category_2,
    final.category_3,
    final.ranking,
    final.click,
    final.impression,
    final.ad_cost,
    final.total_order,
    final.total_order_sku,
    final.total_conversion_sales,
    final.total_conversion_sales_sku,
    final.sales_gmv,
    final.sales_gmv_sku,
    final.pa_cost,
    final.bpa_cost,
    final.sa_cost,
    final.observed_price,
    final.rocket_wow_price,
    final.total_monthly_search_volume,
    final.payment_amount
FROM (
        (
            SELECT derived_table8.brand,
                derived_table8.keyword,
                derived_table8.search_area,
                derived_table8.ad_types,
                derived_table8.retailer,
                derived_table8.sub_retailer,
                derived_table8.product_name,
                derived_table8.barcode,
                derived_table8.keyword_group,
                derived_table8.fisc_wk_num,
                derived_table8.fisc_mnth,
                derived_table8.fisc_per,
                derived_table8.fisc_day,
                derived_table8.category_1,
                derived_table8.category_2,
                derived_table8.category_3,
                derived_table8.ranking,
                sum((derived_table8.click)::numeric) AS click,
                sum((derived_table8.impression)::numeric) AS impression,
                sum((derived_table8.ad_cost)::numeric) AS ad_cost,
                sum((derived_table8.total_order)::numeric) AS total_order,
                sum((derived_table8.total_order_sku)::numeric) AS total_order_sku,
                sum((derived_table8.total_conversion_sales)::numeric) AS total_conversion_sales,
                sum(
                    (derived_table8.total_conversion_sales_sku)::numeric
                ) AS total_conversion_sales_sku,
                sum(derived_table8.sales_gmv) AS sales_gmv,
                sum(derived_table8.sales_gmv_sku) AS sales_gmv_sku,
                sum((derived_table8.pa_cost)::numeric) AS pa_cost,
                sum((derived_table8.bpa_cost)::numeric) AS bpa_cost,
                sum((derived_table8.sa_cost)::numeric) AS sa_cost,
                avg((derived_table8.observed_price)::numeric) AS observed_price,
                avg(derived_table8.rocket_wow_price) AS rocket_wow_price,
                sum(derived_table8.total_monthly_search_volume) AS total_monthly_search_volume,
                sum(derived_table8.payment_amount) AS payment_amount
            FROM (
                    SELECT abc.date,
                        abc.campaign_name,
                        abc.campaign_id,
                        CASE
                            WHEN (
                                (abc.brand IS NULL)
                                OR ((abc.brand)::text = ''::text)
                            ) THEN NULL::character varying
                            ELSE abc.brand
                        END AS brand,
                        abc.keyword,
                        abc.search_area,
                        abc.ad_types,
                        abc.click,
                        abc.impression,
                        abc.ad_cost,
                        abc.total_order,
                        abc.total_order_sku,
                        abc.total_conversion_sales,
                        abc.total_conversion_sales_sku,
                        abc.file_name,
                        abc.retailer,
                        abc.sub_retailer,
                        abc.sales_gmv,
                        abc.sales_gmv_sku,
                        abc.product_name,
                        abc.barcode,
                        abc.sku_id,
                        abc.pa_cost,
                        abc.bpa_cost,
                        abc.sa_cost,
                        abc.data_refresh_time,
                        abc.keyword_group,
                        abc.observed_price,
                        (
                            CASE
                                WHEN (
                                    (abc.rocket_wow_price IS NULL)
                                    OR ((abc.rocket_wow_price)::text = ''::text)
                                ) THEN abc.observed_price
                                ELSE abc.rocket_wow_price
                            END
                        )::numeric(18, 0) AS rocket_wow_price,
                        CASE
                            WHEN ((tim.fisc_wk_num)::text = (6)::text) THEN 'W6'::character varying
                            ELSE tim.fisc_wk_num
                        END AS fisc_wk_num,
                        tim.pstng_per AS fisc_mnth,
                        tim.cal_day,
                        tim.fisc_per,
                        to_date(
                            (
                                ((tim.fisc_yr)::text || '-'::text) || (tim.pstng_per)::text
                            ),
                            'YYYY-MM'::text
                        ) AS fisc_day,
                        abc.category_1,
                        abc.category_2,
                        abc.category_3,
                        abc.ranking,
                        (abc.total_monthly_search_volume)::numeric(20, 4) AS total_monthly_search_volume,
                        (abc.payment_amount)::numeric(20, 4) AS payment_amount
                    FROM (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            SELECT a.date,
                                                                a.campaign_name,
                                                                a.campaign_id,
                                                                (trim((b.brand_code)::text))::character varying AS brand,
                                                                (
                                                                    "replace"(
                                                                        lower(
                                                                            (
                                                                                COALESCE(a.keyword, '[비검색광고영역]'::character varying)
                                                                            )::text
                                                                        ),
                                                                        ' '::text,
                                                                        ''::text
                                                                    )
                                                                )::character varying AS keyword,
                                                                a.ad_impression_area AS search_area,
                                                                a.ad_types,
                                                                a.click_count AS click,
                                                                a.impression_count AS impression,
                                                                a.ad_cost,
                                                                a.total_orders_1d AS total_order,
                                                                a.direct_orders_1d AS total_order_sku,
                                                                a.total_conversion_sales_1d AS total_conversion_sales,
                                                                a.direct_conversion_sales_1d AS total_conversion_sales_sku,
                                                                a.file_name,
                                                                'COUPANG'::character varying AS retailer,
                                                                'COUPANG-PA'::character varying AS sub_retailer,
                                                                (a.total_conversion_sales_1d)::numeric(20, 4) AS sales_gmv,
                                                                (a.direct_conversion_sales_1d)::numeric(20, 4) AS sales_gmv_sku,
                                                                (trim((d.sku_people)::text))::character varying AS product_name,
                                                                (trim((d.barcode)::text))::character varying AS barcode,
                                                                a.ad_execution_option_id AS sku_id,
                                                                a.ad_cost AS pa_cost,
                                                                NULL::character varying AS bpa_cost,
                                                                NULL::character varying AS sa_cost,
                                                                a.crtd_dttm AS data_refresh_time,
                                                                COALESCE(
                                                                    e.keyword_group,
                                                                    'other keyword'::character varying
                                                                ) AS keyword_group,
                                                                NULL::character varying AS observed_price,
                                                                NULL::character varying AS rocket_wow_price,
                                                                d.product_category_high AS category_1,
                                                                d.product_category_mid AS category_2,
                                                                d.product_category_low AS category_3,
                                                                NULL::character varying AS ranking,
                                                                NULL::character varying AS total_monthly_search_volume,
                                                                NULL::character varying AS payment_amount
                                                            FROM (
                                                                    (
                                                                        (
                                                                            itg_kr_coupang_pa_report a
                                                                            LEFT JOIN (
                                                                                SELECT DISTINCT itg_mds_kr_brand_campaign_promotion.code,
                                                                                    itg_mds_kr_brand_campaign_promotion.brand_code
                                                                                FROM itg_mds_kr_brand_campaign_promotion
                                                                            ) b ON (
                                                                                (
                                                                                    ((trim((b.code)::text))::character varying)::text = (
                                                                                        (trim((a.campaign_name)::text))::character varying
                                                                                    )::text
                                                                                )
                                                                            )
                                                                        )
                                                                        LEFT JOIN (
                                                                            SELECT e.transaction_date,
                                                                                e.vendor_item_id,
                                                                                g.sku_people,
                                                                                e.barcode,
                                                                                e.product_category_high,
                                                                                e.product_category_mid,
                                                                                e.product_category_low
                                                                            FROM (
                                                                                    (
                                                                                        SELECT derived_table1.transaction_date,
                                                                                            derived_table1.vendor_item_id,
                                                                                            derived_table1.sku_people,
                                                                                            derived_table1.barcode,
                                                                                            derived_table1.product_category_high,
                                                                                            derived_table1.product_category_mid,
                                                                                            derived_table1.product_category_low,
                                                                                            derived_table1.rn
                                                                                        FROM (
                                                                                                SELECT DISTINCT itg_kr_coupang_productsalereport.transaction_date,
                                                                                                    itg_kr_coupang_productsalereport.vendor_item_id,
                                                                                                    itg_kr_coupang_productsalereport.sku_people,
                                                                                                    itg_kr_coupang_productsalereport.barcode,
                                                                                                    itg_kr_coupang_productsalereport.product_category_high,
                                                                                                    itg_kr_coupang_productsalereport.product_category_mid,
                                                                                                    itg_kr_coupang_productsalereport.product_category_low,
                                                                                                    row_number() OVER(
                                                                                                        PARTITION BY itg_kr_coupang_productsalereport.transaction_date,
                                                                                                        itg_kr_coupang_productsalereport.vendor_item_id
                                                                                                        ORDER BY length(
                                                                                                                (
                                                                                                                    COALESCE(
                                                                                                                        itg_kr_coupang_productsalereport.product_category_high,
                                                                                                                        ''::character varying
                                                                                                                    )
                                                                                                                )::text
                                                                                                            ) DESC
                                                                                                    ) AS rn
                                                                                                FROM itg_kr_coupang_productsalereport
                                                                                            ) derived_table1
                                                                                        WHERE (derived_table1.rn = 1)
                                                                                    ) e
                                                                                    LEFT JOIN (
                                                                                        SELECT f.sku_people,
                                                                                            f.barcode,
                                                                                            f.rno
                                                                                        FROM (
                                                                                                SELECT DISTINCT itg_kr_coupang_productsalereport.sku_people,
                                                                                                    itg_kr_coupang_productsalereport.barcode,
                                                                                                    row_number() OVER(
                                                                                                        PARTITION BY itg_kr_coupang_productsalereport.barcode
                                                                                                        ORDER BY itg_kr_coupang_productsalereport.transaction_date DESC
                                                                                                    ) AS rno
                                                                                                FROM itg_kr_coupang_productsalereport
                                                                                            ) f
                                                                                        WHERE (f.rno = 1)
                                                                                    ) g ON (((e.barcode)::text = (g.barcode)::text))
                                                                                )
                                                                        ) d ON (
                                                                            (
                                                                                (
                                                                                    (a.date)::text = "replace"((d.transaction_date)::text, '-'::text, ''::text)
                                                                                )
                                                                                AND (
                                                                                    trim((a.ad_execution_option_id)::text) = trim((d.vendor_item_id)::text)
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                    LEFT JOIN (
                                                                        SELECT DISTINCT "replace"(
                                                                                lower(
                                                                                    (
                                                                                        COALESCE(
                                                                                            itg_mds_kr_keyword_classifications.keyword,
                                                                                            '[비검색광고영역]'::character varying
                                                                                        )
                                                                                    )::text
                                                                                ),
                                                                                ' '::text,
                                                                                ''::text
                                                                            ) AS keyword2,
                                                                            itg_mds_kr_keyword_classifications.keyword_group
                                                                        FROM itg_mds_kr_keyword_classifications
                                                                    ) e ON (
                                                                        (
                                                                            "replace"(
                                                                                lower(
                                                                                    (
                                                                                        COALESCE(a.keyword, '[비검색광고영역]'::character varying)
                                                                                    )::text
                                                                                ),
                                                                                ' '::text,
                                                                                ''::text
                                                                            ) = e.keyword2
                                                                        )
                                                                    )
                                                                )
                                                            UNION ALL
                                                            SELECT a.date,
                                                                a.campaign_name,
                                                                a.campaign_id,
                                                                (trim((b.brand_code)::text))::character varying AS brand,
                                                                (
                                                                    "replace"(
                                                                        lower(
                                                                            (
                                                                                COALESCE(
                                                                                    a.impressed_keywords,
                                                                                    '[비검색광고영역]'::character varying
                                                                                )
                                                                            )::text
                                                                        ),
                                                                        ' '::text,
                                                                        ''::text
                                                                    )
                                                                )::character varying AS keyword,
                                                                NULL::character varying AS search_area,
                                                                'BPA'::character varying AS ad_types,
                                                                a.click_count AS click,
                                                                a.impression_count AS impression,
                                                                a.ad_cost,
                                                                a.total_orders_1d AS total_order,
                                                                a.direct_orders_1d AS total_order_sku,
                                                                a.total_conversion_sales_1d AS total_conversion_sales,
                                                                a.direct_conversion_sales_1d AS total_conversion_sales_sku,
                                                                a.file_name,
                                                                'COUPANG'::character varying AS retailer,
                                                                'COUPANG-BPA'::character varying AS sub_retailer,
                                                                (a.total_conversion_sales_1d)::numeric(20, 4) AS sales_gmv,
                                                                (a.direct_conversion_sales_1d)::numeric(20, 4) AS sales_gmv_sku,
                                                                (trim((d.sku_people)::text))::character varying AS product_name,
                                                                (trim((d.barcode)::text))::character varying AS barcode,
                                                                a.ad_execution_option_id AS sku_id,
                                                                NULL::character varying AS pa_cost,
                                                                a.ad_cost AS bpa_cost,
                                                                NULL::character varying AS sa_cost,
                                                                a.crtd_dttm AS data_refresh_time,
                                                                COALESCE(
                                                                    e.keyword_group,
                                                                    'other keyword'::character varying
                                                                ) AS keyword_group,
                                                                NULL::character varying AS observed_price,
                                                                NULL::character varying AS rocket_wow_price,
                                                                d.product_category_high AS category_1,
                                                                d.product_category_mid AS category_2,
                                                                d.product_category_low AS category_3,
                                                                NULL::character varying AS ranking,
                                                                NULL::character varying AS total_monthly_search_volume,
                                                                NULL::character varying AS payment_amount
                                                            FROM (
                                                                    (
                                                                        (
                                                                            itg_kr_coupang_bpa_report a
                                                                            LEFT JOIN (
                                                                                SELECT DISTINCT itg_mds_kr_brand_campaign_promotion.code,
                                                                                    itg_mds_kr_brand_campaign_promotion.brand_code
                                                                                FROM itg_mds_kr_brand_campaign_promotion
                                                                            ) b ON (
                                                                                (
                                                                                    ((trim((b.code)::text))::character varying)::text = (
                                                                                        (trim((a.campaign_name)::text))::character varying
                                                                                    )::text
                                                                                )
                                                                            )
                                                                        )
                                                                        LEFT JOIN (
                                                                            SELECT e.transaction_date,
                                                                                e.vendor_item_id,
                                                                                g.sku_people,
                                                                                e.barcode,
                                                                                e.product_category_high,
                                                                                e.product_category_mid,
                                                                                e.product_category_low
                                                                            FROM (
                                                                                    (
                                                                                        SELECT derived_table2.transaction_date,
                                                                                            derived_table2.vendor_item_id,
                                                                                            derived_table2.sku_people,
                                                                                            derived_table2.barcode,
                                                                                            derived_table2.product_category_high,
                                                                                            derived_table2.product_category_mid,
                                                                                            derived_table2.product_category_low,
                                                                                            derived_table2.rn
                                                                                        FROM (
                                                                                                SELECT DISTINCT itg_kr_coupang_productsalereport.transaction_date,
                                                                                                    itg_kr_coupang_productsalereport.vendor_item_id,
                                                                                                    itg_kr_coupang_productsalereport.sku_people,
                                                                                                    itg_kr_coupang_productsalereport.barcode,
                                                                                                    itg_kr_coupang_productsalereport.product_category_high,
                                                                                                    itg_kr_coupang_productsalereport.product_category_mid,
                                                                                                    itg_kr_coupang_productsalereport.product_category_low,
                                                                                                    row_number() OVER(
                                                                                                        PARTITION BY itg_kr_coupang_productsalereport.transaction_date,
                                                                                                        itg_kr_coupang_productsalereport.vendor_item_id
                                                                                                        ORDER BY length(
                                                                                                                (
                                                                                                                    COALESCE(
                                                                                                                        itg_kr_coupang_productsalereport.product_category_high,
                                                                                                                        ''::character varying
                                                                                                                    )
                                                                                                                )::text
                                                                                                            ) DESC
                                                                                                    ) AS rn
                                                                                                FROM itg_kr_coupang_productsalereport
                                                                                            ) derived_table2
                                                                                        WHERE (derived_table2.rn = 1)
                                                                                    ) e
                                                                                    LEFT JOIN (
                                                                                        SELECT f.sku_people,
                                                                                            f.barcode,
                                                                                            f.rno
                                                                                        FROM (
                                                                                                SELECT DISTINCT itg_kr_coupang_productsalereport.sku_people,
                                                                                                    itg_kr_coupang_productsalereport.barcode,
                                                                                                    row_number() OVER(
                                                                                                        PARTITION BY itg_kr_coupang_productsalereport.barcode
                                                                                                        ORDER BY itg_kr_coupang_productsalereport.transaction_date DESC
                                                                                                    ) AS rno
                                                                                                FROM itg_kr_coupang_productsalereport
                                                                                            ) f
                                                                                        WHERE (f.rno = 1)
                                                                                    ) g ON (((e.barcode)::text = (g.barcode)::text))
                                                                                )
                                                                        ) d ON (
                                                                            (
                                                                                (
                                                                                    (a.date)::text = "replace"((d.transaction_date)::text, '-'::text, ''::text)
                                                                                )
                                                                                AND (
                                                                                    trim((a.ad_execution_option_id)::text) = trim((d.vendor_item_id)::text)
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                    LEFT JOIN (
                                                                        SELECT DISTINCT "replace"(
                                                                                lower(
                                                                                    (
                                                                                        COALESCE(
                                                                                            itg_mds_kr_keyword_classifications.keyword,
                                                                                            '[비검색광고영역]'::character varying
                                                                                        )
                                                                                    )::text
                                                                                ),
                                                                                ' '::text,
                                                                                ''::text
                                                                            ) AS keyword2,
                                                                            itg_mds_kr_keyword_classifications.keyword_group
                                                                        FROM itg_mds_kr_keyword_classifications
                                                                    ) e ON (
                                                                        (
                                                                            "replace"(
                                                                                lower(
                                                                                    (
                                                                                        COALESCE(
                                                                                            a.impressed_keywords,
                                                                                            '[비검색광고영역]'::character varying
                                                                                        )
                                                                                    )::text
                                                                                ),
                                                                                ' '::text,
                                                                                ''::text
                                                                            ) = e.keyword2
                                                                        )
                                                                    )
                                                                )
                                                        )
                                                        UNION ALL
                                                        SELECT (
                                                                "replace"((d.transaction_date)::text, '-'::text, ''::text)
                                                            )::character varying AS date,
                                                            NULL::character varying AS campaign_name,
                                                            NULL::character varying AS campaign_id,
                                                            (d.brand)::character varying AS brand,
                                                            NULL::character varying AS keyword,
                                                            NULL::character varying AS search_area,
                                                            'non-SA'::character varying AS ad_types,
                                                            NULL::character varying AS click,
                                                            NULL::character varying AS impression,
                                                            NULL::character varying AS ad_cost,
                                                            NULL::character varying AS total_order,
                                                            NULL::character varying AS total_order_sku,
                                                            NULL::character varying AS total_conversion_sales,
                                                            NULL::character varying AS total_conversion_sales_sku,
                                                            d.file_name,
                                                            'COUPANG'::character varying AS retailer,
                                                            'COUPANG-GMV'::character varying AS sub_retailer,
                                                            d.sales_gmv,
                                                            d.sales_gmv AS sales_gmv_sku,
                                                            (trim((g.sku_people)::text))::character varying AS product_name,
                                                            (trim((d.barcode)::text))::character varying AS barcode,
                                                            d.vendor_item_id AS sku_id,
                                                            NULL::character varying AS pa_cost,
                                                            NULL::character varying AS bpa_cost,
                                                            NULL::character varying AS sa_cost,
                                                            d.updt_dttm AS data_refresh_time,
                                                            'non-SA'::character varying AS keyword_group,
                                                            NULL::character varying AS observed_price,
                                                            NULL::character varying AS rocket_wow_price,
                                                            d.product_category_high AS category_1,
                                                            d.product_category_mid AS category_2,
                                                            d.product_category_low AS category_3,
                                                            NULL::character varying AS ranking,
                                                            NULL::character varying AS total_monthly_search_volume,
                                                            NULL::character varying AS payment_amount
                                                        FROM (
                                                                (
                                                                    SELECT derived_table3.sales_gmv,
                                                                        derived_table3.transaction_date,
                                                                        derived_table3.sku_people,
                                                                        derived_table3.sku_id,
                                                                        derived_table3.vendor_item_id,
                                                                        derived_table3.barcode,
                                                                        derived_table3.product_category_high,
                                                                        derived_table3.product_category_mid,
                                                                        derived_table3.product_category_low,
                                                                        derived_table3.rn,
                                                                        derived_table3.brand,
                                                                        derived_table3.file_name,
                                                                        derived_table3.updt_dttm
                                                                    FROM (
                                                                            SELECT DISTINCT itg_kr_coupang_productsalereport.sales_gmv,
                                                                                itg_kr_coupang_productsalereport.transaction_date,
                                                                                itg_kr_coupang_productsalereport.sku_people,
                                                                                itg_kr_coupang_productsalereport.sku_id,
                                                                                itg_kr_coupang_productsalereport.vendor_item_id,
                                                                                itg_kr_coupang_productsalereport.barcode,
                                                                                itg_kr_coupang_productsalereport.product_category_high,
                                                                                itg_kr_coupang_productsalereport.product_category_mid,
                                                                                itg_kr_coupang_productsalereport.product_category_low,
                                                                                row_number() OVER(
                                                                                    PARTITION BY itg_kr_coupang_productsalereport.transaction_date,
                                                                                    itg_kr_coupang_productsalereport.vendor_item_id
                                                                                    ORDER BY length(
                                                                                            (
                                                                                                COALESCE(
                                                                                                    itg_kr_coupang_productsalereport.product_category_high,
                                                                                                    ''::character varying
                                                                                                )
                                                                                            )::text
                                                                                        ) DESC
                                                                                ) AS rn,
                                                                                CASE
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '아비노'::text
                                                                                    ) THEN 'Aveeno H&B'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '아비노베이비'::text
                                                                                    ) THEN 'Aveeno Baby'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '리스테린'::text
                                                                                    ) THEN 'Listerine'::text
                                                                                    WHEN (
                                                                                        (
                                                                                            trim((itg_kr_coupang_productsalereport.brand)::text) = '뉴트로지나'::text
                                                                                        )
                                                                                        AND (
                                                                                            (
                                                                                                itg_kr_coupang_productsalereport.product_category_high
                                                                                            )::text = 'Beauty'::text
                                                                                        )
                                                                                    ) THEN 'Neut.Cleansing'::text
                                                                                    WHEN (
                                                                                        (
                                                                                            trim((itg_kr_coupang_productsalereport.brand)::text) = '뉴트로지나'::text
                                                                                        )
                                                                                        AND (
                                                                                            (
                                                                                                itg_kr_coupang_productsalereport.product_category_high
                                                                                            )::text <> 'Beauty'::text
                                                                                        )
                                                                                    ) THEN 'Neut.H&B'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '존슨즈베이비'::text
                                                                                    ) THEN 'Johnson\'s Baby'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '오지엑스'::text
                                                                                    ) THEN 'OGX'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '클린앤클리어'::text
                                                                                    ) THEN 'C&C'::text
                                                                                    ELSE ''::text
                                                                                END AS brand,
                                                                                itg_kr_coupang_productsalereport.file_name,
                                                                                itg_kr_coupang_productsalereport.updt_dttm
                                                                            FROM itg_kr_coupang_productsalereport
                                                                        ) derived_table3
                                                                    WHERE (derived_table3.rn = 1)
                                                                ) d
                                                                LEFT JOIN (
                                                                    SELECT f.sku_people,
                                                                        f.barcode,
                                                                        f.rno
                                                                    FROM (
                                                                            SELECT DISTINCT itg_kr_coupang_productsalereport.sku_people,
                                                                                itg_kr_coupang_productsalereport.barcode,
                                                                                row_number() OVER(
                                                                                    PARTITION BY itg_kr_coupang_productsalereport.barcode
                                                                                    ORDER BY itg_kr_coupang_productsalereport.transaction_date DESC
                                                                                ) AS rno
                                                                            FROM itg_kr_coupang_productsalereport
                                                                        ) f
                                                                    WHERE (f.rno = 1)
                                                                ) g ON (((d.barcode)::text = (g.barcode)::text))
                                                            )
                                                    )
                                                    UNION ALL
                                                    SELECT (
                                                            "replace"((d.transaction_date)::text, '-'::text, ''::text)
                                                        )::character varying AS date,
                                                        NULL::character varying AS campaign_name,
                                                        NULL::character varying AS campaign_id,
                                                        (d.brand)::character varying AS brand,
                                                        NULL::character varying AS keyword,
                                                        NULL::character varying AS search_area,
                                                        'non-SA'::character varying AS ad_types,
                                                        NULL::character varying AS click,
                                                        NULL::character varying AS impression,
                                                        NULL::character varying AS ad_cost,
                                                        NULL::character varying AS total_order,
                                                        NULL::character varying AS total_order_sku,
                                                        NULL::character varying AS total_conversion_sales,
                                                        NULL::character varying AS total_conversion_sales_sku,
                                                        d.file_name,
                                                        'COUPANG'::character varying AS retailer,
                                                        'COUPANG-GMV'::character varying AS sub_retailer,
                                                        NULL::numeric AS sales_gmv,
                                                        NULL::numeric AS sales_gmv_sku,
                                                        (trim((g.sku_people)::text))::character varying AS product_name,
                                                        (trim((d.barcode)::text))::character varying AS barcode,
                                                        d.vendor_item_id AS sku_id,
                                                        NULL::character varying AS pa_cost,
                                                        NULL::character varying AS bpa_cost,
                                                        NULL::character varying AS sa_cost,
                                                        d.updt_dttm AS data_refresh_time,
                                                        'non-SA'::character varying AS keyword_group,
                                                        f.observed_price,
                                                        (
                                                            regexp_substr(
                                                                "replace"((f.promotion_text)::text, ','::text, ''::text),
                                                                '[0-9]*'::text,
                                                                7
                                                            )
                                                        )::character varying AS rocket_wow_price,
                                                        d.product_category_high AS category_1,
                                                        d.product_category_mid AS category_2,
                                                        d.product_category_low AS category_3,
                                                        NULL::character varying AS ranking,
                                                        NULL::character varying AS total_monthly_search_volume,
                                                        NULL::character varying AS payment_amount
                                                    FROM (
                                                            (
                                                                (
                                                                    SELECT derived_table4.transaction_date,
                                                                        derived_table4.sku_people,
                                                                        derived_table4.sku_id,
                                                                        derived_table4.vendor_item_id,
                                                                        derived_table4.barcode,
                                                                        derived_table4.product_category_high,
                                                                        derived_table4.product_category_mid,
                                                                        derived_table4.product_category_low,
                                                                        derived_table4.rn,
                                                                        derived_table4.brand,
                                                                        derived_table4.file_name,
                                                                        derived_table4.updt_dttm
                                                                    FROM (
                                                                            SELECT DISTINCT itg_kr_coupang_productsalereport.transaction_date,
                                                                                itg_kr_coupang_productsalereport.sku_people,
                                                                                itg_kr_coupang_productsalereport.sku_id,
                                                                                itg_kr_coupang_productsalereport.vendor_item_id,
                                                                                itg_kr_coupang_productsalereport.barcode,
                                                                                itg_kr_coupang_productsalereport.product_category_high,
                                                                                itg_kr_coupang_productsalereport.product_category_mid,
                                                                                itg_kr_coupang_productsalereport.product_category_low,
                                                                                row_number() OVER(
                                                                                    PARTITION BY itg_kr_coupang_productsalereport.transaction_date,
                                                                                    itg_kr_coupang_productsalereport.sku_id
                                                                                    ORDER BY length(
                                                                                            (
                                                                                                COALESCE(
                                                                                                    itg_kr_coupang_productsalereport.product_category_high,
                                                                                                    ''::character varying
                                                                                                )
                                                                                            )::text
                                                                                        ) DESC
                                                                                ) AS rn,
                                                                                CASE
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '아비노'::text
                                                                                    ) THEN 'Aveeno H&B'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '아비노베이비'::text
                                                                                    ) THEN 'Aveeno Baby'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '리스테린'::text
                                                                                    ) THEN 'Listerine'::text
                                                                                    WHEN (
                                                                                        (
                                                                                            trim((itg_kr_coupang_productsalereport.brand)::text) = '뉴트로지나'::text
                                                                                        )
                                                                                        AND (
                                                                                            (
                                                                                                itg_kr_coupang_productsalereport.product_category_high
                                                                                            )::text = 'Beauty'::text
                                                                                        )
                                                                                    ) THEN 'Neut.Cleansing'::text
                                                                                    WHEN (
                                                                                        (
                                                                                            trim((itg_kr_coupang_productsalereport.brand)::text) = '뉴트로지나'::text
                                                                                        )
                                                                                        AND (
                                                                                            (
                                                                                                itg_kr_coupang_productsalereport.product_category_high
                                                                                            )::text <> 'Beauty'::text
                                                                                        )
                                                                                    ) THEN 'Neut.H&B'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '존슨즈베이비'::text
                                                                                    ) THEN 'Johnson\'s Baby'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '오지엑스'::text
                                                                                    ) THEN 'OGX'::text
                                                                                    WHEN (
                                                                                        trim((itg_kr_coupang_productsalereport.brand)::text) = '클린앤클리어'::text
                                                                                    ) THEN 'C&C'::text
                                                                                    ELSE ''::text
                                                                                END AS brand,
                                                                                itg_kr_coupang_productsalereport.file_name,
                                                                                itg_kr_coupang_productsalereport.updt_dttm
                                                                            FROM itg_kr_coupang_productsalereport
                                                                        ) derived_table4
                                                                    WHERE (derived_table4.rn = 1)
                                                                ) d
                                                                LEFT JOIN (
                                                                    SELECT f.sku_people,
                                                                        f.barcode,
                                                                        f.rno
                                                                    FROM (
                                                                            SELECT DISTINCT itg_kr_coupang_productsalereport.sku_people,
                                                                                itg_kr_coupang_productsalereport.barcode,
                                                                                row_number() OVER(
                                                                                    PARTITION BY itg_kr_coupang_productsalereport.barcode
                                                                                    ORDER BY itg_kr_coupang_productsalereport.transaction_date DESC
                                                                                ) AS rno
                                                                            FROM itg_kr_coupang_productsalereport
                                                                        ) f
                                                                    WHERE (f.rno = 1)
                                                                ) g ON (((d.barcode)::text = (g.barcode)::text))
                                                            )
                                                            LEFT JOIN (
                                                                SELECT derived_table5.trusted_upc,
                                                                    derived_table5.report_date,
                                                                    derived_table5.observed_price,
                                                                    derived_table5.promotion_text
                                                                FROM (
                                                                        SELECT DISTINCT itg_kr_dads_coupang_price.trusted_upc,
                                                                            itg_kr_dads_coupang_price.report_date,
                                                                            itg_kr_dads_coupang_price.observed_price,
                                                                            itg_kr_dads_coupang_price.promotion_text
                                                                        FROM itg_kr_dads_coupang_price
                                                                    ) derived_table5
                                                            ) f ON (
                                                                (
                                                                    (
                                                                        d.transaction_date = CASE
                                                                            WHEN ("position"((f.report_date)::text, '/'::text) > 0) THEN to_date((f.report_date)::text, 'MM/DD/YYYY'::text)
                                                                            ELSE (f.report_date)::date
                                                                        END
                                                                    )
                                                                    AND (
                                                                        ltrim((d.sku_id)::text, (0)::text) = ltrim((f.trusted_upc)::text, (0)::text)
                                                                    )
                                                                )
                                                            )
                                                        )
                                                )
                                                UNION ALL
                                                SELECT a.date,
                                                    a.campaign_name,
                                                    a.campaign_id,
                                                    (trim((b.brand_code)::text))::character varying AS brand,
                                                    NULL::character varying AS keyword,
                                                    NULL::character varying AS search_area,
                                                    a.ad_types,
                                                    NULL::character varying AS click,
                                                    NULL::character varying AS impression,
                                                    NULL::character varying AS ad_cost,
                                                    NULL::character varying AS total_order,
                                                    NULL::character varying AS total_order_sku,
                                                    NULL::character varying AS total_conversion_sales,
                                                    NULL::character varying AS total_conversion_sales_sku,
                                                    a.file_name,
                                                    'COUPANG'::character varying AS retailer,
                                                    'COUPANG-PA_extra'::character varying AS sub_retailer,
                                                    (- (a.total_conversion_sales_1d)::numeric(20, 4)) AS sales_gmv,
                                                    (- (a.direct_conversion_sales_1d)::numeric(20, 4)) AS sales_gmv_sku,
                                                    (trim((d.sku_people)::text))::character varying AS product_name,
                                                    (trim((d.barcode)::text))::character varying AS barcode,
                                                    a.ad_execution_option_id AS sku_id,
                                                    NULL::character varying AS pa_cost,
                                                    NULL::character varying AS bpa_cost,
                                                    NULL::character varying AS sa_cost,
                                                    a.crtd_dttm AS data_refresh_time,
                                                    'non-SA'::character varying AS keyword_group,
                                                    NULL::character varying AS observed_price,
                                                    NULL::character varying AS rocket_wow_price,
                                                    d.product_category_high AS category_1,
                                                    d.product_category_mid AS category_2,
                                                    d.product_category_low AS category_3,
                                                    NULL::character varying AS ranking,
                                                    NULL::character varying AS total_monthly_search_volume,
                                                    NULL::character varying AS payment_amount
                                                FROM (
                                                        (
                                                            itg_kr_coupang_pa_report a
                                                            LEFT JOIN (
                                                                SELECT DISTINCT itg_mds_kr_brand_campaign_promotion.code,
                                                                    itg_mds_kr_brand_campaign_promotion.brand_code
                                                                FROM itg_mds_kr_brand_campaign_promotion
                                                            ) b ON (
                                                                (
                                                                    ((trim((b.code)::text))::character varying)::text = (
                                                                        (trim((a.campaign_name)::text))::character varying
                                                                    )::text
                                                                )
                                                            )
                                                        )
                                                        LEFT JOIN (
                                                            SELECT e.transaction_date,
                                                                e.vendor_item_id,
                                                                g.sku_people,
                                                                e.barcode,
                                                                e.product_category_high,
                                                                e.product_category_mid,
                                                                e.product_category_low
                                                            FROM (
                                                                    (
                                                                        SELECT derived_table6.transaction_date,
                                                                            derived_table6.vendor_item_id,
                                                                            derived_table6.sku_people,
                                                                            derived_table6.barcode,
                                                                            derived_table6.product_category_high,
                                                                            derived_table6.product_category_mid,
                                                                            derived_table6.product_category_low,
                                                                            derived_table6.rn
                                                                        FROM (
                                                                                SELECT DISTINCT itg_kr_coupang_productsalereport.transaction_date,
                                                                                    itg_kr_coupang_productsalereport.vendor_item_id,
                                                                                    itg_kr_coupang_productsalereport.sku_people,
                                                                                    itg_kr_coupang_productsalereport.barcode,
                                                                                    itg_kr_coupang_productsalereport.product_category_high,
                                                                                    itg_kr_coupang_productsalereport.product_category_mid,
                                                                                    itg_kr_coupang_productsalereport.product_category_low,
                                                                                    row_number() OVER(
                                                                                        PARTITION BY itg_kr_coupang_productsalereport.transaction_date,
                                                                                        itg_kr_coupang_productsalereport.vendor_item_id
                                                                                        ORDER BY length(
                                                                                                (
                                                                                                    COALESCE(
                                                                                                        itg_kr_coupang_productsalereport.product_category_high,
                                                                                                        ''::character varying
                                                                                                    )
                                                                                                )::text
                                                                                            ) DESC
                                                                                    ) AS rn
                                                                                FROM itg_kr_coupang_productsalereport
                                                                            ) derived_table6
                                                                        WHERE (derived_table6.rn = 1)
                                                                    ) e
                                                                    LEFT JOIN (
                                                                        SELECT f.sku_people,
                                                                            f.barcode,
                                                                            f.rno
                                                                        FROM (
                                                                                SELECT DISTINCT itg_kr_coupang_productsalereport.sku_people,
                                                                                    itg_kr_coupang_productsalereport.barcode,
                                                                                    row_number() OVER(
                                                                                        PARTITION BY itg_kr_coupang_productsalereport.barcode
                                                                                        ORDER BY itg_kr_coupang_productsalereport.transaction_date DESC
                                                                                    ) AS rno
                                                                                FROM itg_kr_coupang_productsalereport
                                                                            ) f
                                                                        WHERE (f.rno = 1)
                                                                    ) g ON (((e.barcode)::text = (g.barcode)::text))
                                                                )
                                                        ) d ON (
                                                            (
                                                                (
                                                                    (a.date)::text = "replace"((d.transaction_date)::text, '-'::text, ''::text)
                                                                )
                                                                AND (
                                                                    trim((a.ad_execution_option_id)::text) = trim((d.vendor_item_id)::text)
                                                                )
                                                            )
                                                        )
                                                    )
                                            )
                                            UNION ALL
                                            SELECT a.date,
                                                a.campaign_name,
                                                a.campaign_id,
                                                (trim((b.brand_code)::text))::character varying AS brand,
                                                NULL::character varying AS keyword,
                                                NULL::character varying AS search_area,
                                                'BPA'::character varying AS ad_types,
                                                NULL::character varying AS click,
                                                NULL::character varying AS impression,
                                                NULL::character varying AS ad_cost,
                                                NULL::character varying AS total_order,
                                                NULL::character varying AS total_order_sku,
                                                NULL::character varying AS total_conversion_sales,
                                                NULL::character varying AS total_conversion_sales_sku,
                                                a.file_name,
                                                'COUPANG'::character varying AS retailer,
                                                'COUPANG-BPA_extra'::character varying AS sub_retailer,
                                                (- (a.total_conversion_sales_1d)::numeric(20, 4)) AS sales_gmv,
                                                (- (a.direct_conversion_sales_1d)::numeric(20, 4)) AS sales_gmv_sku,
                                                (trim((d.sku_people)::text))::character varying AS product_name,
                                                (trim((d.barcode)::text))::character varying AS barcode,
                                                a.ad_execution_option_id AS sku_id,
                                                NULL::character varying AS pa_cost,
                                                NULL::character varying AS bpa_cost,
                                                NULL::character varying AS sa_cost,
                                                a.crtd_dttm AS data_refresh_time,
                                                'non-SA'::character varying AS keyword_group,
                                                NULL::character varying AS observed_price,
                                                NULL::character varying AS rocket_wow_price,
                                                d.product_category_high AS category_1,
                                                d.product_category_mid AS category_2,
                                                d.product_category_low AS category_3,
                                                NULL::character varying AS ranking,
                                                NULL::character varying AS total_monthly_search_volume,
                                                NULL::character varying AS payment_amount
                                            FROM (
                                                    (
                                                        itg_kr_coupang_bpa_report a
                                                        LEFT JOIN (
                                                            SELECT DISTINCT itg_mds_kr_brand_campaign_promotion.code,
                                                                itg_mds_kr_brand_campaign_promotion.brand_code
                                                            FROM itg_mds_kr_brand_campaign_promotion
                                                        ) b ON (
                                                            (
                                                                ((trim((b.code)::text))::character varying)::text = (
                                                                    (trim((a.campaign_name)::text))::character varying
                                                                )::text
                                                            )
                                                        )
                                                    )
                                                    LEFT JOIN (
                                                        SELECT e.transaction_date,
                                                            e.vendor_item_id,
                                                            g.sku_people,
                                                            e.barcode,
                                                            e.product_category_high,
                                                            e.product_category_mid,
                                                            e.product_category_low
                                                        FROM (
                                                                (
                                                                    SELECT derived_table7.transaction_date,
                                                                        derived_table7.vendor_item_id,
                                                                        derived_table7.sku_people,
                                                                        derived_table7.barcode,
                                                                        derived_table7.product_category_high,
                                                                        derived_table7.product_category_mid,
                                                                        derived_table7.product_category_low,
                                                                        derived_table7.rn
                                                                    FROM (
                                                                            SELECT DISTINCT itg_kr_coupang_productsalereport.transaction_date,
                                                                                itg_kr_coupang_productsalereport.vendor_item_id,
                                                                                itg_kr_coupang_productsalereport.sku_people,
                                                                                itg_kr_coupang_productsalereport.barcode,
                                                                                itg_kr_coupang_productsalereport.product_category_high,
                                                                                itg_kr_coupang_productsalereport.product_category_mid,
                                                                                itg_kr_coupang_productsalereport.product_category_low,
                                                                                row_number() OVER(
                                                                                    PARTITION BY itg_kr_coupang_productsalereport.transaction_date,
                                                                                    itg_kr_coupang_productsalereport.vendor_item_id
                                                                                    ORDER BY length(
                                                                                            (
                                                                                                COALESCE(
                                                                                                    itg_kr_coupang_productsalereport.product_category_high,
                                                                                                    ''::character varying
                                                                                                )
                                                                                            )::text
                                                                                        ) DESC
                                                                                ) AS rn
                                                                            FROM itg_kr_coupang_productsalereport
                                                                        ) derived_table7
                                                                    WHERE (derived_table7.rn = 1)
                                                                ) e
                                                                LEFT JOIN (
                                                                    SELECT f.sku_people,
                                                                        f.barcode,
                                                                        f.rno
                                                                    FROM (
                                                                            SELECT DISTINCT itg_kr_coupang_productsalereport.sku_people,
                                                                                itg_kr_coupang_productsalereport.barcode,
                                                                                row_number() OVER(
                                                                                    PARTITION BY itg_kr_coupang_productsalereport.barcode
                                                                                    ORDER BY itg_kr_coupang_productsalereport.transaction_date DESC
                                                                                ) AS rno
                                                                            FROM itg_kr_coupang_productsalereport
                                                                        ) f
                                                                    WHERE (f.rno = 1)
                                                                ) g ON (((e.barcode)::text = (g.barcode)::text))
                                                            )
                                                    ) d ON (
                                                        (
                                                            (
                                                                (a.date)::text = "replace"((d.transaction_date)::text, '-'::text, ''::text)
                                                            )
                                                            AND (
                                                                trim((a.ad_execution_option_id)::text) = trim((d.vendor_item_id)::text)
                                                            )
                                                        )
                                                    )
                                                )
                                        )
                                        UNION ALL
                                        SELECT a.file_date AS date,
                                            a.campaign_name,
                                            NULL::character varying AS campaign_id,
                                            (trim((b.brands_name)::text))::character varying AS brand,
                                            (
                                                "replace"(
                                                    lower(
                                                        (
                                                            COALESCE(a.keyword, '[비검색광고영역]'::character varying)
                                                        )::text
                                                    ),
                                                    ' '::text,
                                                    ''::text
                                                )
                                            )::character varying AS keyword,
                                            NULL::character varying AS search_area,
                                            NULL::character varying AS ad_types,
                                            trunc(a.click_count::number(18,4),0) AS click,
                                            trunc(a.impression::number(18,4),0) as impression,
                                            trunc(a.consumed_cost::number(18,4),0) AS ad_cost,
                                            trunc(a.conversion_count::number(18,4),0) AS total_order,
                                            NULL::character varying AS total_order_sku,
                                            trunc(a.purchased_amount::number(18,4),0) AS total_conversion_sales,
                                            NULL::character varying AS total_conversion_sales_sku,
                                            a.file_name,
                                            'NAVER'::character varying AS retailer,
                                            'NAVER'::character varying AS sub_retailer,
                                            trunc((a.purchased_amount)::numeric(20, 8),4) AS sales_gmv,
                                            NULL::numeric AS sales_gmv_sku,
                                            NULL::character varying AS product_name,
                                            NULL::character varying AS barcode,
                                            NULL::character varying AS sku_id,
                                            NULL::character varying AS pa_cost,
                                            NULL::character varying AS bpa_cost,
                                            trunc(a.consumed_cost::number(18,4),0) AS sa_cost,
                                            a.crtd_dttm AS data_refresh_time,
                                            COALESCE(
                                                e.keyword_group,
                                                'other keyword'::character varying
                                            ) AS keyword_group,
                                            NULL::character varying AS observed_price,
                                            NULL::character varying AS rocket_wow_price,
                                            b.category_l_name AS category_1,
                                            b.category_m_name AS category_2,
                                            b.category_s_name AS category_3,
                                            NULL::character varying AS ranking,
                                            NULL::character varying AS total_monthly_search_volume,
                                            NULL::character varying AS payment_amount
                                        FROM (
                                                (
                                                    itg_kr_dads_linkprice a
                                                    LEFT JOIN itg_mds_kr_naver_product_master b ON (
                                                        (
                                                            ((trim((b.code)::text))::character varying)::text = (
                                                                (trim((a.product_number)::text))::character varying
                                                            )::text
                                                        )
                                                    )
                                                )
                                                LEFT JOIN (
                                                    SELECT DISTINCT "replace"(
                                                            lower(
                                                                (
                                                                    COALESCE(
                                                                        itg_mds_kr_keyword_classifications.keyword,
                                                                        '[비검색광고영역]'::character varying
                                                                    )
                                                                )::text
                                                            ),
                                                            ' '::text,
                                                            ''::text
                                                        ) AS keyword2,
                                                        itg_mds_kr_keyword_classifications.keyword_group
                                                    FROM itg_mds_kr_keyword_classifications
                                                ) e ON (
                                                    (
                                                        "replace"(
                                                            lower(
                                                                (
                                                                    COALESCE(a.keyword, '[비검색광고영역]'::character varying)
                                                                )::text
                                                            ),
                                                            ' '::text,
                                                            ''::text
                                                        ) = e.keyword2
                                                    )
                                                )
                                            )
                                    )
                                    UNION ALL
                                    SELECT a.file_date AS date,
                                        NULL::character varying AS campaign_name,
                                        NULL::character varying AS campaign_id,
                                        (trim((b.brands_name)::text))::character varying AS brand,
                                        NULL::character varying AS keyword,
                                        NULL::character varying AS search_area,
                                        NULL::character varying AS ad_types,
                                        NULL::character varying AS click,
                                        NULL::character varying AS impression,
                                        NULL::character varying AS ad_cost,
                                        NULL::character varying AS total_order,
                                        NULL::character varying AS total_order_sku,
                                        NULL::character varying AS total_conversion_sales,
                                        NULL::character varying AS total_conversion_sales_sku,
                                        a.file_name,
                                        'NAVER'::character varying AS retailer,
                                        'NAVER-GMV'::character varying AS sub_retailer,
                                        (a.payment_amount)::numeric(20, 4) AS sales_gmv,
                                        NULL::numeric AS sales_gmv_sku,
                                        NULL::character varying AS product_name,
                                        NULL::character varying AS barcode,
                                        NULL::character varying AS sku_id,
                                        NULL::character varying AS pa_cost,
                                        NULL::character varying AS bpa_cost,
                                        NULL::character varying AS sa_cost,
                                        a.crtd_dttm AS data_refresh_time,
                                        'non-SA'::character varying AS keyword_group,
                                        NULL::character varying AS observed_price,
                                        NULL::character varying AS rocket_wow_price,
                                        a.product_category_l AS category_1,
                                        a.product_category_m AS category_2,
                                        a.product_category_s AS category_3,
                                        NULL::character varying AS ranking,
                                        NULL::character varying AS total_monthly_search_volume,
                                        NULL::character varying AS payment_amount
                                    FROM (
                                            itg_kr_dads_naver_gmv a
                                            LEFT JOIN itg_mds_kr_naver_product_master b ON (
                                                (
                                                    trim((a.product_id)::text) = trim((b.code)::text)
                                                )
                                            )
                                        )
                                )
                                UNION ALL
                                SELECT a.file_date AS date,
                                    a.campaign_name,
                                    NULL::character varying AS campaign_id,
                                    b.brands_name AS brand,
                                    NULL::character varying AS keyword,
                                    NULL::character varying AS search_area,
                                    NULL::character varying AS ad_types,
                                    NULL::character varying AS click,
                                    NULL::character varying AS impression,
                                    NULL::character varying AS ad_cost,
                                    NULL::character varying AS total_order,
                                    NULL::character varying AS total_order_sku,
                                    NULL::character varying AS total_conversion_sales,
                                    NULL::character varying AS total_conversion_sales_sku,
                                    a.file_name,
                                    'NAVER'::character varying AS retailer,
                                    'NAVER-Extra'::character varying AS sub_retailer,
                                    (- trunc((a.purchased_amount)::numeric(20, 8),4)) AS sales_gmv,
                                    NULL::numeric AS sales_gmv_sku,
                                    NULL::character varying AS product_name,
                                    NULL::character varying AS barcode,
                                    NULL::character varying AS sku_id,
                                    NULL::character varying AS pa_cost,
                                    NULL::character varying AS bpa_cost,
                                    NULL::character varying AS sa_cost,
                                    a.crtd_dttm AS data_refresh_time,
                                    'non-SA'::character varying AS keyword_group,
                                    NULL::character varying AS observed_price,
                                    NULL::character varying AS rocket_wow_price,
                                    b.category_l_name AS category_1,
                                    b.category_m_name AS category_2,
                                    b.category_s_name AS category_3,
                                    NULL::character varying AS ranking,
                                    NULL::character varying AS total_monthly_search_volume,
                                    NULL::character varying AS payment_amount
                                FROM (
                                        itg_kr_dads_linkprice a
                                        LEFT JOIN itg_mds_kr_naver_product_master b ON (((b.code)::text = (a.product_number)::text))
                                    )
                            ) abc
                            LEFT JOIN (
                                SELECT (edw_intrm_calendar.fisc_wk_num)::character varying AS fisc_wk_num,
                                    edw_intrm_calendar.pstng_per,
                                    edw_intrm_calendar.cal_day,
                                    edw_intrm_calendar.fisc_per,
                                    edw_intrm_calendar.fisc_yr
                                FROM edw_intrm_calendar
                            ) tim ON (
                                (
                                    (abc.date)::text = "replace"(
                                        ((tim.cal_day)::character varying)::text,
                                        ('-'::character varying)::text,
                                        (''::character varying)::text
                                    )
                                )
                            )
                        )
                ) derived_table8
            GROUP BY derived_table8.brand,
                derived_table8.keyword,
                derived_table8.search_area,
                derived_table8.ad_types,
                derived_table8.retailer,
                derived_table8.sub_retailer,
                derived_table8.product_name,
                derived_table8.barcode,
                derived_table8.keyword_group,
                derived_table8.fisc_wk_num,
                derived_table8.fisc_mnth,
                derived_table8.fisc_per,
                derived_table8.fisc_day,
                derived_table8.category_1,
                derived_table8.category_2,
                derived_table8.category_3,
                derived_table8.ranking
            UNION ALL
            SELECT derived_table9.brand,
                (derived_table9.keyword)::character varying AS keyword,
                derived_table9.search_area,
                derived_table9.ad_types,
                derived_table9.retailer,
                derived_table9.sub_retailer,
                (derived_table9.product_name)::character varying AS product_name,
                derived_table9.barcode,
                derived_table9.keyword_group,
                derived_table9.fisc_wk_num,
                derived_table9.fisc_mnth,
                derived_table9.fisc_per,
                derived_table9.fisc_day,
                derived_table9.category_1,
                derived_table9.category_2,
                derived_table9.category_3,
                derived_table9.ranking,
                (derived_table9.click)::numeric(18, 0) AS click,
                (derived_table9.impression)::numeric(18, 0) AS impression,
                (derived_table9.ad_cost)::numeric(18, 0) AS ad_cost,
                (derived_table9.total_order)::numeric(18, 0) AS total_order,
                (derived_table9.total_order_sku)::numeric(18, 0) AS total_order_sku,
                (derived_table9.total_conversion_sales)::numeric(18, 0) AS total_conversion_sales,
                (derived_table9.total_conversion_sales_sku)::numeric(18, 0) AS total_conversion_sales_sku,
                (derived_table9.sales_gmv)::numeric(18, 0) AS sales_gmv,
                (derived_table9.sales_gmv_sku)::numeric(18, 0) AS sales_gmv_sku,
                (derived_table9.pa_cost)::numeric(18, 0) AS pa_cost,
                (derived_table9.bpa_cost)::numeric(18, 0) AS bpa_cost,
                (derived_table9.sa_cost)::numeric(18, 0) AS sa_cost,
                (derived_table9.observed_price)::numeric(18, 0) AS observed_price,
                (derived_table9.rocket_wow_price)::numeric(18, 0) AS rocket_wow_price,
                (derived_table9.total_monthly_search_volume)::numeric(18, 0) AS total_monthly_search_volume,
                (derived_table9.payment_amount)::numeric(18, 0) AS payment_amount
            FROM (
                    SELECT a.file_date AS date,
                        NULL::character varying AS campaign_name,
                        NULL::character varying AS campaign_id,
                        NULL::character varying AS brand,
                        "replace"(
                            lower(
                                (
                                    COALESCE(a.search_keyword, '[비검색광고영역]'::character varying)
                                )::text
                            ),
                            ' '::text,
                            ''::text
                        ) AS keyword,
                        NULL::character varying AS search_area,
                        NULL::character varying AS ad_types,
                        NULL::character varying AS click,
                        NULL::character varying AS impression,
                        NULL::character varying AS ad_cost,
                        NULL::character varying AS total_order,
                        NULL::character varying AS total_order_sku,
                        NULL::character varying AS total_conversion_sales,
                        NULL::character varying AS total_conversion_sales_sku,
                        a.file_name,
                        'COUPANG'::character varying AS retailer,
                        'COUPANG_SEARCH_KEYWORD'::character varying AS sub_retailer,
                        NULL::character varying AS sales_gmv,
                        NULL::character varying AS sales_gmv_sku,
                        trim((a.product_name)::text) AS product_name,
                        NULL::character varying AS barcode,
                        NULL::character varying AS sku_id,
                        NULL::character varying AS pa_cost,
                        NULL::character varying AS bpa_cost,
                        NULL::character varying AS sa_cost,
                        a.crtd_dttm AS data_refresh_time,
                        COALESCE(
                            e.keyword_group,
                            'other keyword'::character varying
                        ) AS keyword_group,
                        NULL::character varying AS observed_price,
                        NULL::character varying AS rocket_wow_price,
                        NULL::character varying AS category_1,
                        NULL::character varying AS category_2,
                        NULL::character varying AS category_3,
                        a.product_ranking AS ranking,
                        NULL::character varying AS total_monthly_search_volume,
                        NULL::character varying AS payment_amount,
                        '1'::character varying AS fisc_wk_num,
                        NULL::integer AS fisc_mnth,
                        NULL::date AS cal_day,
                        NULL::integer AS fisc_per,
                        to_date(
                            (
                                ((tim.fisc_yr)::text || '-'::text) || (tim.pstng_per)::text
                            ),
                            'YYYY-MM'::text
                        ) AS fisc_day
                    FROM (
                            (
                                itg_kr_dads_coupang_search_keyword a
                                LEFT JOIN (
                                    SELECT DISTINCT "replace"(
                                            lower(
                                                (
                                                    COALESCE(
                                                        itg_mds_kr_keyword_classifications.keyword,
                                                        '[비검색광고영역]'::character varying
                                                    )
                                                )::text
                                            ),
                                            ' '::text,
                                            ''::text
                                        ) AS keyword2,
                                        itg_mds_kr_keyword_classifications.keyword_group
                                    FROM itg_mds_kr_keyword_classifications
                                ) e ON (
                                    (
                                        "replace"(
                                            lower(
                                                (
                                                    COALESCE(a.search_keyword, '[비검색광고영역]'::character varying)
                                                )::text
                                            ),
                                            ' '::text,
                                            ''::text
                                        ) = e.keyword2
                                    )
                                )
                            )
                            LEFT JOIN edw_intrm_calendar tim ON (
                                (
                                    (a.file_date)::text = "replace"(
                                        ((tim.cal_day)::character varying)::text,
                                        ('-'::character varying)::text,
                                        (''::character varying)::text
                                    )
                                )
                            )
                        )
                ) derived_table9
        )
        UNION ALL
        SELECT derived_table10.brand,
            (derived_table10.keyword)::character varying AS keyword,
            derived_table10.search_area,
            derived_table10.ad_types,
            derived_table10.retailer,
            derived_table10.sub_retailer,
            derived_table10.product_name,
            derived_table10.barcode,
            derived_table10.keyword_group,
            derived_table10.fisc_wk_num,
            derived_table10.fisc_mnth,
            derived_table10.fisc_per,
            derived_table10.fisc_day,
            derived_table10.category_1,
            derived_table10.category_2,
            derived_table10.category_3,
            derived_table10.ranking,
            (derived_table10.click)::numeric(18, 0) AS click,
            (derived_table10.impression)::numeric(18, 0) AS impression,
            (derived_table10.ad_cost)::numeric(18, 0) AS ad_cost,
            (derived_table10.total_order)::numeric(18, 0) AS total_order,
            (derived_table10.total_order_sku)::numeric(18, 0) AS total_order_sku,
            (derived_table10.total_conversion_sales)::numeric(18, 0) AS total_conversion_sales,
            (derived_table10.total_conversion_sales_sku)::numeric(18, 0) AS total_conversion_sales_sku,
            (derived_table10.sales_gmv)::numeric(18, 0) AS sales_gmv,
            (derived_table10.sales_gmv_sku)::numeric(18, 0) AS sales_gmv_sku,
            (derived_table10.pa_cost)::numeric(18, 0) AS pa_cost,
            (derived_table10.bpa_cost)::numeric(18, 0) AS bpa_cost,
            (derived_table10.sa_cost)::numeric(18, 0) AS sa_cost,
            (derived_table10.observed_price)::numeric(18, 0) AS observed_price,
            (derived_table10.rocket_wow_price)::numeric(18, 0) AS rocket_wow_price,
            derived_table10.total_monthly_search_volume,
            derived_table10.payment_amount
        FROM (
                SELECT a.date,
                    NULL::character varying AS campaign_name,
                    NULL::character varying AS campaign_id,
                    NULL::character varying AS brand,
                    a.keyword,
                    NULL::character varying AS search_area,
                    NULL::character varying AS ad_types,
                    NULL::character varying AS click,
                    NULL::character varying AS impression,
                    NULL::character varying AS ad_cost,
                    NULL::character varying AS total_order,
                    NULL::character varying AS total_order_sku,
                    NULL::character varying AS total_conversion_sales,
                    NULL::character varying AS total_conversion_sales_sku,
                    NULL::character varying AS file_name,
                    'NAVER'::character varying AS retailer,
                    'NAVER_SEARCH_KEYWORD'::character varying AS sub_retailer,
                    NULL::character varying AS sales_gmv,
                    NULL::character varying AS sales_gmv_sku,
                    NULL::character varying AS product_name,
                    NULL::character varying AS barcode,
                    NULL::character varying AS sku_id,
                    NULL::character varying AS pa_cost,
                    NULL::character varying AS bpa_cost,
                    NULL::character varying AS sa_cost,
                    convert_timezone('UTC', current_timestamp())::timestamp without time zone AS data_refresh_time,
                    COALESCE(
                        e.keyword_group,
                        'other keyword'::character varying
                    ) AS keyword_group,
                    NULL::character varying AS observed_price,
                    NULL::character varying AS rocket_wow_price,
                    NULL::character varying AS category_1,
                    NULL::character varying AS category_2,
                    NULL::character varying AS category_3,
                    NULL::character varying AS ranking,
                    (a.total_monthly_search_volume)::numeric(20, 4) AS total_monthly_search_volume,
                    (b.payment_amount)::numeric(20, 4) AS payment_amount,
                    '1'::character varying AS fisc_wk_num,
                    NULL::integer AS fisc_mnth,
                    NULL::date AS cal_day,
                    NULL::integer AS fisc_per,
                    to_date(
                        (
                            (
                                substring((a.file_date)::text, 1, 4) || '-'::text
                            ) || substring((a.file_date)::text, 5, 2)
                        ),
                        'YYYY-MM'::text
                    ) AS fisc_day
                FROM (
                        (
                            (
                                SELECT r.keyword,
                                    r.total_monthly_search_volume,
                                    r.rno,
                                    r.date,
                                    r.file_date
                                FROM (
                                        SELECT "replace"(
                                                lower(
                                                    (
                                                        COALESCE(
                                                            itg_kr_dads_naver_keyword_search_volume.keyword,
                                                            '[비검색광고영역]'::character varying
                                                        )
                                                    )::text
                                                ),
                                                ' '::text,
                                                ''::text
                                            ) AS keyword,
                                            itg_kr_dads_naver_keyword_search_volume.total_monthly_search_volume,
                                            row_number() OVER(
                                                PARTITION BY "replace"(
                                                    lower(
                                                        (
                                                            COALESCE(
                                                                itg_kr_dads_naver_keyword_search_volume.keyword,
                                                                '[비검색광고영역]'::character varying
                                                            )
                                                        )::text
                                                    ),
                                                    ' '::text,
                                                    ''::text
                                                ),
                                                substring(
                                                    (
                                                        itg_kr_dads_naver_keyword_search_volume.file_date
                                                    )::text,
                                                    1,
                                                    6
                                                )
                                                ORDER BY itg_kr_dads_naver_keyword_search_volume.total_monthly_search_volume DESC
                                            ) AS rno,
                                            (
                                                substring(
                                                    (
                                                        itg_kr_dads_naver_keyword_search_volume.file_date
                                                    )::text,
                                                    1,
                                                    6
                                                ) || '01'::text
                                            ) AS date,
                                            itg_kr_dads_naver_keyword_search_volume.file_date
                                        FROM itg_kr_dads_naver_keyword_search_volume
                                    ) r
                                WHERE (r.rno = 1)
                            ) a
                            LEFT JOIN (
                                SELECT "replace"(
                                        lower(
                                            (
                                                COALESCE(
                                                    itg_kr_dads_naver_search_channel.keyword,
                                                    '[비검색광고영역]'::character varying
                                                )
                                            )::text
                                        ),
                                        ' '::text,
                                        ''::text
                                    ) AS keyword,
                                    sum(
                                        (itg_kr_dads_naver_search_channel.payment_amount)::numeric
                                    ) AS payment_amount,
                                    (
                                        substring(
                                            (itg_kr_dads_naver_search_channel.file_date)::text,
                                            1,
                                            6
                                        ) || '01'::text
                                    ) AS date
                                FROM itg_kr_dads_naver_search_channel
                                WHERE (
                                        "replace"(
                                            lower(
                                                (
                                                    COALESCE(
                                                        itg_kr_dads_naver_search_channel.keyword,
                                                        '[비검색광고영역]'::character varying
                                                    )
                                                )::text
                                            ),
                                            ' '::text,
                                            ''::text
                                        ) <> '(검색어 없음)'::text
                                    )
                                GROUP BY "replace"(
                                        lower(
                                            (
                                                COALESCE(
                                                    itg_kr_dads_naver_search_channel.keyword,
                                                    '[비검색광고영역]'::character varying
                                                )
                                            )::text
                                        ),
                                        ' '::text,
                                        ''::text
                                    ),
                                    (
                                        substring(
                                            (itg_kr_dads_naver_search_channel.file_date)::text,
                                            1,
                                            6
                                        ) || '01'::text
                                    )
                            ) b ON (
                                (
                                    (a.keyword = b.keyword)
                                    AND (a.date = b.date)
                                )
                            )
                        )
                        LEFT JOIN (
                            SELECT DISTINCT "replace"(
                                    lower(
                                        (
                                            COALESCE(
                                                itg_mds_kr_keyword_classifications.keyword,
                                                '[비검색광고영역]'::character varying
                                            )
                                        )::text
                                    ),
                                    ' '::text,
                                    ''::text
                                ) AS keyword2,
                                itg_mds_kr_keyword_classifications.keyword_group
                            FROM itg_mds_kr_keyword_classifications
                        ) e ON ((a.keyword = e.keyword2))
                    )
            ) derived_table10
    ) final
WHERE (
        (
            (substring((final.fisc_day)::text, 1, 4))::numeric(18, 0) >= (
                SELECT (
                        (
                            (
                                date_part(
                                    year,
                                    current_timestamp()::timestamp without time zone
                                )
                            )::numeric
                        )::numeric(18, 0) - (itg_query_parameters.parameter_value)::numeric(18, 0)
                    )
                FROM itg_query_parameters
                WHERE (
                        (
                            (itg_query_parameters.country_code)::text = ('KR'::character varying)::text
                        )
                        AND (
                            (itg_query_parameters.parameter_name)::text = ('KR_DADS'::character varying)::text
                        )
                    )
            )
        )
        AND (
            (
                "replace"(
                    substring((final.fisc_day)::text, 1, 7),
                    '-'::text,
                    ''::text
                )
            )::numeric(18, 0) >= (
                CASE
                    WHEN ((final.retailer)::text = 'COUPANG'::text) THEN '202207'::text
                    ELSE '202201'::text
                END
            )::numeric(18, 0)
        )
    )
)
select * from final