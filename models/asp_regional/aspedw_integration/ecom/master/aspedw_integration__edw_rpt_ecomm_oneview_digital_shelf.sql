with edw_clavis_gb_products as(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CLAVIS_GB_PRODUCTS
),
itg_mds_rg_ecom_digital_shelf_customer_mapping as(
    select * from snapaspitg_integration.itg_mds_rg_ecom_digital_shelf_customer_mapping
),
itg_ecom_digital_salesweight as(
    select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.ITG_ECOM_DIGITAL_SALESWEIGHT
),
edw_calendar_dim as(
    select * from snapaspedw_integration.edw_calendar_dim
),
edw_product_key_attributes as(
    select * from snapaspedw_integration.edw_product_key_attributes
),
itg_mds_ap_digital_shelf_targets as(
    select * from DEV_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_DIGITAL_SHELF_TARGETS
),
edw_clavis_gb_search_terms_results as(
    select * from aspedw_integration.edw_clavis_gb_search_terms_results
),
itg_mds_ap_ecom_oneview_config as(
    select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.ITG_MDS_AP_ECOM_ONEVIEW_CONFIG
),
union1 as(
    SELECT 'Act' AS "data_type",
        'Clavis' AS "datasource",
        'SKU' AS "data_level",
        'Portfolio Availability' AS "kpi",
        'Continuous' AS "period_type",
        fisc_yr AS fisc_year,
        cal_yr AS "cal_year",
        pstng_per AS "fisc_month",
        cal_mo_2 AS "cal_month",
        a.report_date AS "fisc_day",
        a.report_date AS "cal_day",
        fisc_per AS "fisc_yr_per",
        c.cluster,
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "market",
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "sub_market",
        c.channel,
        c.channel AS "sub_channel",
        c.re AS "retail_environment",
        NULL AS "go_to_model",
        NULL AS "profit_center",
        NULL AS "company_code",
        NULL AS "sap_customer_code",
        NULL AS "sap_customer_name",
        NULL AS "banner",
        NULL AS "banner_format",
        c.group_customer AS "platform_name",
        a.online_store AS "retailer_name",
        a.online_store AS "retailer_name_english",
        'Johnson & Johnson' AS "manufacturer_name",
        'Y' AS "jj_manufacturer_flag",
        "gcph_franchise" AS "prod_hier_l1",
        "gcph_needstate" AS "prod_hier_l2",
        category AS "prod_hier_l3",
        "gcph_subcategory" AS "prod_hier_l4",
        brand AS "prod_hier_l5",
        "gcph_subbrand" AS "prod_hier_l6",
        "gcph_variant" AS "prod_hier_l7",
        "put_up_desc" AS "prod_hier_l8",
        product_desc AS "prod_hier_l9",
        NULL AS "product_minor_code",
        NULL AS "prod_minor_name",
        "matl_num" AS "material_number",
        upc AS "ean",
        rpc AS "retailer_sku_code",
        "pka_rootcode" AS "product_key",
        "pka_rootcodedes" AS "product_key_description",
        f.value AS "target_value",
        CAST(avail_comp AS DECIMAL(4, 3)) AS "actual_value",
        NULL AS "value_usd",
        1 AS "denominator",
        CAST(sw.salesweight AS DECIMAL(4, 3)) AS salesweight,
        NULL AS "from_crncy",
        NULL AS "to_crncy",
        NULL AS "account_number",
        'Portfolio Availability' AS "account_name",
        NULL AS "account_description_l1",
        NULL AS "account_description_l2",
        'Availability Status' AS "account_description_l3",
        avail_status AS "additional_information",
        NULL AS "ppm_role"
    FROM edw_clavis_gb_products a
    -- Get content records
    LEFT JOIN itg_mds_rg_ecom_digital_shelf_customer_mapping c
        -- Get customer details
        ON a.region = c.cntry_cd
        AND a.online_store = c.online_store
        AND c.data_provider = 'Clavis'
    LEFT JOIN (
        SELECT DISTINCT market,
            eretailer,
            salesweight
        FROM itg_ecom_digital_salesweight
        ) sw ON --new
        upper(a.online_store) = upper(sw.eretailer)
        AND upper(CASE 
                WHEN a.brand = 'Dr. Ci:Labo'
                    AND c.market = 'Japan'
                    THEN 'Japan DCL'
                ELSE c.market
                END) = upper(sw.market) --new
    LEFT JOIN edw_calendar_dim e ON e.cal_day = a.report_date
    LEFT JOIN (
        SELECT ctry_nm,
            ean_upc,
            MAX(gcph_franchise) AS "gcph_franchise",
            MAX(gcph_needstate) AS "gcph_needstate",
            MAX(gcph_subcategory) AS "gcph_subcategory",
            MAX(gcph_subbrand) AS "gcph_subbrand",
            MAX(gcph_variant) AS "gcph_variant",
            MAX(put_up_desc) AS "put_up_desc",
            MAX(pka_rootcode) AS "pka_rootcode",
            MAX(pka_rootcodedes) AS "pka_rootcodedes",
            MAX(matl_num) AS "matl_num"
        FROM edw_product_key_attributes
        GROUP BY 1,
            2
        ) d
        -- Get GCPH
        ON LTRIM(a.upc, '0') = LTRIM(d.ean_upc, '0')
        AND c.market = d.ctry_nm
    LEFT JOIN itg_mds_ap_digital_shelf_targets f
        -- Assign targets
        ON c.market = f.market
        AND f.kpi = 'Portfolio Availability'
        AND a.report_date >= f.valid_from
        AND a.report_date <= f.valid_to
    WHERE is_competitor = 'False'
        AND UPPER(dimension8) NOT IN ('NO', 'UNKNOWN', 'UNCATEGORIZED')
        AND UPPER(avail_status) <> 'VOID'
        AND a.region IN (
            SELECT DISTINCT cntry_cd
            FROM itg_mds_rg_ecom_digital_shelf_customer_mapping
            )
        -- Limit to markets in the customer master
        AND EXTRACT(YEAR FROM a.report_date) > EXTRACT(YEAR FROM CURRENT_DATE) - 2

),
union2 as(
    SELECT 'Act' AS "data_type",
        'Clavis' AS "datasource",
        'SKU' AS "data_level",
        'Ratings' AS "kpi",
        'Continuous' AS "period_type",
        fisc_yr AS fisc_year,
        cal_yr AS "cal_year",
        pstng_per AS "fisc_month",
        cal_mo_2 AS "cal_month",
        a.report_date AS "fisc_day",
        a.report_date AS "cal_day",
        fisc_per AS "fisc_yr_per",
        c.cluster,
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "market",
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "sub_market",
        c.channel,
        c.channel AS "sub_channel",
        c.re AS "retail_environment",
        NULL AS "go_to_model",
        NULL AS "profit_center",
        NULL AS "company_code",
        NULL AS "sap_customer_code",
        NULL AS "sap_customer_name",
        NULL AS "banner",
        NULL AS "banner_format",
        c.group_customer AS "platform_name",
        a.online_store AS "retailer_name",
        a.online_store AS "retailer_name_english",
        'Johnson & Johnson' AS "manufacturer_name",
        'Y' AS "jj_manufacturer_flag",
        "gcph_franchise" AS "prod_hier_l1",
        "gcph_needstate" AS "prod_hier_l2",
        category AS "prod_hier_l3",
        "gcph_subcategory" AS "prod_hier_l4",
        brand AS "prod_hier_l5",
        "gcph_subbrand" AS "prod_hier_l6",
        "gcph_variant" AS "prod_hier_l7",
        "put_up_desc" AS "prod_hier_l8",
        product_desc AS "prod_hier_l9",
        NULL AS "product_minor_code",
        NULL AS "prod_minor_name",
        "matl_num" AS "material_number",
        upc AS "ean",
        rpc AS "retailer_sku_code",
        "pka_rootcode" AS "product_key",
        "pka_rootcodedes" AS "product_key_description",
        f.value AS "target_value",
        CAST(rating_comp AS DECIMAL(4, 3)) AS "actual_value",
        NULL AS "value_usd",
        1 AS "denominator",
        CAST(sw.salesweight AS DECIMAL(4, 3)) AS salesweight,
        NULL AS "from_crncy",
        NULL AS "to_crncy",
        NULL AS "account_number",
        'Ratings' AS "account_name",
        NULL AS "account_description_l1",
        NULL AS "account_description_l2",
        'Overall Rating' AS "account_description_l3",
        overall_rating::TEXT AS "additional_information",
        NULL AS "ppm_role"
    FROM edw_clavis_gb_products a
    -- Get content records
    LEFT JOIN itg_mds_rg_ecom_digital_shelf_customer_mapping c
        -- Get customer details
        ON a.region = c.cntry_cd
        AND a.online_store = c.online_store
        AND c.data_provider = 'Clavis'
    LEFT JOIN (
        SELECT DISTINCT market,
            eretailer,
            salesweight
        FROM itg_ecom_digital_salesweight
        ) sw ON --new
        upper(a.online_store) = upper(sw.eretailer)
        AND upper(CASE 
                WHEN a.brand = 'Dr. Ci:Labo'
                    AND c.market = 'Japan'
                    THEN 'Japan DCL'
                ELSE c.market
                END) = upper(sw.market) --new
    LEFT JOIN edw_calendar_dim e ON e.cal_day = a.report_date
    LEFT JOIN (
        SELECT ctry_nm,
            ean_upc,
            MAX(gcph_franchise) AS "gcph_franchise",
            MAX(gcph_needstate) AS "gcph_needstate",
            MAX(gcph_subcategory) AS "gcph_subcategory",
            MAX(gcph_subbrand) AS "gcph_subbrand",
            MAX(gcph_variant) AS "gcph_variant",
            MAX(put_up_desc) AS "put_up_desc",
            MAX(pka_rootcode) AS "pka_rootcode",
            MAX(pka_rootcodedes) AS "pka_rootcodedes",
            MAX(matl_num) AS "matl_num"
        FROM edw_product_key_attributes
        GROUP BY 1,
            2
        ) d
        -- Get GCPH
        ON LTRIM(a.upc, '0') = LTRIM(d.ean_upc, '0')
        AND c.market = d.ctry_nm
    LEFT JOIN itg_mds_ap_digital_shelf_targets f
        -- Assign targets
        ON c.market = f.market
        AND f.kpi = 'Ratings'
        AND a.report_date >= f.valid_from
        AND a.report_date <= f.valid_to
    WHERE is_competitor = 'False'
        AND UPPER(dimension8) NOT IN ('NO', 'UNKNOWN', 'UNCATEGORIZED')
        AND a.region IN (
            SELECT DISTINCT cntry_cd
            FROM itg_mds_rg_ecom_digital_shelf_customer_mapping
            )
        -- Limit to markets in the customer master
        AND EXTRACT(YEAR FROM a.report_date) > EXTRACT(YEAR FROM CURRENT_DATE) - 2
        AND UPPER(avail_status) <> 'VOID'
        AND overall_rating IS NOT NULL
),
union3 as(

    SELECT 'Act' AS "data_type",
        'Clavis' AS "datasource",
        'SKU' AS "data_level",
        'Reviews' AS "kpi",
        'Continuous' AS "period_type",
        fisc_yr AS fisc_year,
        cal_yr AS "cal_year",
        pstng_per AS "fisc_month",
        cal_mo_2 AS "cal_month",
        a.report_date AS "fisc_day",
        a.report_date AS "cal_day",
        fisc_per AS "fisc_yr_per",
        c.cluster,
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "market",
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "sub_market",
        c.channel,
        c.channel AS "sub_channel",
        c.re AS "retail_environment",
        NULL AS "go_to_model",
        NULL AS "profit_center",
        NULL AS "company_code",
        NULL AS "sap_customer_code",
        NULL AS "sap_customer_name",
        NULL AS "banner",
        NULL AS "banner_format",
        c.group_customer AS "platform_name",
        a.online_store AS "retailer_name",
        a.online_store AS "retailer_name_english",
        'Johnson & Johnson' AS "manufacturer_name",
        'Y' AS "jj_manufacturer_flag",
        "gcph_franchise" AS "prod_hier_l1",
        "gcph_needstate" AS "prod_hier_l2",
        category AS "prod_hier_l3",
        "gcph_subcategory" AS "prod_hier_l4",
        brand AS "prod_hier_l5",
        "gcph_subbrand" AS "prod_hier_l6",
        "gcph_variant" AS "prod_hier_l7",
        "put_up_desc" AS "prod_hier_l8",
        product_desc AS "prod_hier_l9",
        NULL AS "product_minor_code",
        NULL AS "prod_minor_name",
        "matl_num" AS "material_number",
        upc AS "ean",
        rpc AS "retailer_sku_code",
        "pka_rootcode" AS "product_key",
        "pka_rootcodedes" AS "product_key_description",
        f.value AS "target_value",
        CAST(review_comp AS DECIMAL(4, 3)) AS "actual_value",
        NULL AS "value_usd",
        1 AS "value_lcy",
        CAST(sw.salesweight AS DECIMAL(4, 3)) AS salesweight,
        NULL AS "from_crncy",
        NULL AS "to_crncy",
        NULL AS "account_number",
        'Reviews' AS "account_name",
        NULL AS "account_description_l1",
        NULL AS "account_description_l2",
        'Review Count' AS "account_description_l3",
        review_count::TEXT AS "additional_information",
        NULL AS "ppm_role"
    FROM edw_clavis_gb_products a
    -- Get content records
    LEFT JOIN itg_mds_rg_ecom_digital_shelf_customer_mapping c
        -- Get customer details
        ON a.region = c.cntry_cd
        AND a.online_store = c.online_store
        AND c.data_provider = 'Clavis'
    LEFT JOIN (
        SELECT DISTINCT market,
            eretailer,
            salesweight
        FROM itg_ecom_digital_salesweight
        ) sw ON --new
        upper(a.online_store) = upper(sw.eretailer)
        AND upper(CASE 
                WHEN a.brand = 'Dr. Ci:Labo'
                    AND c.market = 'Japan'
                    THEN 'Japan DCL'
                ELSE c.market
                END) = upper(sw.market) --new
    LEFT JOIN edw_calendar_dim e ON e.cal_day = a.report_date
    LEFT JOIN (
        SELECT ctry_nm,
            ean_upc,
            MAX(gcph_franchise) AS "gcph_franchise",
            MAX(gcph_needstate) AS "gcph_needstate",
            MAX(gcph_subcategory) AS "gcph_subcategory",
            MAX(gcph_subbrand) AS "gcph_subbrand",
            MAX(gcph_variant) AS "gcph_variant",
            MAX(put_up_desc) AS "put_up_desc",
            MAX(pka_rootcode) AS "pka_rootcode",
            MAX(pka_rootcodedes) AS "pka_rootcodedes",
            MAX(matl_num) AS "matl_num"
        FROM edw_product_key_attributes
        GROUP BY 1,
            2
        ) d
        -- Get GCPH
        ON LTRIM(a.upc, '0') = LTRIM(d.ean_upc, '0')
        AND c.market = d.ctry_nm
    LEFT JOIN itg_mds_ap_digital_shelf_targets f
        -- Assign targets
        ON c.market = f.market
        AND f.kpi = 'Reviews'
        AND a.report_date >= f.valid_from
        AND a.report_date <= f.valid_to
    WHERE is_competitor = 'False'
        AND UPPER(dimension8) NOT IN ('NO', 'UNKNOWN', 'UNCATEGORIZED')
        AND a.region IN (
            SELECT DISTINCT cntry_cd
            FROM itg_mds_rg_ecom_digital_shelf_customer_mapping
            )
        -- Limit to markets in the customer master
        AND EXTRACT(YEAR FROM a.report_date) > EXTRACT(YEAR FROM CURRENT_DATE) - 2
        AND UPPER(avail_status) <> 'VOID'
        AND overall_rating IS NOT NULL
        AND review_count >= 0
),
union4 as(
    -- Content. Take latest date in the month & apply to the whole month.
    SELECT 'Act' AS "data_type",
        'Clavis' AS "datasource",
        'SKU' AS "data_level",
        'Content Integrity' AS "kpi",
        'Continuous' AS "period_type",
        fisc_yr AS fisc_year,
        cal_yr AS "cal_year",
        pstng_per AS "fisc_month",
        cal_mo_2 AS "cal_month",
        a.report_date AS "fisc_day",
        a.report_date AS "cal_day",
        fisc_per AS "fisc_yr_per",
        c.cluster,
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "market",
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "sub_market",
        c.channel,
        c.channel AS "sub_channel",
        c.re AS "retail_environment",
        NULL AS "go_to_model",
        NULL AS "profit_center",
        NULL AS "company_code",
        NULL AS "sap_customer_code",
        NULL AS "sap_customer_name",
        NULL AS "banner",
        NULL AS "banner_format",
        c.group_customer AS "platform_name",
        a.online_store AS "retailer_name",
        a.online_store AS "retailer_name_english",
        'Johnson & Johnson' AS "manufacturer_name",
        'Y' AS "jj_manufacturer_flag",
        "gcph_franchise" AS "prod_hier_l1",
        "gcph_needstate" AS "prod_hier_l2",
        category AS"prod_hier_l3",
        "gcph_subcategory" AS "prod_hier_l4",
        brand AS"prod_hier_l5",
        "gcph_subbrand" AS "prod_hier_l6",
        "gcph_variant" AS "prod_hier_l7",
        "put_up_desc" AS "prod_hier_l8",
        product_desc AS"prod_hier_l9",
        NULL AS"product_minor_code",
        NULL AS"prod_minor_name",
        "matl_num" AS "material_number",
        upc AS"ean",
        rpc AS"retailer_sku_code",
        "pka_rootcode" AS "product_key",
        "pka_rootcodedes" AS "product_key_description",
        f.value AS "target_value",
        CAST(milestone5_comp AS DECIMAL(4, 3)) AS "actual_value",
        NULL AS "value_usd",
        1 AS "value_lcy",
        CAST(sw.salesweight AS DECIMAL(4, 3)) AS salesweight,
        NULL AS "from_crncy",
        NULL AS "to_crncy",
        NULL AS "account_number",
        'Core Global' AS "account_name",
        NULL AS "account_description_l1",
        NULL AS "account_description_l2",
        'Image URL' AS "account_description_l3",
        product_image_url AS "additional_information",
        NULL AS "ppm_role"
    FROM edw_clavis_gb_products a
    -- Get content records
    JOIN (
        SELECT region,
            MAX(report_date) AS "report_date",
            EXTRACT(MONTH FROM report_date) AS "month",
            EXTRACT(YEAR FROM report_date) AS "year"
        FROM edw_clavis_gb_products
        WHERE content_comp IS NOT NULL
        GROUP BY 1,
            3,
            4
        ) b
        -- Get Latest Report Date by Month Year
        ON a.region = b.region
        AND a.report_date = b."report_date"
    LEFT JOIN itg_mds_rg_ecom_digital_shelf_customer_mapping c
        -- Get customer details
        ON a.region = c.cntry_cd
        AND a.online_store = c.online_store
        AND c.data_provider = 'Clavis'
    LEFT JOIN (
        SELECT DISTINCT market,
            eretailer,
            salesweight
        FROM itg_ecom_digital_salesweight
        ) sw ON --new
        upper(a.online_store) = upper(sw.eretailer)
        AND upper(CASE 
                WHEN a.brand = 'Dr. Ci:Labo'
                    AND c.market = 'Japan'
                    THEN 'Japan DCL'
                ELSE c.market
                END) = upper(sw.market) --new
    LEFT JOIN edw_calendar_dim e ON e.cal_day = a.report_date
    LEFT JOIN (
        SELECT ctry_nm,
            ean_upc,
            MAX(gcph_franchise) AS "gcph_franchise",
            MAX(gcph_needstate) AS "gcph_needstate",
            MAX(gcph_subcategory) AS "gcph_subcategory",
            MAX(gcph_subbrand) AS "gcph_subbrand",
            MAX(gcph_variant) AS "gcph_variant",
            MAX(put_up_desc) AS "put_up_desc",
            MAX(pka_rootcode) AS "pka_rootcode",
            MAX(pka_rootcodedes) AS "pka_rootcodedes",
            MAX(matl_num) AS "matl_num"
        FROM edw_product_key_attributes
        GROUP BY 1,
            2
        ) d
        -- Get GCPH
        ON LTRIM(a.upc, '0') = LTRIM(d.ean_upc, '0')
        AND c.market = d.ctry_nm
    LEFT JOIN itg_mds_ap_digital_shelf_targets f
        -- Assign targets
        ON c.market = f.market
        AND f.kpi = 'Content'
        AND a.report_date >= f.valid_from
        AND a.report_date <= f.valid_to
    WHERE is_competitor = 'False'
        AND UPPER(dimension8) NOT IN ('NO', 'UNKNOWN', 'UNCATEGORIZED')
        AND milestone5_comp IS NOT NULL
        AND a.region IN (
            SELECT DISTINCT cntry_cd
            FROM itg_mds_rg_ecom_digital_shelf_customer_mapping
            )
        -- Limit to markets in the customer master
        AND EXTRACT(YEAR FROM a.report_date) > EXTRACT(YEAR FROM CURRENT_DATE) - 2
),
union5 as(
    SELECT 'Act' AS "data_type",
        'Clavis' AS "datasource",
        'SKU' AS "data_level",
        'Price' AS "kpi",
        'Continuous' AS "period_type",
        fisc_yr AS fisc_year,
        cal_yr AS "cal_year",
        pstng_per AS "fisc_month",
        cal_mo_2 AS "cal_month",
        a.report_date AS "fisc_day",
        a.report_date AS "cal_day",
        fisc_per AS "fisc_yr_per",
        c.cluster,
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "market",
        CASE 
            WHEN a.brand = 'Dr. Ci:Labo'
                AND c.market = 'Japan'
                THEN 'Japan DCL'
            WHEN c.market = 'Australia'
                THEN 'Pacific'
            ELSE c.market
            END AS "sub_market",
        c.channel,
        c.channel AS "sub_channel",
        c.re AS "retail_environment",
        NULL AS "go_to_model",
        NULL AS "profit_center",
        NULL AS "company_code",
        NULL AS "sap_customer_code",
        NULL AS "sap_customer_name",
        NULL AS "banner",
        NULL AS "banner_format",
        c.group_customer AS "platform_name",
        a.online_store AS "retailer_name",
        a.online_store AS "retailer_name_english",
        'Johnson & Johnson' AS "manufacturer_name",
        'Y' AS "jj_manufacturer_flag",
        "gcph_franchise" AS "prod_hier_l1",
        "gcph_needstate" AS "prod_hier_l2",
        category AS"prod_hier_l3",
        "gcph_subcategory" AS "prod_hier_l4",
        brand AS"prod_hier_l5",
        "gcph_subbrand" AS "prod_hier_l6",
        "gcph_variant" AS "prod_hier_l7",
        "put_up_desc" AS "prod_hier_l8",
        product_desc AS"prod_hier_l9",
        NULL AS"product_minor_code",
        NULL AS"prod_minor_name",
        "matl_num" AS "material_number",
        upc AS"ean",
        rpc AS"retailer_sku_code",
        "pka_rootcode" AS "product_key",
        "pka_rootcodedes" AS "product_key_description",
        f.value AS "target_value",
        price_comp AS "actual_value",
        NULL AS "value_usd",
        1 AS "denominator",
        CAST(sw.salesweight AS DECIMAL(4, 3)) AS salesweight,
        NULL AS "from_crncy",
        NULL AS "to_crncy",
        NULL AS "account_number",
        'Min Max Price' AS "account_name",
        NULL AS "account_description_l1",
        NULL AS "account_description_l2",
        'Observed Price' AS "account_description_l3",
        observed_price::TEXT AS "additional_information",
        NULL AS "ppm_role"
    FROM edw_clavis_gb_products a
    -- Get content records
    LEFT JOIN itg_mds_rg_ecom_digital_shelf_customer_mapping c
        -- Get customer details
        ON a.region = c.cntry_cd
        AND a.online_store = c.online_store
        AND c.data_provider = 'Clavis'
    LEFT JOIN (
        SELECT DISTINCT market,
            eretailer,
            salesweight
        FROM itg_ecom_digital_salesweight
        ) sw ON --new
        upper(a.online_store) = upper(sw.eretailer)
        AND upper(CASE 
                WHEN a.brand = 'Dr. Ci:Labo'
                    AND c.market = 'Japan'
                    THEN 'Japan DCL'
                ELSE c.market
                END) = upper(sw.market) --new
    LEFT JOIN edw_calendar_dim e ON e.cal_day = a.report_date
    LEFT JOIN (
        SELECT ctry_nm,
            ean_upc,
            MAX(gcph_franchise) AS "gcph_franchise",
            MAX(gcph_needstate) AS "gcph_needstate",
            MAX(gcph_subcategory) AS "gcph_subcategory",
            MAX(gcph_subbrand) AS "gcph_subbrand",
            MAX(gcph_variant) AS "gcph_variant",
            MAX(put_up_desc) AS "put_up_desc",
            MAX(pka_rootcode) AS "pka_rootcode",
            MAX(pka_rootcodedes) AS "pka_rootcodedes",
            MAX(matl_num) AS "matl_num"
        FROM edw_product_key_attributes
        GROUP BY 1,
            2
        ) d
        -- Get GCPH
        ON LTRIM(a.upc, '0') = LTRIM(d.ean_upc, '0')
        AND c.market = d.ctry_nm
    LEFT JOIN itg_mds_ap_digital_shelf_targets f
        -- Assign targets
        ON c.market = f.market
        AND f.kpi = 'Price'
        AND a.report_date >= f.valid_from
        AND a.report_date <= f.valid_to
    WHERE is_competitor = 'False'
        AND UPPER(dimension8) NOT IN ('NO', 'UNKNOWN', 'UNCATEGORIZED')
        AND a.region IN (
            SELECT DISTINCT cntry_cd
            FROM itg_mds_rg_ecom_digital_shelf_customer_mapping
            )
        -- Limit to markets in the customer master
        AND EXTRACT(YEAR FROM a.report_date) > EXTRACT(YEAR FROM CURRENT_DATE) - 2
        AND avail_comp = 1
        AND price_comp IS NOT NULL
),
union6 as(
  SELECT 'Act' AS "data_type",
	'Clavis' AS "datasource",
	'Search Term' AS "data_level",
	'Share of Search' AS "kpi",
	'Continuous' AS "period_type",
	fisc_yr AS fisc_year,
	cal_yr AS "cal_year",
	pstng_per AS "fisc_month",
	cal_mo_2 AS "cal_month",
	a.report_date AS "fisc_day",
	a.report_date AS "cal_day",
	fisc_per AS "fisc_yr_per",
	a.cluster,
	CASE 
		WHEN a."market" = 'Australia'
			THEN 'Pacific'
		ELSE a."market"
		END AS "market",
	CASE 
		WHEN a."market" = 'Australia'
			THEN 'Pacific'
		ELSE a."market"
		END AS "sub_market",
	a.channel,
	a.channel AS "sub_channel",
	a.re AS "retail_environment",
	NULL AS "go_to_model",
	NULL AS "profit_center",
	NULL AS "company_code",
	NULL AS "sap_customer_code",
	NULL AS "sap_customer_name",
	NULL AS "banner",
	NULL AS "banner_format",
	a."platform" AS "platform_name",
	a.online_store AS "retailer_name",
	a.online_store AS "retailer_name_english",
	'Johnson & Johnson' AS "manufacturer_name",
	'Y' AS "jj_manufacturer_flag",
	NULL AS "prod_hier_l1",
	NULL AS "prod_hier_l2",
	NULL AS "prod_hier_l3",
	NULL AS "prod_hier_l4",
	NULL AS "prod_hier_l5",
	NULL AS "prod_hier_l6",
	NULL AS "prod_hier_l7",
	NULL AS "prod_hier_l8",
	NULL AS "prod_hier_l9",
	NULL AS "product_minor_code",
	NULL AS "prod_minor_name",
	NULL AS "material_number",
	NULL AS "ean",
	NULL AS "retailer_sku_code",
	NULL AS "product_key",
	NULL AS "product_key_description",
	"target" AS "target_value",
	"final_paid_score" AS "actual_value",
	NULL AS "value_usd",
	NULL AS "value_lcy",
	salesweight AS salesweight,
	NULL AS "from_crncy",
	NULL AS "to_crncy",
	NULL AS "account_number",
	'Share of Search' AS "account_name",
	NULL AS "account_description_l1",
	NULL AS "account_description_l2",
	'Share of Search' AS "account_description_l3",
	search_term AS "additional_information",
	NULL AS "ppm_role" FROM (
	SELECT cluster,
		"market",
		channel,
		re,
		"platform",
		online_store,
		search_term,
		report_date,
		AVG("target") AS "target",
		SUM("final_paid_weighted_score") AS "final_paid_score",
		max(salesweight) AS salesweight
	FROM (
		SELECT cust.cluster,
			CASE 
				WHEN UPPER(trans.CATEGORY) = 'FACE CARE'
					AND cust.market = 'Japan'
					THEN 'Japan DCL'
				ELSE cust.market
				END AS "market",
			channel,
			re,
			region,
			group_customer AS "platform",
			trans.online_store,
			report_date,
			search_results_segment,
			search_results_segment_value,
			trans.category,
			dimension1 AS "sub_category",
			manufacturer,
			brand,
			search_term,
			is_priority_search_term,
			is_competitor,
			trusted_upc,
			trusted_rpc,
			harvested_product_description,
			dimension8 AS "msl",
			results_per_page,
			search_results_rank,
			search_results_weighted_score,
			CASE 
				WHEN (
						search_results_segment IS NULL
						OR search_results_segment = ''
						)
					AND is_competitor = 'False'
					THEN search_results_weighted_score
				WHEN UPPER(search_results_segment) = 'CATEGORY'
					AND UPPER(search_results_segment_value) = UPPER(trans.category)
					AND is_competitor = 'False'
					THEN search_results_weighted_score
				WHEN UPPER(search_results_segment) = 'DIMENSION1'
					AND UPPER(search_results_segment_value) = UPPER(dimension1)
					AND is_competitor = 'False'
					THEN search_results_weighted_score
				WHEN UPPER(search_results_segment) = 'BRAND'
					AND UPPER(search_results_segment_value) = UPPER(brand)
					AND is_competitor = 'False'
					THEN search_results_weighted_score
				WHEN UPPER(search_results_segment) = 'DIMENSION3'
					AND UPPER(search_results_segment_value) = UPPER(dimension3)
					AND is_competitor = 'False'
					THEN search_results_weighted_score
				ELSE 0
				END AS "final_paid_weighted_score",
			search_results_is_paid,
			"target",
			salesweight
		FROM edw_clavis_gb_search_terms_results trans
		LEFT JOIN itg_mds_rg_ecom_digital_shelf_customer_mapping cust ON trans.region = cust.cntry_cd
			AND trans.online_store = cust.online_store
			AND cust.data_provider = 'Clavis'
		LEFT JOIN (
			SELECT DISTINCT market,
				eretailer,
				salesweight
			FROM itg_ecom_digital_salesweight
			) sw ON --new
			upper(trans.online_store) = upper(sw.eretailer)
			AND upper(CASE 
					WHEN UPPER(trans.category) = 'FACE CARE'
						AND cust.market = 'Japan'
						THEN 'Japan DCL'
					ELSE cust.market
					END) = upper(sw.market) --new
		LEFT JOIN (
			SELECT market,
				kpi,
				attribute_1 AS "category",
				to_date(valid_from) AS "valid_from",
				to_date(valid_to) AS "valid_to",
				MAX(value) AS "target"
			FROM itg_mds_ap_digital_shelf_targets
			WHERE kpi = 'Share of Search'
			GROUP BY 1, 2, 3, 4, 5
			) targets ON trans.category = targets."category"
			AND targets.market = cust.market
			AND report_date >= "valid_from"
			AND report_date <= "valid_to"
		WHERE is_priority_search_term = 'True'
			AND region IN (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset = 'Clavis'
					AND dataset_area = 'Share of Search'
					AND column_name = 'region'
				GROUP BY 1
				)
		) b
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
	) a LEFT JOIN edw_calendar_dim e ON e.cal_day = a.report_date WHERE EXTRACT(YEAR FROM a.report_date) > EXTRACT(YEAR FROM CURRENT_DATE) - 2
),
union7 as(
	SELECT 'Act' AS "data_type",
		'Clavis' AS "datasource",
		'Search Term' AS "data_level",
		'Share of Search' AS "kpi",
		'Continuous' AS "period_type",
		fisc_yr AS fisc_year,
		cal_yr AS "cal_year",
		pstng_per AS "fisc_month",
		cal_mo_2 AS "cal_month",
		a.report_date AS "fisc_day",
		a.report_date AS "cal_day",
		fisc_per AS "fisc_yr_per",
		a.cluster,
		CASE 
			WHEN a."market" = 'Australia'
				THEN 'Pacific'
			ELSE a."market"
			END AS "market",
		CASE 
			WHEN a."market" = 'Australia'
				THEN 'Pacific'
			ELSE a."market"
			END AS "sub_market",
		a.channel,
		a.channel AS "sub_channel",
		a.re AS "retail_environment",
		NULL AS "go_to_model",
		NULL AS "profit_center",
		NULL AS "company_code",
		NULL AS "sap_customer_code",
		NULL AS "sap_customer_name",
		NULL AS "banner",
		NULL AS "banner_format",
		a."platform" AS "platform_name",
		a.online_store AS "retailer_name",
		a.online_store AS "retailer_name_english",
		'Johnson & Johnson' AS "manufacturer_name",
		'Y' AS "jj_manufacturer_flag",
		NULL AS "prod_hier_l1",
		NULL AS "prod_hier_l2",
		NULL AS "prod_hier_l3",
		NULL AS "prod_hier_l4",
		NULL AS "prod_hier_l5",
		NULL AS "prod_hier_l6",
		NULL AS "prod_hier_l7",
		NULL AS "prod_hier_l8",
		NULL AS "prod_hier_l9",
		NULL AS "product_minor_code",
		NULL AS "prod_minor_name",
		NULL AS "material_number",
		NULL AS "ean",
		NULL AS "retailer_sku_code",
		NULL AS "product_key",
		NULL AS "product_key_description",
		"target" AS "target_value",
		"final_paid_score" AS "actual_value",
		NULL AS "value_usd",
		NULL AS "value_lcy", -- MISSING
		"salesweight",
		NULL AS "from_crncy",
		NULL AS "to_crncy",
		NULL AS "account_number",
		'Share of Search' AS "account_name",
		NULL AS "account_description_l1",
		NULL AS "account_description_l2",
		'Share of Search' AS "account_description_l3",
		search_term AS "additional_information",
		NULL AS "ppm_role"
	FROM (
		SELECT cluster,
			"market",
			channel,
			re,
			"platform",
			online_store,
			search_term,
			report_date,
			AVG("target") AS "target",
			SUM("final_paid_weighted_score") AS "final_paid_score",
			max(salesweight) AS "salesweight"
		FROM (
			SELECT cust.cluster,
				CASE 
					WHEN UPPER(trans.category) = 'FACE CARE'
						AND cust.market = 'Japan'
						THEN 'Japan DCL'
					ELSE cust.market
					END AS "market",
				channel,
				re,
				region,
				group_customer AS "platform",
				trans.online_store,
				report_date,
				search_results_segment,
				search_results_segment_value,
				trans.category,
				dimension1 AS "sub_category",
				manufacturer,
				brand,
				search_term,
				is_priority_search_term,
				is_competitor,
				trusted_upc,
				trusted_rpc,
				harvested_product_description,
				dimension8 AS "msl",
				results_per_page,
				search_results_rank,
				search_results_weighted_score,
				CASE 
					WHEN (
							search_results_segment IS NULL
							OR search_results_segment = ''
							)
						AND is_competitor = 'False'
						THEN search_results_weighted_score
					WHEN UPPER(search_results_segment) = 'CATEGORY'
						AND UPPER(search_results_segment_value) = UPPER(trans.category)
						AND is_competitor = 'False'
						THEN search_results_weighted_score
					WHEN UPPER(search_results_segment) = 'DIMENSION1'
						AND UPPER(search_results_segment_value) = UPPER(dimension1)
						AND is_competitor = 'False'
						THEN search_results_weighted_score
					WHEN UPPER(search_results_segment) = 'BRAND'
						AND is_competitor = 'False'
						THEN search_results_weighted_score
					ELSE 0
					END AS "final_paid_weighted_score",
				search_results_is_paid,
				"target",
				salesweight
			FROM edw_clavis_gb_search_terms_results trans
			LEFT JOIN itg_mds_rg_ecom_digital_shelf_customer_mapping cust ON trans.region = cust.cntry_cd
				AND trans.online_store = cust.online_store
				AND cust.data_provider = 'Clavis'
			LEFT JOIN (
				SELECT DISTINCT market,
					eretailer,
					salesweight
				FROM itg_ecom_digital_salesweight
				) sw ON --new
				upper(trans.online_store) = upper(sw.eretailer)
				AND upper(cust.market) = upper(sw.market) --new
			LEFT JOIN (
				SELECT market,
					kpi,
					attribute_1 AS "category",
					to_date(valid_from) AS "valid_from",
					to_date(valid_to) AS "valid_to",
					MAX(value) AS "target"
				FROM itg_mds_ap_digital_shelf_targets
				WHERE kpi = 'Share of Search'
				GROUP BY 1,
					2,
					3,
					4,
					5
				) targets ON trans.category = targets."category"
				AND targets.market = cust.market
				AND report_date >= "valid_from"
				AND report_date <= "valid_to"
			WHERE is_priority_search_term = 'True'
				AND region = 'AU'
			) b
		GROUP BY 1,
			2,
			3,
			4,
			5,
			6,
			7,
			8
		) a
	LEFT JOIN edw_calendar_dim e ON e.cal_day = a.report_date
	WHERE EXTRACT(YEAR FROM a.report_date) > EXTRACT(YEAR FROM CURRENT_DATE) - 2
),
final as(
    select * from union1
    union all
    select * from union2
    union all
    select * from union3
    union all
    select * from union4
    union all
    select * from union5
    union all
    select * from union6
    union all
    select * from union7
)
select * from final