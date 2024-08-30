{{
    config(
        materialized='view'
    )
}}

with itg_lookup_category_mapping as
(
    select * from {{ ref('aspitg_integration__itg_lookup_category_mapping') }}
),
edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_clavis_gb_products as
(
    select * from {{ ref('aspedw_integration__edw_clavis_gb_products') }}
),
itg_mds_rg_ecom_digital_shelf_customer_mapping as
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_ecom_digital_shelf_customer_mapping') }}
),
itg_mds_rg_ecom_product as
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_ecom_product') }}
),
edw_product_key_attributes as
(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
itg_mds_rg_sku_benchmarks as
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_sku_benchmarks') }}
),
edw_vw_greenlight_skus as
(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_crncy_exch_rates as
(
    select * from {{ ref('aspedw_integration__edw_crncy_exch_rates') }}
),
itg_query_parameters as
(
    select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
edw_market_mirror_fact as
(
    select * from {{ ref('aspedw_integration__edw_market_mirror_fact') }}
),
itg_mds_ap_sales_ops_map as
(
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
),
edw_cn_digital_shelf_data as
(
    select * from {{ ref('aspedw_integration__edw_cn_digital_shelf_data') }}
),
ecgp as 
(
    SELECT DISTINCT EDW_CLAVIS_GB_PRODUCTS.region, 
                    edw_clavis_gb_products.manufacturer, 
                    edw_clavis_gb_products.is_competitor, 
                    edw_clavis_gb_products.online_store, 
                    edw_clavis_gb_products.dimension7, 
                    edw_clavis_gb_products.report_date, 
                    edw_clavis_gb_products.category, 
                    edw_clavis_gb_products.dimension1, 
                    edw_clavis_gb_products.brand, 
                    edw_clavis_gb_products.product_desc, 
                    edw_clavis_gb_products.upc, 
                    edw_clavis_gb_products.rpc, 
                    edw_clavis_gb_products.dimension8, 
                    edw_clavis_gb_products.msrp, 
                    edw_clavis_gb_products.is_on_promotion, 
                    edw_clavis_gb_products.observed_price, 
                    edw_clavis_gb_products.promo_text, 
                    edw_clavis_gb_products.avail_status, 
                    edw_clavis_gb_products.avail_comp 
    FROM edw_clavis_gb_products WHERE ((((((EDW_CLAVIS_GB_PRODUCTS.region):: text LIKE ANY ('AU','CN','ID','IN','JP','PH','TH','VN','MY','SG','KR','TW','HK')) 
    AND (edw_clavis_gb_products.avail_comp = 1)) 
    AND (edw_clavis_gb_products.observed_price IS NOT NULL)) 
    AND (upper((edw_clavis_gb_products.category):: text) <> ('BUNDLE' :: character varying):: text)) 
    AND ((edw_clavis_gb_products.category IS NOT NULL) 
    OR ((edw_clavis_gb_products.category):: text <> ('' :: character varying):: text)))
),
rgcust as 
(
    SELECT DISTINCT itg_mds_rg_ecom_digital_shelf_customer_mapping.cntry_cd, 
                    itg_mds_rg_ecom_digital_shelf_customer_mapping.market, 
                    itg_mds_rg_ecom_digital_shelf_customer_mapping.channel, 
                    itg_mds_rg_ecom_digital_shelf_customer_mapping.re, 
                    itg_mds_rg_ecom_digital_shelf_customer_mapping.group_customer, 
                    itg_mds_rg_ecom_digital_shelf_customer_mapping.online_store, 
                    itg_mds_rg_ecom_digital_shelf_customer_mapping.data_provider, 
                    ITG_MDS_RG_ECOM_DIGITAL_SHELF_CUSTOMER_MAPPING.cluster, 
                    itg_mds_rg_ecom_digital_shelf_customer_mapping.ctrd_dttm 
    FROM itg_mds_rg_ecom_digital_shelf_customer_mapping
),
rgp as
(
    SELECT DISTINCT itg_mds_rg_ecom_product.market, 
                    itg_mds_rg_ecom_product.manufacturer, 
                    itg_mds_rg_ecom_product.account_name, 
                    itg_mds_rg_ecom_product.rpc, 
                    max(itg_mds_rg_ecom_product.ean) AS ean 
    FROM itg_mds_rg_ecom_product 
        GROUP BY itg_mds_rg_ecom_product.market, 
                itg_mds_rg_ecom_product.manufacturer, 
                itg_mds_rg_ecom_product.account_name, 
                itg_mds_rg_ecom_product.rpc
),
pka as
(
    SELECT derived_table1.ctry_nm, 
        derived_table1.ean_upc, 
        derived_table1.gcph_franchise, 
        derived_table1.pka_franchise_description, 
        derived_table1.gcph_category, 
        derived_table1.gcph_subcategory, 
        derived_table1.pka_brand_description, 
        derived_table1.pka_subbranddesc, 
        derived_table1.pka_variantdesc, 
        derived_table1.pka_subvariantdesc, 
        derived_table1.pka_package, 
        derived_table1.pka_rootcode, 
        derived_table1.pka_productdesc, 
        derived_table1.pka_sizedesc, 
        derived_table1.pka_skuiddesc, 
        row_number() OVER(PARTITION BY derived_table1.ctry_nm,derived_table1.ean_upc ORDER BY derived_table1.pka_rootcode DESC) AS row_number 
    FROM (SELECT CASE WHEN ((a.ctry_nm):: text = ('APSC Regional' :: character varying):: text) THEN 'China Personal Care' :: character varying ELSE a.ctry_nm END AS ctry_nm, 
                (a.ean_upc):: character varying AS ean_upc, 
                a.gcph_franchise, 
                a.pka_franchise_description, 
                a.gcph_category, 
                a.gcph_subcategory, 
                a.pka_brand_description, 
                a.pka_subbranddesc, 
                a.pka_variantdesc, 
                a.pka_subvariantdesc, 
                a.pka_package, 
                a.pka_rootcode, 
                a.pka_productdesc, 
                a.pka_sizedesc, 
                a.pka_skuiddesc 
        FROM ((SELECT edw_product_key_attributes.ctry_nm, 
                        edw_product_key_attributes.gcph_franchise, 
                        edw_product_key_attributes.pka_franchise_description, 
                        edw_product_key_attributes.gcph_category, 
                        edw_product_key_attributes.gcph_subcategory, 
                        edw_product_key_attributes.pka_brand_description, 
                        edw_product_key_attributes.pka_subbranddesc, 
                        edw_product_key_attributes.pka_variantdesc, 
                        edw_product_key_attributes.pka_subvariantdesc, 
                        edw_product_key_attributes.pka_package, 
                        edw_product_key_attributes.pka_rootcode, 
                        edw_product_key_attributes.pka_productdesc, 
                        edw_product_key_attributes.pka_sizedesc, 
                        edw_product_key_attributes.pka_skuiddesc, 
                        ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text) AS ean_upc, 
                        edw_product_key_attributes.lst_nts AS nts_date 
                FROM edw_product_key_attributes 
                WHERE (((((edw_product_key_attributes.matl_type_cd):: text = ('FERT' :: character varying):: text) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('HALB' :: character varying):: text)) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('SAPR' :: character varying):: text))
                AND (edw_product_key_attributes.lst_nts IS NOT NULL)) 
                GROUP BY edw_product_key_attributes.ctry_nm, 
                            edw_product_key_attributes.gcph_franchise, 
                            edw_product_key_attributes.pka_franchise_description, 
                            edw_product_key_attributes.gcph_category, 
                            edw_product_key_attributes.gcph_subcategory, 
                            edw_product_key_attributes.pka_brand_description, 
                            edw_product_key_attributes.pka_subbranddesc, 
                            edw_product_key_attributes.pka_variantdesc, 
                            edw_product_key_attributes.pka_subvariantdesc, 
                            edw_product_key_attributes.pka_package, 
                            edw_product_key_attributes.pka_rootcode, 
                            edw_product_key_attributes.pka_productdesc, 
                            edw_product_key_attributes.pka_sizedesc, 
                            edw_product_key_attributes.pka_skuiddesc, 
                            ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text),edw_product_key_attributes.lst_nts) a 
        JOIN (SELECT edw_product_key_attributes.ctry_nm, 
                    ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text) AS ean_upc, 
                    edw_product_key_attributes.pka_rootcode, 
                    edw_product_key_attributes.lst_nts AS latest_nts_date, 
                    row_number() OVER(PARTITION BY edw_product_key_attributes.ctry_nm,edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.lst_nts DESC) AS row_number 
                FROM edw_product_key_attributes 
                WHERE (((((edw_product_key_attributes.matl_type_cd):: text = ('FERT' :: character varying):: text)
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('HALB' :: character varying):: text))
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('SAPR' :: character varying):: text))
                AND (edw_product_key_attributes.lst_nts IS NOT NULL)) 
                GROUP BY edw_product_key_attributes.ctry_nm, 
                        edw_product_key_attributes.ean_upc, 
                        edw_product_key_attributes.pka_rootcode, 
                        edw_product_key_attributes.lst_nts
            ) b ON (((((((a.ctry_nm):: text = (b.ctry_nm):: text) 
                AND (a.ean_upc = b.ean_upc)) 
                AND ((a.pka_rootcode):: text = (b.pka_rootcode):: text)) 
                AND (b.latest_nts_date = a.nts_date))
                AND (b.row_number = 1)))) 
    GROUP BY CASE WHEN ((a.ctry_nm):: text = ('APSC Regional' :: character varying):: text) THEN 'China Personal Care' :: character varying ELSE a.ctry_nm END, 
            (a.ean_upc):: character varying, 
            a.gcph_franchise, 
            a.pka_franchise_description, 
            a.gcph_category, 
            a.gcph_subcategory, 
            a.pka_brand_description, 
            a.pka_subbranddesc, 
            a.pka_variantdesc, 
            a.pka_subvariantdesc, 
            a.pka_package, 
            a.pka_rootcode, 
            a.pka_productdesc, 
            a.pka_sizedesc, 
            a.pka_skuiddesc 

    UNION ALL 

    SELECT 'Japan DCL' :: character varying AS ctry_nm, 
            (a.ean_upc):: character varying AS ean_upc, 
            a.gcph_franchise, 
            a.pka_franchise_description, 
            a.gcph_category, 
            a.gcph_subcategory, 
            a.pka_brand_description, 
            a.pka_subbranddesc, 
            a.pka_variantdesc, 
            a.pka_subvariantdesc, 
            a.pka_package, 
            a.pka_rootcode, 
            a.pka_productdesc, 
            a.pka_sizedesc, 
            a.pka_skuiddesc 
    FROM ((SELECT 'Japan DCL' :: character varying AS ctry_nm, 
                edw_product_key_attributes.gcph_franchise, 
                edw_product_key_attributes.pka_franchise_description, 
                edw_product_key_attributes.gcph_category, 
                edw_product_key_attributes.gcph_subcategory, 
                edw_product_key_attributes.pka_brand_description, 
                edw_product_key_attributes.pka_subbranddesc, 
                edw_product_key_attributes.pka_variantdesc, 
                edw_product_key_attributes.pka_subvariantdesc, 
                edw_product_key_attributes.pka_package, 
                edw_product_key_attributes.pka_rootcode, 
                edw_product_key_attributes.pka_productdesc, 
                edw_product_key_attributes.pka_sizedesc, 
                edw_product_key_attributes.pka_skuiddesc, 
                ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text) AS ean_upc, 
                edw_product_key_attributes.lst_nts AS nts_date 
            FROM edw_product_key_attributes 
            WHERE ((((((edw_product_key_attributes.matl_type_cd):: text = ('FERT' :: character varying):: text)
            OR ((edw_product_key_attributes.matl_type_cd):: text = ('HALB' :: character varying):: text))
            OR ((edw_product_key_attributes.matl_type_cd):: text = ('SAPR' :: character varying):: text)) 
            AND (edw_product_key_attributes.lst_nts IS NOT NULL)) 
            AND (upper((edw_product_key_attributes.gcph_brand):: text) = ('DR. CI: LABO' :: character varying):: text))
            GROUP BY 1, 
                    edw_product_key_attributes.gcph_franchise, 
                    edw_product_key_attributes.pka_franchise_description, 
                    edw_product_key_attributes.gcph_category, 
                    edw_product_key_attributes.gcph_subcategory, 
                    edw_product_key_attributes.pka_brand_description, 
                    edw_product_key_attributes.pka_subbranddesc, 
                    edw_product_key_attributes.pka_variantdesc, 
                    edw_product_key_attributes.pka_subvariantdesc, 
                    edw_product_key_attributes.pka_package, 
                    edw_product_key_attributes.pka_rootcode, 
                    edw_product_key_attributes.pka_productdesc, 
                    edw_product_key_attributes.pka_sizedesc, 
                    edw_product_key_attributes.pka_skuiddesc, 
                    ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text),
                    edw_product_key_attributes.lst_nts
                    ) a 
                    JOIN (SELECT 'Japan DCL' :: character varying AS ctry_nm, 
                                ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text) AS ean_upc, 
                                edw_product_key_attributes.pka_rootcode, 
                                edw_product_key_attributes.lst_nts AS latest_nts_date, 
                                row_number() OVER(PARTITION BY edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.lst_nts DESC) AS row_number 
                            FROM edw_product_key_attributes 
                            WHERE (((((edw_product_key_attributes.matl_type_cd):: text = ('FERT' :: character varying):: text) 
                            OR ((edw_product_key_attributes.matl_type_cd):: text = ('HALB' :: character varying):: text))
                            OR ((edw_product_key_attributes.matl_type_cd):: text = ('SAPR' :: character varying):: text))
                            AND (edw_product_key_attributes.lst_nts IS NOT NULL)) 
                            GROUP BY edw_product_key_attributes.ctry_nm, 
                                    edw_product_key_attributes.ean_upc, 
                                    edw_product_key_attributes.pka_rootcode, 
                                    edw_product_key_attributes.lst_nts
                        ) b ON (((((((a.ctry_nm):: text = (b.ctry_nm):: text) 
                            AND (a.ean_upc = b.ean_upc))
                            AND ((a.pka_rootcode):: text = (b.pka_rootcode):: text))
                            AND (b.latest_nts_date = a.nts_date))
                            AND (b.row_number = 1)))) 
                            GROUP BY 1, 
                            (a.ean_upc):: character varying, 
                            a.gcph_franchise, 
                            a.pka_franchise_description, 
                            a.gcph_category, 
                            a.gcph_subcategory, 
                            a.pka_brand_description, 
                            a.pka_subbranddesc, 
                            a.pka_variantdesc, 
                            a.pka_subvariantdesc, 
                            a.pka_package, 
                            a.pka_rootcode, 
                            a.pka_productdesc, 
                            a.pka_sizedesc, 
                            a.pka_skuiddesc
    ) derived_table1
),
benchmarks_jj as
(
    SELECT DISTINCT itg_mds_rg_sku_benchmarks.market, 
                    ltrim((itg_mds_rg_sku_benchmarks.jj_upc):: text,('0' :: character varying):: text) AS jj_upc, 
                    itg_mds_rg_sku_benchmarks.jj_packsize, 
                    itg_mds_rg_sku_benchmarks.jj_target, 
                    itg_mds_rg_sku_benchmarks.variance, 
                    itg_mds_rg_sku_benchmarks.valid_from, 
                    itg_mds_rg_sku_benchmarks.valid_to, 
                    '1' :: character varying AS benchmark_flag 
    FROM itg_mds_rg_sku_benchmarks
),
benchmarks_comp as
(
    SELECT DISTINCT itg_mds_rg_sku_benchmarks.market, 
                    ltrim((itg_mds_rg_sku_benchmarks.comp_upc):: text,('0' :: character varying):: text) AS comp_upc, 
                    itg_mds_rg_sku_benchmarks.comp_packsize, 
                    itg_mds_rg_sku_benchmarks.valid_from, 
                    itg_mds_rg_sku_benchmarks.valid_to, 
                    '1' :: character varying AS benchmark_flag 
    FROM itg_mds_rg_sku_benchmarks
),
greenlight as
(
    SELECT gn.sls_org, 
        gn.dstr_chnl, 
        gn.matl_num, 
        gn.ctry_key, 
        gn.crncy_key, 
        gn.ctry_nm, 
        gn.ctry_group, 
        gn.cluster, 
        gn.market, 
        gn.greenlight_sku_flag, 
        gn.ean_num, 
        gn.rnk 
    FROM (SELECT edw_vw_greenlight_skus.sls_org, 
                edw_vw_greenlight_skus.dstr_chnl, 
                edw_vw_greenlight_skus.matl_num, 
                edw_vw_greenlight_skus.ctry_key, 
                edw_vw_greenlight_skus.crncy_key, 
                edw_vw_greenlight_skus.ctry_nm, 
                edw_vw_greenlight_skus.ctry_group, 
                EDW_VW_GREENLIGHT_SKUS.cluster, 
                edw_vw_greenlight_skus.market, 
                edw_vw_greenlight_skus.greenlight_sku_flag, 
                ltrim((edw_vw_greenlight_skus.ean_num):: text,('0' :: character varying):: text) AS ean_num, 
                row_number() OVER(PARTITION BY ltrim((edw_vw_greenlight_skus.ean_num):: text,('0' :: character varying):: text),edw_vw_greenlight_skus.ctry_nm,EDW_VW_GREENLIGHT_SKUS.cluster ORDER BY edw_vw_greenlight_skus.matl_num,edw_vw_greenlight_skus.dstr_chnl DESC) AS rnk 
        FROM edw_vw_greenlight_skus) gn 
        WHERE gn.rnk = 1
),
crncy as
(
    SELECT derived_table2.market, 
        derived_table2.from_crncy, 
        derived_table2.to_crncy, 
        derived_table2.ex_rt, 
        derived_table2.row_number 
    FROM (SELECT CASE WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('AUD' :: character varying):: text) THEN 'Australia' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('RMB' :: character varying):: text) THEN 'China Personal Care' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('SGD' :: character varying):: text) THEN 'Singapore' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('MYR' :: character varying):: text) THEN 'Malaysia' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('IDR' :: character varying):: text) THEN 'Indonesia' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('INR' :: character varying):: text) THEN 'India' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('JPY' :: character varying):: text) THEN 'Japan' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('JPY' :: character varying):: text) THEN 'Japan DCL' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('KRW' :: character varying):: text) THEN 'Korea' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('THB' :: character varying):: text) THEN 'Thailand' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('PHP' :: character varying):: text) THEN 'Philippines' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('HKD' :: character varying):: text) THEN 'Hong Kong' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('TWD' :: character varying):: text) THEN 'Taiwan' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('VND' :: character varying):: text) THEN 'Vietnam' :: character varying 
                    WHEN ((edw_crncy_exch_rates.from_crncy):: text = ('NZD' :: character varying):: text) THEN 'New Zealand' :: character varying 
                    ELSE 'Exclude' :: character varying END AS market, 
        edw_crncy_exch_rates.from_crncy, 
        edw_crncy_exch_rates.to_crncy, 
        ((edw_crncy_exch_rates.ex_rt / edw_crncy_exch_rates.from_ratio) * edw_crncy_exch_rates.to_ratio) AS ex_rt, 
        row_number() OVER(PARTITION BY edw_crncy_exch_rates.from_crncy ORDER BY edw_crncy_exch_rates.valid_from DESC) AS row_number 
        FROM edw_crncy_exch_rates 
        WHERE (((edw_crncy_exch_rates.ex_rt_typ):: text = ('BWAR' :: character varying):: text) 
        AND ((edw_crncy_exch_rates.to_crncy):: text = ('USD' :: character varying):: text)) 
        GROUP BY edw_crncy_exch_rates.from_crncy, 
                    edw_crncy_exch_rates.to_crncy, 
                    edw_crncy_exch_rates.ex_rt, 
                    edw_crncy_exch_rates.valid_from, 
                    edw_crncy_exch_rates.from_ratio, 
                    edw_crncy_exch_rates.to_ratio
        ) derived_table2 
        WHERE ((derived_table2.row_number = 1)
        AND ((derived_table2.market):: text <> ('Exclude' :: character varying):: text))
),
rolling_asp as 
(
    SELECT a.region, 
        a.report_date, 
        a.online_store, 
        a.rpc, 
        avg(b.observed_price) AS asp 
    FROM ((SELECT EDW_CLAVIS_GB_PRODUCTS.region, 
                edw_clavis_gb_products.report_date, 
                edw_clavis_gb_products.online_store, 
                edw_clavis_gb_products.rpc 
            FROM edw_clavis_gb_products 
            WHERE ((((date_part(year,edw_clavis_gb_products.report_date) > (date_part(year,convert_timezone('UTC',current_timestamp()):: date) -2)) 
            AND (upper((edw_clavis_gb_products.avail_status):: text) <> ('VOID' :: character varying):: text))
            AND (edw_clavis_gb_products.observed_price IS NOT NULL))
            AND ((EDW_CLAVIS_GB_PRODUCTS.region):: text LIKE ANY ('AU','CN','ID','IN','JP','PH','TH','VN','MY','SG','KR','TW','HK'))) 
            GROUP BY EDW_CLAVIS_GB_PRODUCTS.region, 
                    edw_clavis_gb_products.report_date, 
                    edw_clavis_gb_products.online_store, 
                    edw_clavis_gb_products.rpc
            ) a 
            LEFT JOIN (SELECT EDW_CLAVIS_GB_PRODUCTS.region, 
                            edw_clavis_gb_products.report_date, 
                            edw_clavis_gb_products.online_store, 
                            edw_clavis_gb_products.rpc, 
                            CASE WHEN (((((EDW_CLAVIS_GB_PRODUCTS.region):: text = ('KR' :: character varying):: text) 
                                AND (upper((edw_clavis_gb_products.online_store):: text) like ('%COUPANG%' :: character varying):: text)) 
                                AND (regexp_substr(split_part(regexp_substr(replace((edw_clavis_gb_products.promo_text):: text,(',' :: character varying):: text,('' :: character varying):: text),('^[0-9]+[%]\\s[|]\\s[0-9]+[?]' :: character varying):: text),(' | ' :: character varying):: text,2),('^[0-9]+' :: character varying):: text) <> ('' :: character varying):: text))
                                AND (regexp_substr(split_part(regexp_substr(replace((edw_clavis_gb_products.promo_text):: text,(',' :: character varying):: text,('' :: character varying):: text),('^[0-9]+[%]\\s[|]\\s[0-9]+[?]' :: character varying):: text),(' | ' :: character varying):: text,2),('^[0-9]+' :: character varying):: text) IS NOT NULL)) 
                                THEN (regexp_substr(split_part(regexp_substr(replace((edw_clavis_gb_products.promo_text):: text,(',' :: character varying):: text,('' :: character varying):: text),('^[0-9]+[%]\\s[|]\\s[0-9]+[?]' :: character varying):: text),(' | ' :: character varying):: text,2),('^[0-9]+' :: character varying):: text)):: double precision 
                                ELSE (edw_clavis_gb_products.observed_price):: double precision END AS observed_price 
                    FROM edw_clavis_gb_products 
                    WHERE ((((date_part(year,edw_clavis_gb_products.report_date) > (date_part(year,convert_timezone('UTC',current_timestamp()):: date) -3))
                    AND (upper((edw_clavis_gb_products.avail_status):: text) <> ('VOID' :: character varying):: text))
                    AND (edw_clavis_gb_products.observed_price IS NOT NULL)) 
                    AND ((EDW_CLAVIS_GB_PRODUCTS.region):: text LIKE ANY ('AU','CN','ID','IN','JP','PH','TH','VN','MY','SG','KR','TW','HK'))) 
                    GROUP BY EDW_CLAVIS_GB_PRODUCTS.region, 
                    edw_clavis_gb_products.report_date, 
                    edw_clavis_gb_products.online_store, 
                    edw_clavis_gb_products.rpc, 
                    edw_clavis_gb_products.observed_price, 
                    edw_clavis_gb_products.promo_text
                ) b ON (((((((a.region):: text = (b.region):: text) 
                    AND ((a.online_store):: text = (b.online_store):: text)) 
                    AND ((a.rpc):: text = (b.rpc):: text))
                    AND (dateadd('week',- 13,(a.report_date):: date) <= b.report_date))
                    AND (a.report_date >= b.report_date))))
                    GROUP BY a.region, 
                    a.report_date, 
                    a.online_store, 
                    a.rpc
),
rolling_mrp1 as
(
    SELECT derived_table3.region, 
            derived_table3.online_store, 
            derived_table3.platform, 
            derived_table3.report_date, 
            derived_table3.rpc, 
            derived_table3.mrp, 
            'Definite' :: character varying AS mrp_type 
    FROM (SELECT base.region, 
                base.online_store, 
                base.report_date, 
                base.is_on_promotion, 
                ref.report_date AS ref_date, 
                base.platform, 
                base.rpc, 
                base.observed_price, 
                ref.observed_price AS mrp, 
                (base.report_date - ref.report_date) AS day_diff, 
                row_number() OVER(PARTITION BY base.region,base.online_store,base.report_date,base.rpc ORDER BY (base.report_date - ref.report_date)) AS row_num 
        FROM ((SELECT a.report_date, 
                        a.region, 
                        a.online_store, 
                        b.group_customer AS platform, 
                        a.rpc, 
                        a.observed_price, 
                        a.is_on_promotion 
                FROM (edw_clavis_gb_products a
                        LEFT JOIN (SELECT DISTINCT itg_mds_rg_ecom_digital_shelf_customer_mapping.cntry_cd, 
                                        itg_mds_rg_ecom_digital_shelf_customer_mapping.market, 
                                        itg_mds_rg_ecom_digital_shelf_customer_mapping.channel, 
                                        itg_mds_rg_ecom_digital_shelf_customer_mapping.re, 
                                        itg_mds_rg_ecom_digital_shelf_customer_mapping.group_customer, 
                                        itg_mds_rg_ecom_digital_shelf_customer_mapping.online_store, 
                                        itg_mds_rg_ecom_digital_shelf_customer_mapping.data_provider, 
                                        ITG_MDS_RG_ECOM_DIGITAL_SHELF_CUSTOMER_MAPPING.cluster, 
                                        itg_mds_rg_ecom_digital_shelf_customer_mapping.ctrd_dttm 
                                    FROM itg_mds_rg_ecom_digital_shelf_customer_mapping
                            ) b ON (((((a.region):: text = (b.cntry_cd):: text) 
                                AND ((a.online_store):: text = (b.online_store):: text)) 
                                AND (upper((b.data_provider):: text) = ('CLAVIS' :: character varying):: text)))) 
                WHERE (((date_part(year,a.report_date) > (date_part(year,convert_timezone('UTC',current_timestamp()):: date) -2))
                AND (a.avail_comp = 1))
                AND ((a.region):: text LIKE ANY ('AU','CN','ID','IN','JP','PH','TH','VN','MY','SG','KR','TW','HK')))) base 
                LEFT JOIN (SELECT a.report_date, 
                                a.region, 
                                a.online_store, 
                                b.group_customer AS platform, 
                                a.rpc, 
                                a.observed_price, 
                                a.is_on_promotion 
                            FROM (edw_clavis_gb_products a 
                            LEFT JOIN (SELECT DISTINCT itg_mds_rg_ecom_digital_shelf_customer_mapping.cntry_cd, 
                                            itg_mds_rg_ecom_digital_shelf_customer_mapping.market, 
                                            itg_mds_rg_ecom_digital_shelf_customer_mapping.channel, 
                                            itg_mds_rg_ecom_digital_shelf_customer_mapping.re, 
                                            itg_mds_rg_ecom_digital_shelf_customer_mapping.group_customer, 
                                            itg_mds_rg_ecom_digital_shelf_customer_mapping.online_store, 
                                            itg_mds_rg_ecom_digital_shelf_customer_mapping.data_provider, 
                                            ITG_MDS_RG_ECOM_DIGITAL_SHELF_CUSTOMER_MAPPING.cluster, 
                                            itg_mds_rg_ecom_digital_shelf_customer_mapping.ctrd_dttm 
                            FROM itg_mds_rg_ecom_digital_shelf_customer_mapping
                            ) b ON (((((a.region):: text = (b.cntry_cd):: text) 
                                AND ((a.online_store):: text = (b.online_store):: text))
                                AND (upper((b.data_provider):: text) = ('CLAVIS' :: character varying):: text)))) 
                    WHERE ((((date_part(year,a.report_date) > (date_part(year,convert_timezone('UTC',current_timestamp()):: date) -3))
                    AND (a.avail_comp = 1)) 
                    AND (a.is_on_promotion = 0))
                    AND ((a.region):: text LIKE ANY ('AU','CN','ID','IN','JP','PH','TH','VN','MY','SG','KR','TW','HK')))
                    ) ref 
                    ON (((((((datediff(day,(base.report_date):: date,(ref.report_date):: date) > -92)
                    AND (datediff(day,(base.report_date):: date,(ref.report_date):: date) < 1))
                    AND ((base.rpc):: text = (ref.rpc):: text))
                    AND ((base.region):: text = (ref.region):: text))
                    AND ((base.platform):: text = (ref.platform):: text))
                    AND ((base.online_store):: text = (ref.online_store):: text)))) 
                GROUP BY base.region, 
                        base.online_store, 
                        base.report_date, 
                        base.is_on_promotion, 
                        ref.report_date, 
                        base.platform, 
                        base.rpc, 
                        base.observed_price, 
                        ref.observed_price, 
                        (base.report_date - ref.report_date)
                ) derived_table3 
            WHERE derived_table3.row_num = 1 
            GROUP BY derived_table3.region, 
                    derived_table3.online_store, 
                    derived_table3.platform, 
                    derived_table3.report_date, 
                    derived_table3.rpc, 
                    derived_table3.mrp, 
                    7
),
rolling_mrp2 as
(
    SELECT a.region, 
        a.report_date, 
        a.online_store, 
        a.rpc, 
        max(b.observed_price) AS mrp, 
        'Estimated' :: character varying AS mrp_type 
    FROM ((SELECT EDW_CLAVIS_GB_PRODUCTS.region, 
                edw_clavis_gb_products.report_date, 
                edw_clavis_gb_products.online_store, 
                edw_clavis_gb_products.rpc 
            FROM edw_clavis_gb_products
            WHERE ((((date_part(year,edw_clavis_gb_products.report_date) > (date_part(year,convert_timezone('UTC',current_timestamp()):: date) -2)) 
            AND (upper((edw_clavis_gb_products.avail_status):: text) <> ('VOID' :: character varying):: text))
            AND (edw_clavis_gb_products.observed_price IS NOT NULL)) 
            AND ((EDW_CLAVIS_GB_PRODUCTS.region):: text LIKE ANY ('AU','CN','ID','IN','JP','PH','TH','VN','MY','SG','KR','TW','HK')))
            GROUP BY EDW_CLAVIS_GB_PRODUCTS.region, 
                    edw_clavis_gb_products.report_date, 
                    edw_clavis_gb_products.online_store, 
                    edw_clavis_gb_products.rpc
                ) a 
        LEFT JOIN (SELECT EDW_CLAVIS_GB_PRODUCTS.region, 
                        edw_clavis_gb_products.report_date, 
                        edw_clavis_gb_products.online_store, 
                        edw_clavis_gb_products.rpc, 
                        edw_clavis_gb_products.observed_price 
                FROM edw_clavis_gb_products 
                WHERE ((((date_part(year,edw_clavis_gb_products.report_date) > (date_part(year,convert_timezone('UTC',current_timestamp()):: date) -3))
                AND (upper((edw_clavis_gb_products.avail_status):: text) <> ('VOID' :: character varying):: text))
                AND (edw_clavis_gb_products.observed_price IS NOT NULL))
                AND ((EDW_CLAVIS_GB_PRODUCTS.region):: text LIKE ANY ('AU','CN','ID','IN','JP','PH','TH','VN','MY','SG','KR','TW','HK'))) 
                GROUP BY EDW_CLAVIS_GB_PRODUCTS.region, 
                        edw_clavis_gb_products.report_date, 
                        edw_clavis_gb_products.online_store, 
                        edw_clavis_gb_products.rpc, 
                        edw_clavis_gb_products.observed_price
            ) b ON (((((((a.region):: text = (b.region):: text) 
                AND ((a.online_store):: text = (b.online_store):: text))
                AND ((a.rpc):: text = (b.rpc):: text))
                AND (dateadd(week,-13,(a.report_date):: date) <= b.report_date))
                AND (a.report_date >= b.report_date)))) 
            GROUP BY a.region, 
                    a.report_date, 
                    a.online_store, 
                    a.rpc, 
                    6
),
mkt_mirror as
(
    SELECT derived_table1.report_date, 
        derived_table1.supplier, 
        derived_table1.source, 
        derived_table1.cluster, 
        derived_table1.market, 
        derived_table1.parent_customer, 
        derived_table1.manufacturer, 
        derived_table1.is_competitor, 
        derived_table1.platform, 
        derived_table1.source_prod_hier_l1, 
        derived_table1.source_prod_hier_l2, 
        derived_table1.source_prod_hier_l3, 
        derived_table1.source_prod_hier_l5, 
        derived_table1.source_packsize, 
        derived_table1.ean_upc, 
        derived_table1.utag, 
        derived_table1.product, 
        derived_table1.asp_lcy, 
        derived_table1.asp_usd 
    FROM (SELECT trans.time_period AS report_date, 
                trans.supplier, 
                trans.source, 
                market_map.destination_cluster AS cluster, 
                CASE WHEN (((trans.market):: text = ('China' :: character varying):: text) AND ((trans.supplier):: text = ('IQVIA' :: character varying):: text)) THEN 'China Selfcare' :: character varying 
                    WHEN (((trans.market):: text = ('China' :: character varying):: text) AND ((trans.supplier):: text <> ('IQVIA' :: character varying):: text)) THEN 'China Personal Care' :: character varying 
                    WHEN (((market_map.destination_market):: text = ('Japan' :: character varying):: text) AND ((upper((trans.brand):: text) like ('DR%CI%LABO' :: character varying):: text) OR (upper((trans.category):: text) like ('%FACIAL%' :: character varying):: text))) THEN 'Japan DCL' :: character varying ELSE market_map.destination_market END AS market, 
                CASE WHEN (upper((trans.channel_type):: text) = ('KAD' :: character varying):: text) THEN trans.channel ELSE NULL :: character varying END AS parent_customer, 
                CASE WHEN ((upper((trans.manufacturer):: text)):: character varying(300) IN (SELECT DISTINCT itg_query_parameters.parameter_value AS manufacturer FROM itg_query_parameters WHERE (((itg_query_parameters.country_code):: text = ('APAC' :: character varying):: text) AND ((itg_query_parameters.parameter_name):: text = ('price_tracker_mfr' :: character varying):: text)))) THEN 'J&J' :: character varying ELSE trans.manufacturer END AS manufacturer, 
                CASE WHEN (((upper((trans.manufacturer):: text)):: character varying(300) IN (SELECT DISTINCT itg_query_parameters.parameter_value AS manufacturer FROM itg_query_parameters WHERE (((itg_query_parameters.country_code):: text = ('APAC' :: character varying):: text) AND ((itg_query_parameters.parameter_name):: text = ('price_tracker_mfr' :: character varying):: text)))) OR (upper((trans.brand):: text) like ('DR%CI%LABO' :: character varying):: text)) THEN 'FALSE' :: character varying ELSE 'TRUE' :: character varying END AS is_competitor, 
                trans.channel_description AS platform, 
                trans.category AS source_prod_hier_l1, 
                trans.segment AS source_prod_hier_l2, 
                trans.brand AS source_prod_hier_l3, 
                trans.sub_brand AS source_prod_hier_l5, 
                trans.packsize AS source_packsize, 
                max(CASE WHEN (upper((trans.attribute_1):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_1_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_1_desc):: text, ('0' :: character varying):: text)
                                ELSE left(ltrim((trans.attribute_1_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_1_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_2):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_2_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_2_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_2_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim(( trans.attribute_2_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_3):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_3_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_3_desc):: text, ('0' :: character varying):: text)
                                ELSE left(ltrim((trans.attribute_3_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_3_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_4):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_4_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_4_desc):: text, ('0' :: character varying):: text)
                                ELSE left(ltrim((trans.attribute_4_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_4_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_5):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_5_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_5_desc):: text, ('0' :: character varying):: text)
                                ELSE left(ltrim((trans.attribute_5_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_5_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_6):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_6_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_6_desc):: text, ('0' :: character varying):: text)
                                ELSE left(ltrim((trans.attribute_6_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_6_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_7):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_7_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_7_desc):: text, ('0' :: character varying):: text)
                                ELSE left(ltrim((trans.attribute_7_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_7_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_8):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_8_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_8_desc):: text, ('0' :: character varying):: text)
                                ELSE left(ltrim((trans.attribute_8_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_8_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_9):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_9_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_9_desc):: text, ('0' :: character varying):: text)
                                ELSE left(ltrim((trans.attribute_9_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_9_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_10):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_10_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_10_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_10_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_10_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_11):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_11_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_11_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_11_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_11_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_12):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_12_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_12_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_12_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_12_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_13):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_13_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_13_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_13_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_13_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_14):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_14_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_14_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_14_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_14_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_15):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_15_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_15_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_15_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_15_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_16):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_16_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_16_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_16_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_16_desc):: text,('0' :: character varying):: text):: text)) END 
                        WHEN (upper((trans.attribute_17):: text) = ('EAN' :: character varying):: text) 
                        THEN CASE WHEN (position((' ' :: character varying),ltrim((trans.attribute_17_desc):: text,('0' :: character varying):: text):: text) IS NULL) THEN ltrim((trans.attribute_17_desc):: text, ('0' :: character varying):: text) 
                                ELSE left(ltrim((trans.attribute_17_desc):: text,('0' :: character varying):: text),position((' ' :: character varying),ltrim((trans.attribute_17_desc):: text,('0' :: character varying):: text):: text)) END ELSE ltrim((("map".ean):: character varying):: text, ('0' :: character varying):: text) END) AS ean_upc, 
                trans.utag, 
                trans.product, 
                CASE WHEN (sum(trans.sku_unit_sales) <> (0):: double precision) THEN (sum(trans.sku_value_sales_lc) / sum(trans.sku_unit_sales)) ELSE NULL :: double precision END AS asp_lcy, 
                CASE WHEN (sum(trans.sku_unit_sales) <> (0):: double precision) THEN (sum(trans.sku_value_sales_usd) / sum(trans.sku_unit_sales)) ELSE NULL :: double precision END AS asp_usd 
                FROM ((edw_market_mirror_fact trans 
                LEFT JOIN (SELECT itg_mds_rg_ecom_product.rpc,max((itg_mds_rg_ecom_product.ean):: text) AS ean 
                            FROM itg_mds_rg_ecom_product 
                            GROUP BY itg_mds_rg_ecom_product.rpc) "map" 
                ON (((trans.utag):: text = ("map".rpc):: text))) 
                LEFT JOIN itg_mds_ap_sales_ops_map market_map 
                ON (((upper((market_map.source_market):: text) = upper((trans.ggh_country):: text)) 
                AND ((market_map.dataset):: text = ('Market Share QSD' :: character varying):: text)))) 
                WHERE ((((((((trans.ggh_region):: text = ('APAC' :: character varying):: text)
                AND (upper((trans.channel_source):: text) <> ('E-COMMERCE' :: character varying):: text))
                AND (upper((trans.product):: text) ! like ('ALL OTHER%' :: character varying):: text))
                AND (((upper((trans.channel_type):: text) = ('FORMAT' :: character varying):: text)
                OR (upper((trans.channel_type):: text) = ('KAD' :: character varying):: text))
                OR ((((trans.market):: text = ('China' :: character varying):: text)
                AND ((trans.supplier):: text = ('IQVIA' :: character varying):: text))
                AND ((trans.channel_type):: text = ('Total' :: character varying):: text))))
                AND (trans.sku_unit_sales IS NOT NULL))
                AND (trans.sku_unit_sales <> (0):: double precision)) 
                AND ((date_part(year,convert_timezone('UTC',current_timestamp()):: date) -2) < date_part(year,trans.time_period))) 
                GROUP BY trans.time_period, 
                        trans.supplier, 
                        trans.source, 
                        market_map.destination_cluster, 
                        CASE WHEN (((trans.market):: text = ('China' :: character varying):: text) AND ((trans.supplier):: text = ('IQVIA' :: character varying):: text)) THEN 'China Selfcare' :: character varying 
                            WHEN (((trans.market):: text = ('China' :: character varying):: text) AND ((trans.supplier):: text <> ('IQVIA' :: character varying):: text)) THEN 'China Personal Care' :: character varying 
                            WHEN (((market_map.destination_market):: text = ('Japan' :: character varying):: text) AND ((upper((trans.brand):: text) like ('DR%CI%LABO' :: character varying):: text) OR (upper((trans.category):: text) like ('%FACIAL%' :: character varying):: text))) THEN 'Japan DCL' :: character varying ELSE market_map.destination_market END, 
                        CASE WHEN (upper((trans.channel_type):: text) = ('KAD' :: character varying):: text) THEN trans.channel ELSE NULL :: character varying END, 
                        CASE WHEN ((upper((trans.manufacturer):: text)):: character varying(300) IN (SELECT DISTINCT itg_query_parameters.parameter_value AS manufacturer FROM itg_query_parameters WHERE(((itg_query_parameters.country_code):: text = ('APAC' :: character varying):: text) AND ((itg_query_parameters.parameter_name):: text = ('price_tracker_mfr' :: character varying):: text)))) THEN 'J&J' :: character varying ELSE trans.manufacturer END, 
                        CASE WHEN (((upper((trans.manufacturer):: text)):: character varying(300) IN (SELECT DISTINCT itg_query_parameters.parameter_value AS manufacturer FROM itg_query_parameters WHERE(((itg_query_parameters.country_code):: text = ('APAC' :: character varying):: text) AND ((itg_query_parameters.parameter_name):: text = ('price_tracker_mfr' :: character varying):: text)))) OR (upper((trans.brand):: text) like ('DR%CI%LABO' :: character varying):: text)) THEN 'FALSE' :: character varying ELSE 'TRUE' :: character varying END, 
                        trans.manufacturer, 
                        trans.channel_description, 
                        trans.category, 
                        trans.segment, 
                        trans.brand, 
                        trans.sub_brand, 
                        trans.packsize, 
                        trans.utag, 
                        trans.product
                ) derived_table1
),
pka1 as
(
    SELECT CASE WHEN ((a.ctry_nm):: text = ('APSC Regional' :: character varying):: text) THEN 'China Personal Care' :: character varying ELSE a.ctry_nm END AS ctry_nm, 
        (a.ean_upc):: character varying AS ean_upc, 
        a.gcph_franchise, 
        a.pka_franchise_description, 
        a.gcph_category, 
        a.gcph_subcategory, 
        a.pka_brand_description, 
        a.pka_subbranddesc, 
        a.pka_variantdesc, 
        a.pka_subvariantdesc, 
        a.pka_package, 
        a.pka_rootcode, 
        a.pka_productdesc, 
        a.pka_sizedesc, 
        a.pka_skuiddesc 
    FROM ((SELECT edw_product_key_attributes.ctry_nm, 
                edw_product_key_attributes.gcph_franchise, 
                edw_product_key_attributes.pka_franchise_description, 
                edw_product_key_attributes.gcph_category, 
                edw_product_key_attributes.gcph_subcategory, 
                edw_product_key_attributes.pka_brand_description, 
                edw_product_key_attributes.pka_subbranddesc, 
                edw_product_key_attributes.pka_variantdesc, 
                edw_product_key_attributes.pka_subvariantdesc, 
                edw_product_key_attributes.pka_package, 
                edw_product_key_attributes.pka_rootcode, 
                edw_product_key_attributes.pka_productdesc, 
                edw_product_key_attributes.pka_sizedesc, 
                edw_product_key_attributes.pka_skuiddesc, 
                ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text) AS ean_upc, 
                edw_product_key_attributes.lst_nts AS nts_date 
            FROM edw_product_key_attributes 
            WHERE (((((edw_product_key_attributes.matl_type_cd):: text = ('FERT' :: character varying):: text) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('HALB' :: character varying):: text)) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('SAPR' :: character varying):: text)) 
                AND (edw_product_key_attributes.lst_nts IS NOT NULL)) 
            GROUP BY edw_product_key_attributes.ctry_nm, 
                    edw_product_key_attributes.gcph_franchise, 
                    edw_product_key_attributes.pka_franchise_description, 
                    edw_product_key_attributes.gcph_category, 
                    edw_product_key_attributes.gcph_subcategory, 
                    edw_product_key_attributes.pka_brand_description, 
                    edw_product_key_attributes.pka_subbranddesc, 
                    edw_product_key_attributes.pka_variantdesc, 
                    edw_product_key_attributes.pka_subvariantdesc, 
                    edw_product_key_attributes.pka_package, 
                    edw_product_key_attributes.pka_rootcode, 
                    edw_product_key_attributes.pka_productdesc, 
                    edw_product_key_attributes.pka_sizedesc, 
                    edw_product_key_attributes.pka_skuiddesc, 
                    ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text),edw_product_key_attributes.lst_nts) a 
            JOIN (SELECT edw_product_key_attributes.ctry_nm, 
                        ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text) AS ean_upc, 
                        edw_product_key_attributes.pka_rootcode, 
                        edw_product_key_attributes.lst_nts AS latest_nts_date, 
                        row_number() OVER(PARTITION BY edw_product_key_attributes.ctry_nm,edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.lst_nts DESC) AS row_number 
                FROM edw_product_key_attributes WHERE (((((edw_product_key_attributes.matl_type_cd):: text = ('FERT' :: character varying):: text) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('HALB' :: character varying):: text)) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('SAPR' :: character varying):: text))
                AND (edw_product_key_attributes.lst_nts IS NOT NULL)) 
                GROUP BY edw_product_key_attributes.ctry_nm, 
                        edw_product_key_attributes.ean_upc, 
                        edw_product_key_attributes.pka_rootcode, 
                        edw_product_key_attributes.lst_nts
                ) b ON (((((((a.ctry_nm):: text = (b.ctry_nm):: text) 
                    AND (a.ean_upc = b.ean_upc)) 
                    AND ((a.pka_rootcode):: text = (b.pka_rootcode):: text)) 
                    AND (b.latest_nts_date = a.nts_date)) 
                    AND (b.row_number = 1)))) 
                    GROUP BY CASE WHEN ((a.ctry_nm):: text = ('APSC Regional' :: character varying):: text) THEN 'China Personal Care' :: character varying ELSE a.ctry_nm END, 
                            (a.ean_upc):: character varying, 
                            a.gcph_franchise, 
                            a.pka_franchise_description, 
                            a.gcph_category, 
                            a.gcph_subcategory, 
                            a.pka_brand_description, 
                            a.pka_subbranddesc, 
                            a.pka_variantdesc, 
                            a.pka_subvariantdesc, 
                            a.pka_package, 
                            a.pka_rootcode, 
                            a.pka_productdesc, 
                            a.pka_sizedesc, 
                            a.pka_skuiddesc 
                            
    UNION ALL 

    SELECT 'Japan DCL' :: character varying AS ctry_nm, 
        (a.ean_upc):: character varying AS ean_upc, 
        a.gcph_franchise, 
        a.pka_franchise_description, 
        a.gcph_category, 
        a.gcph_subcategory, 
        a.pka_brand_description, 
        a.pka_subbranddesc, 
        a.pka_variantdesc, 
        a.pka_subvariantdesc, 
        a.pka_package, 
        a.pka_rootcode, 
        a.pka_productdesc, 
        a.pka_sizedesc, 
        a.pka_skuiddesc 
    FROM ((SELECT 'Japan DCL' :: character varying AS ctry_nm, 
                edw_product_key_attributes.gcph_franchise, 
                edw_product_key_attributes.pka_franchise_description, 
                edw_product_key_attributes.gcph_category, 
                edw_product_key_attributes.gcph_subcategory, 
                edw_product_key_attributes.pka_brand_description, 
                edw_product_key_attributes.pka_subbranddesc, 
                edw_product_key_attributes.pka_variantdesc, 
                edw_product_key_attributes.pka_subvariantdesc, 
                edw_product_key_attributes.pka_package, 
                edw_product_key_attributes.pka_rootcode, 
                edw_product_key_attributes.pka_productdesc, 
                edw_product_key_attributes.pka_sizedesc, 
                edw_product_key_attributes.pka_skuiddesc, 
                ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text) AS ean_upc, 
                edw_product_key_attributes.lst_nts AS nts_date 
            FROM edw_product_key_attributes 
            WHERE ((((((edw_product_key_attributes.matl_type_cd):: text = ('FERT' :: character varying):: text) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('HALB' :: character varying):: text)) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('SAPR' :: character varying):: text)) 
                AND (edw_product_key_attributes.lst_nts IS NOT NULL)) 
                AND (upper((edw_product_key_attributes.gcph_brand):: text) = ('DR. CI: LABO' :: character varying):: text)) 
                GROUP BY 1, 
                        edw_product_key_attributes.gcph_franchise, 
                        edw_product_key_attributes.pka_franchise_description, 
                        edw_product_key_attributes.gcph_category, 
                        edw_product_key_attributes.gcph_subcategory, 
                        edw_product_key_attributes.pka_brand_description, 
                        edw_product_key_attributes.pka_subbranddesc, 
                        edw_product_key_attributes.pka_variantdesc, 
                        edw_product_key_attributes.pka_subvariantdesc, 
                        edw_product_key_attributes.pka_package, 
                        edw_product_key_attributes.pka_rootcode, 
                        edw_product_key_attributes.pka_productdesc, 
                        edw_product_key_attributes.pka_sizedesc, 
                        edw_product_key_attributes.pka_skuiddesc, 
                        ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text),edw_product_key_attributes.lst_nts) a 
            JOIN (SELECT 'Japan DCL' :: character varying AS ctry_nm, 
                        ltrim((edw_product_key_attributes.ean_upc):: text,('0' :: character varying):: text) AS ean_upc, 
                        edw_product_key_attributes.pka_rootcode, 
                        edw_product_key_attributes.lst_nts AS latest_nts_date, 
                        row_number() OVER(PARTITION BY edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.lst_nts DESC) AS row_number 
                FROM edw_product_key_attributes 
                WHERE (((((edw_product_key_attributes.matl_type_cd):: text = ('FERT' :: character varying):: text) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('HALB' :: character varying):: text)) 
                OR ((edw_product_key_attributes.matl_type_cd):: text = ('SAPR' :: character varying):: text)) 
                AND (edw_product_key_attributes.lst_nts IS NOT NULL)) 
                GROUP BY edw_product_key_attributes.ctry_nm, 
                            edw_product_key_attributes.ean_upc, 
                            edw_product_key_attributes.pka_rootcode, 
                            edw_product_key_attributes.lst_nts
                ) b ON (((((((a.ctry_nm):: text = (b.ctry_nm):: text) 
                        AND (a.ean_upc = b.ean_upc)) 
                        AND ((a.pka_rootcode):: text = (b.pka_rootcode):: text)) 
                        AND (b.latest_nts_date = a.nts_date))
                        AND (b.row_number = 1)))) 
                    GROUP BY 1, 
                            (a.ean_upc):: character varying, 
                            a.gcph_franchise, 
                            a.pka_franchise_description, 
                            a.gcph_category, 
                            a.gcph_subcategory, 
                            a.pka_brand_description, 
                            a.pka_subbranddesc, 
                            a.pka_variantdesc, 
                            a.pka_subvariantdesc, 
                            a.pka_package, 
                            a.pka_rootcode, 
                            a.pka_productdesc, 
                            a.pka_sizedesc, 
                            a.pka_skuiddesc
),
crncy1 as
(
    SELECT 
        derived_table2.market, 
        derived_table2.from_crncy, 
        derived_table2.to_crncy, 
        derived_table2.ex_rt, 
        derived_table2.row_number 
    FROM (
        SELECT 
            CASE 
                WHEN (edw_crncy_exch_rates.from_crncy)::text = ('RMB'::character varying)::text 
                THEN 'China Personal Care'::character varying 
                ELSE 'Exclude'::character varying 
            END AS market, 
            edw_crncy_exch_rates.from_crncy, 
            edw_crncy_exch_rates.to_crncy, 
            (
                (edw_crncy_exch_rates.ex_rt / edw_crncy_exch_rates.from_ratio) * 
                edw_crncy_exch_rates.to_ratio
            ) AS ex_rt, 
            row_number() OVER (
                PARTITION BY edw_crncy_exch_rates.from_crncy 
                ORDER BY edw_crncy_exch_rates.valid_from DESC
            ) AS row_number 
        FROM edw_crncy_exch_rates 
        WHERE 
            (edw_crncy_exch_rates.ex_rt_typ)::text = ('BWAR'::character varying)::text 
            AND (edw_crncy_exch_rates.to_crncy)::text = ('USD'::character varying)::text
        GROUP BY 
            edw_crncy_exch_rates.from_crncy, 
            edw_crncy_exch_rates.to_crncy, 
            edw_crncy_exch_rates.ex_rt, 
            edw_crncy_exch_rates.valid_from, 
            edw_crncy_exch_rates.from_ratio, 
            edw_crncy_exch_rates.to_ratio
    ) derived_table2 
    WHERE 
        derived_table2.row_number = 1 
        AND (derived_table2.market)::text <> ('Exclude'::character varying)::text
),
cn_digital_shelf as
(
    SELECT 'China' :: character varying AS cluster, 
        'China Personal Care' :: character varying AS market, 
        customer.channel, 
        customer.re AS retail_environment, 
        trans.processing_date AS report_date, 
        customer.group_customer AS platform, 
        trans.retailer AS online_store, 
        trans.substore, 
        trans.substore_name_en, 
        trans.substore_name_cn, 
        trans.category, 
        trans.subcat1, 
        trans.subcat2, 
        upper((trans.is_competitor):: text) AS is_competitor, 
        CASE WHEN ((upper((trans.manufacturer):: text)):: character varying(300) IN (SELECT DISTINCT itg_query_parameters.parameter_value AS manufacturer FROM itg_query_parameters WHERE (((itg_query_parameters.country_code):: text = ('APAC' :: character varying):: text) AND ((itg_query_parameters.parameter_name):: text = ('price_tracker_mfr' :: character varying):: text)))) 
        THEN 'J&J' :: character varying ELSE trans.manufacturer END AS manufacturer, 
        trans.brand, 
        trans.variant_en, 
        trans.variant_cn, 
        CASE WHEN ((sku_map.upc IS NOT NULL) AND (((sku_map.upc):: character varying):: text <> ('' :: character varying):: text)) 
        THEN (ltrim(((sku_map.upc):: character varying):: text,('0' :: character varying):: text)):: character varying 
        ELSE trans.trusted_upc END AS ean_upc, 
        trans.std_sku_name_cn, 
        trans.std_sku_name_en, 
        trans.sap_code AS materialnumber, 
        trans.rpc, 
        trans.product_description_en, 
        trans.product_description_cn, 
        trans.is_bundle, 
        trans.std_pack_size AS base_packsize, 
        trans.pcs AS packsize_multiplier, 
        trans.volume AS final_packsize, 
        trans.msrp_rmb AS msrp_lcy, 
        trans.listed_price_rmb AS mrp_lcy, 
        trans.observed_price_rmb AS observed_price_lcy, 
        trans.net_price_rmb AS bcp_lcy, 
        CASE WHEN ((trans.on_promotion):: text = ('TRUE' :: character varying):: text) THEN 1 ELSE NULL :: integer 
        END AS cust_promo_flag, 
        CASE WHEN (trans.discount_rmb = (((0):: numeric):: numeric(18, 0)):: numeric(23, 5)) THEN NULL :: integer ELSE 1 END AS price_promo_flag, 
        (((trans.promotion_type):: text || (' | ' :: character varying):: text) || (trans.promotion_effective_en):: text) AS promo_text 
    FROM ((edw_cn_digital_shelf_data trans 
        LEFT JOIN itg_mds_rg_ecom_digital_shelf_customer_mapping customer
        ON ((((upper((trans.cntry_cd):: text) = upper((customer.cntry_cd):: text)) 
        AND (upper((customer.online_store):: text) = upper((trans.retailer):: text))) 
        AND ((customer.data_provider):: text = ('Clavis' :: character varying):: text))))
        LEFT JOIN (SELECT itg_mds_rg_ecom_product.manufacturer, 
                        itg_mds_rg_ecom_product.rpc, 
                        ltrim((itg_mds_rg_ecom_product.ean):: text,('0' :: character varying):: text) AS upc 
                    FROM itg_mds_rg_ecom_product 
                    WHERE ((itg_mds_rg_ecom_product.market):: text = ('China Personal Care' :: character varying):: text) 
                    GROUP BY itg_mds_rg_ecom_product.manufacturer, 
                            itg_mds_rg_ecom_product.rpc, 
                            ltrim((itg_mds_rg_ecom_product.ean):: text,('0' :: character varying):: text)
                    ) sku_map 
                    ON (((sku_map.rpc):: text = (trans.rpc):: text)))
                    WHERE (date_part(year,trans.processing_date) > (date_part(year, convert_timezone('UTC',current_timestamp()):: date) -2))
),
pka2 as
(
    SELECT 
        CASE 
            WHEN (a.ctry_nm)::text = ('APSC Regional'::character varying)::text 
            THEN 'China Personal Care'::character varying 
            ELSE a.ctry_nm 
        END AS ctry_nm, 
        a.ean_upc, 
        a.gcph_franchise, 
        a.pka_franchise_description, 
        a.gcph_category, 
        a.gcph_subcategory, 
        a.pka_brand_description, 
        a.pka_subbranddesc, 
        a.pka_variantdesc, 
        a.pka_subvariantdesc, 
        a.pka_package, 
        a.pka_rootcode, 
        a.pka_productdesc, 
        a.pka_sizedesc, 
        a.pka_skuiddesc 
    FROM (
        SELECT 
            edw_product_key_attributes.ctry_nm, 
            edw_product_key_attributes.gcph_franchise, 
            edw_product_key_attributes.pka_franchise_description, 
            edw_product_key_attributes.gcph_category, 
            edw_product_key_attributes.gcph_subcategory, 
            edw_product_key_attributes.pka_brand_description, 
            edw_product_key_attributes.pka_subbranddesc, 
            edw_product_key_attributes.pka_variantdesc, 
            edw_product_key_attributes.pka_subvariantdesc, 
            edw_product_key_attributes.pka_package, 
            edw_product_key_attributes.pka_rootcode, 
            edw_product_key_attributes.pka_productdesc, 
            edw_product_key_attributes.pka_sizedesc, 
            edw_product_key_attributes.pka_skuiddesc, 
            ltrim(edw_product_key_attributes.ean_upc::text, '0'::character varying) AS ean_upc, 
            edw_product_key_attributes.lst_nts AS nts_date 
        FROM edw_product_key_attributes 
        WHERE (
            (edw_product_key_attributes.matl_type_cd::text IN ('FERT'::character varying, 'HALB'::character varying, 'SAPR'::character varying)) 
            AND edw_product_key_attributes.lst_nts IS NOT NULL
            AND (
                upper(edw_product_key_attributes.ctry_nm::text) IN ('APSC REGIONAL'::character varying, 'China Personal Care'::character varying)
            )
        ) 
        GROUP BY 
            edw_product_key_attributes.ctry_nm, 
            edw_product_key_attributes.gcph_franchise, 
            edw_product_key_attributes.pka_franchise_description, 
            edw_product_key_attributes.gcph_category, 
            edw_product_key_attributes.gcph_subcategory, 
            edw_product_key_attributes.pka_brand_description, 
            edw_product_key_attributes.pka_subbranddesc, 
            edw_product_key_attributes.pka_variantdesc, 
            edw_product_key_attributes.pka_subvariantdesc, 
            edw_product_key_attributes.pka_package, 
            edw_product_key_attributes.pka_rootcode, 
            edw_product_key_attributes.pka_productdesc, 
            edw_product_key_attributes.pka_sizedesc, 
            edw_product_key_attributes.pka_skuiddesc, 
            ltrim(edw_product_key_attributes.ean_upc::text, '0'::character varying), 
            edw_product_key_attributes.lst_nts
    ) a 
    JOIN (
        SELECT 
            edw_product_key_attributes.ctry_nm, 
            ltrim(edw_product_key_attributes.ean_upc::text, '0'::character varying) AS ean_upc, 
            edw_product_key_attributes.pka_rootcode, 
            edw_product_key_attributes.lst_nts AS latest_nts_date, 
            row_number() OVER (
                PARTITION BY edw_product_key_attributes.ctry_nm, 
                edw_product_key_attributes.ean_upc 
                ORDER BY edw_product_key_attributes.lst_nts DESC
            ) AS row_number 
        FROM edw_product_key_attributes 
        WHERE (
            (edw_product_key_attributes.matl_type_cd::text IN ('FERT'::character varying, 'HALB'::character varying, 'SAPR'::character varying)) 
            AND edw_product_key_attributes.lst_nts IS NOT NULL
        ) 
        GROUP BY 
            edw_product_key_attributes.ctry_nm, 
            edw_product_key_attributes.ean_upc, 
            edw_product_key_attributes.pka_rootcode, 
            edw_product_key_attributes.lst_nts
    ) b 
    ON (
        a.ctry_nm = b.ctry_nm 
        AND a.ean_upc = b.ean_upc 
        AND a.pka_rootcode = b.pka_rootcode 
        AND b.latest_nts_date = a.nts_date 
        AND b.row_number = 1
    ) 
    GROUP BY 
        CASE 
            WHEN (a.ctry_nm)::text = ('APSC Regional'::character varying)::text 
            THEN 'China Personal Care'::character varying 
            ELSE a.ctry_nm 
        END, 
        a.ean_upc, 
        a.gcph_franchise, 
        a.pka_franchise_description, 
        a.gcph_category, 
        a.gcph_subcategory, 
        a.pka_brand_description, 
        a.pka_subbranddesc, 
        a.pka_variantdesc, 
        a.pka_subvariantdesc, 
        a.pka_package, 
        a.pka_rootcode, 
        a.pka_productdesc, 
        a.pka_sizedesc, 
        a.pka_skuiddesc
),
benchmarks_comp1 as
(
    SELECT DISTINCT 
        itg_mds_rg_sku_benchmarks.market, 
        ltrim(
            (itg_mds_rg_sku_benchmarks.comp_upc)::text, 
            ('0'::character varying)::text
        ) AS comp_upc, 
        itg_mds_rg_sku_benchmarks.comp_packsize, 
        itg_mds_rg_sku_benchmarks.valid_from, 
        itg_mds_rg_sku_benchmarks.valid_to, 
        '1'::character varying AS benchmark_flag 
    FROM itg_mds_rg_sku_benchmarks 
    WHERE 
        (itg_mds_rg_sku_benchmarks.market)::text = ('China Personal Care'::character varying)::text
),
benchmarks_jj1 as
(
    SELECT DISTINCT 
        itg_mds_rg_sku_benchmarks.market, 
        ltrim((itg_mds_rg_sku_benchmarks.jj_upc)::text,('0'::character varying)::text) AS jj_upc, 
        itg_mds_rg_sku_benchmarks.jj_packsize, 
        itg_mds_rg_sku_benchmarks.jj_target, 
        itg_mds_rg_sku_benchmarks.variance, 
        itg_mds_rg_sku_benchmarks.valid_from, 
        itg_mds_rg_sku_benchmarks.valid_to, 
        '1'::character varying AS benchmark_flag 
    FROM itg_mds_rg_sku_benchmarks 
    WHERE 
        (itg_mds_rg_sku_benchmarks.market)::text = 'China Personal Care'::character varying
),
asp as
(
    SELECT 
        to_date(base.processing_date::date) AS report_date, 
        base.retailer AS online_store, 
        base.rpc, 
        AVG(ref.observed_price_rmb) AS asp_lcy 
    FROM 
        edw_cn_digital_shelf_data base 
        LEFT JOIN edw_cn_digital_shelf_data ref 
        ON (
            (base.retailer::text = ref.retailer::text) 
            AND (base.rpc::text = ref.rpc::text) 
            AND ref.processing_date < base.processing_date
            AND datediff(week,base.processing_date::date,ref.processing_date::date) > -13
        )
    GROUP BY 
        to_date(base.processing_date::date), 
        base.retailer, 
        base.rpc
),
final as 
(
    SELECT 
        'EDGE' AS data_source, 
        'PRICE TRACKER SKU DATA' AS kpi, 
        rgcust.cluster, 
        CASE 
            WHEN (ecgp.region::text = 'JP'::character varying) 
                AND (upper(ecgp.category::text) = 'FACE CARE'::character varying) 
            THEN 'Japan DCL'::character varying 
            ELSE rgcust.market 
        END AS market, 
        rgcust.channel, 
        rgcust.re AS retail_environment, 
        'E-Commerce Customer' AS parent_customer, 
        NULL AS distributor, 
        CASE 
            WHEN upper(ecgp.manufacturer::text)::character varying(300) IN (
                SELECT DISTINCT 
                    itg_query_parameters.parameter_value AS manufacturer 
                FROM itg_query_parameters 
                WHERE itg_query_parameters.country_code::text = 'APAC'::character varying
                    AND itg_query_parameters.parameter_name::text = 'price_tracker_mfr'::character varying
            )
            THEN 'J&J'::character varying 
            ELSE ecgp.manufacturer 
        END AS manufacturer, 
        upper(ecgp.is_competitor::text)::character varying AS competitor, 
        rgcust.group_customer AS platform, 
        ecgp.online_store AS store, 
        upper(ecgp.dimension7::text)::character varying AS sub_store_1, 
        upper(ecgp.dimension7::text)::character varying AS sub_store_2, 
        ecgp.report_date, 
        cal_dim.cal_wk AS cal_week, 
        cal_dim.fisc_per AS fisc_yr_per, 
        ecgp.category AS source_prod_hier_l1, 
        ecgp.dimension1 AS source_prod_hier_l2, 
        ecgp.brand AS source_prod_hier_l3, 
        upper(lookup.stronghold::text)::character varying AS source_prod_hier_l4, 
        NULL AS source_prod_hier_l5, 
        pka.gcph_franchise AS prod_hier_l1, 
        pka.pka_franchise_description AS prod_hier_l2, 
        upper(
            CASE 
                WHEN lookup.harmonized_category IS NULL 
                THEN ecgp.category 
                ELSE lookup.harmonized_category 
            END::text
        )::character varying AS prod_hier_l3, 
        upper(
            CASE 
                WHEN upper(ecgp.is_competitor::text) = 'TRUE'::character varying 
                THEN ecgp.dimension1 
                ELSE pka.gcph_subcategory 
            END::text
        )::character varying AS prod_hier_l4, 
        upper(
            CASE 
                WHEN upper(ecgp.is_competitor::text) = 'TRUE'::character varying 
                THEN ecgp.brand 
                ELSE pka.pka_brand_description 
            END::text
        )::character varying AS prod_hier_l5, 
        pka.pka_subbranddesc AS prod_hier_l6, 
        pka.pka_variantdesc AS prod_hier_l7, 
        pka.pka_subvariantdesc AS prod_hier_l8, 
        ecgp.product_desc AS prod_hier_l9, 
        regexp_substr(
            upper(pka.pka_package::text), 
            '[A-Z]+'::character varying
        )::character varying AS prod_hier_l10, 
        pka.pka_rootcode, 
        pka.pka_productdesc, 
        NULL AS material_number, 
        ltrim(
            CASE 
                WHEN rgp.ean IS NULL 
                    OR rgp.ean::text = ''::character varying 
                THEN CASE 
                        WHEN ecgp.upc IS NULL 
                        THEN 'NA'::character varying 
                        ELSE ecgp.upc 
                    END::text 
                ELSE rgp.ean 
            END::character varying::text, 
            '0'::character varying
        )::character varying AS ean_upc, 
        ecgp.rpc, 
        CASE 
            WHEN regexp_substr(
                regexp_substr(
                    upper(pka.pka_sizedesc::text), 
                    '[0-9]+'::character varying
                ), 
                '[0-9]+'::character varying
            ) = ''::character varying
            THEN NULL::character varying::text 
            ELSE regexp_substr(
                regexp_substr(
                    upper(pka.pka_sizedesc::text), 
                    '[0-9]+'::character varying
                ), 
                '[0-9]+'::character varying
            ) 
        END::integer AS base_packsize, 
        regexp_substr(
            upper(pka.pka_package::text), 
            '[0-9]+'::character varying
        )::integer AS packsize_multiplier, 
        benchmarks_jj.jj_packsize AS market_input_packsize, 
        CASE 
            WHEN ecgp.is_competitor::text = 'False'::character varying 
                AND benchmarks_jj.jj_packsize IS NOT NULL 
            THEN benchmarks_jj.jj_packsize::double precision 
            WHEN ecgp.is_competitor::text = 'False'::character varying 
                AND CASE 
                        WHEN regexp_substr(
                            regexp_substr(
                                upper(pka.pka_sizedesc::text), 
                                '[0-9]+'::character varying
                            ), 
                            '[0-9]+'::character varying
                        ) = ''::character varying 
                        THEN NULL::character varying::text 
                        ELSE regexp_substr(
                            regexp_substr(
                                upper(pka.pka_sizedesc::text), 
                                '[0-9]+'::character varying
                            ), 
                            '[0-9]+'::character varying
                        ) 
                    END::double precision IS NOT NULL 
            THEN (
                CASE 
                    WHEN regexp_substr(regexp_substr(
                            upper(pka.pka_sizedesc::text), 
                            '[0-9]+'::character varying
                        ), 
                        '[0-9]+'::character varying
                    ) = ''::character varying 
                    THEN NULL::character varying::text 
                    ELSE regexp_substr(regexp_substr(
                            upper(pka.pka_sizedesc::text), 
                            '[0-9]+'::character varying
                        ), 
                        '[0-9]+'::character varying
                    ) 
                END::double precision
            ) * regexp_substr(
                upper(pka.pka_package::text), 
                '[0-9]+'::character varying
            )::double precision 
            WHEN ecgp.is_competitor::text = 'True'::character varying 
                AND benchmarks_comp.comp_packsize IS NOT NULL 
            THEN benchmarks_comp.comp_packsize::double precision 
            ELSE (
                CASE 
                    WHEN regexp_substr(regexp_substr(
                            upper(ecgp.product_desc::text), 
                            '[0-9]+'::character varying
                        ), 
                        '[0-9]+'::character varying
                    ) = ''::character varying 
                    THEN NULL::character varying::text 
                    ELSE regexp_substr(regexp_substr(
                            upper(ecgp.product_desc::text), 
                            '[0-9]+'::character varying
                        ), 
                        '[0-9]+'::character varying
                    ) 
                END::double precision * 
                CASE 
                    WHEN regexp_count(
                        upper(ecgp.product_desc::text), 
                        '[0-9]+'::character varying
                    )::double precision = 1::double precision 
                    THEN 1.0 
                    ELSE NULL::numeric 
                END::double precision
            ) 
        END AS final_packsize, 
        CASE 
            WHEN ecgp.dimension8 IS NOT NULL 
                AND ecgp.dimension8::text <> 'NO'::character varying 
                AND ecgp.dimension8::text <> 'UNKNOWN'::character varying 
                AND ecgp.dimension8::text <> 'UNCATEGORIZED'::character varying 
            THEN '1'::character varying 
            ELSE '0'::character varying 
        END AS msl_flag, 
        CASE 
            WHEN upper(greenlight.greenlight_sku_flag::text) = 'Y'::character varying 
            THEN 1 
            ELSE 0 
        END AS greenlight_flag, 
        crncy.from_crncy, 
        crncy.to_crncy, 
        NULL AS exch_rate_version, 
        ecgp.msrp AS msrp_lcy, 
        ecgp.msrp * crncy.ex_rt AS msrp_usd, 
        CASE 
            WHEN ecgp.region::text = 'KR'::character varying 
                AND upper(ecgp.online_store::text) LIKE '%COUPANG%'::character varying 
            THEN ecgp.observed_price 
            WHEN rolling_mrp1.mrp IS NULL 
            THEN rolling_mrp2.mrp 
            ELSE rolling_mrp1.mrp 
        END AS mrp_lcy, 
        CASE 
            WHEN ecgp.region::text = 'KR'::character varying 
                AND upper(ecgp.online_store::text) LIKE '%COUPANG%'::character varying 
            THEN ecgp.observed_price 
            WHEN rolling_mrp1.mrp IS NULL 
            THEN rolling_mrp2.mrp 
            ELSE rolling_mrp1.mrp 
        END * crncy.ex_rt AS mrp_usd, 
        CASE 
            WHEN (
                (ecgp.region)::text = ('KR'::character varying)::text
                AND upper((ecgp.online_store)::text) LIKE ('%COUPANG%'::character varying)::text
            ) THEN 'Definite'::character varying
            WHEN rolling_mrp1.mrp IS NULL THEN rolling_mrp2.mrp_type
            ELSE rolling_mrp1.mrp_type
        END AS mrp_type,
        rolling_asp.asp AS asp_lcy,
        rolling_asp.asp * (crncy.ex_rt)::double precision AS asp_usd,
        CASE WHEN ((ecgp.region)::text = ('KR'::character varying)::text
                AND upper((ecgp.online_store)::text) LIKE ('%COUPANG%'::character varying)::text
                AND regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying) <> ''::character varying
                AND regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying) IS NOT NULL) 
                THEN (regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying)::double precision)
            ELSE (ecgp.observed_price)::double precision
        END AS observed_price_lcy,
        CASE WHEN ((ecgp.region)::text = ('KR'::character varying)::text
                AND upper((ecgp.online_store)::text) LIKE ('%COUPANG%'::character varying)::text
                AND regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying) <> ''::character varying
                AND regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying) IS NOT NULL) 
                THEN (regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying)::double precision)
            ELSE (ecgp.observed_price)::double precision
        END * (crncy.ex_rt)::double precision AS observed_price_usd,
        NULL AS bcp_lcy,
        NULL AS bcp_usd,
        benchmarks_jj.jj_target AS target_index,
        benchmarks_jj.variance,
        CASE 
            WHEN upper((ecgp.is_competitor)::text) = ('FALSE'::character varying)::text THEN benchmarks_jj.benchmark_flag
            WHEN upper((ecgp.is_competitor)::text) = ('TRUE'::character varying)::text THEN benchmarks_comp.benchmark_flag
            ELSE '0'::character varying
        END AS benchmark_flag,
        ecgp.is_on_promotion AS cust_promo_flag,
        CASE WHEN (ecgp.observed_price)::double precision = CASE WHEN ((ecgp.region)::text = ('KR'::character varying)::text AND upper((ecgp.online_store)::text) LIKE ('%COUPANG%'::character varying)::text AND regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying) <> ''::character varying AND regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying) IS NOT NULL) 
            THEN (regexp_substr(split_part(regexp_substr(replace((ecgp.promo_text)::text, ','::character varying, ''::character varying),'^[0-9]+[%]\\s[|]\\s[0-9]+[?]'::text),' | '::character varying,2),'^[0-9]+'::character varying)::double precision)
            WHEN rolling_mrp1.mrp IS NULL THEN (rolling_mrp2.mrp)::double precision 
            ELSE (rolling_mrp1.mrp)::double precision END
            THEN '0'::character varying ELSE '1'::character varying
        END::integer AS price_promo_flag,
        ecgp.promo_text AS promo_desc,
        pka.pka_skuiddesc AS additional_info
    FROM ecgp LEFT JOIN rgcust 
    ON (((((ecgp.region):: text = (rgcust.cntry_cd):: text) 
    AND ((ecgp.online_store):: text = (rgcust.online_store):: text))
    AND (upper((rgcust.data_provider):: text) = ('CLAVIS' :: character varying):: text)))

    LEFT JOIN rgp ON ((((ecgp.rpc):: text = (rgp.rpc):: text)
        AND (upper((rgp.market):: text) = upper((rgcust.market):: text))))

    LEFT JOIN itg_lookup_category_mapping lookup ON ((((upper((rgcust.market):: text) = upper((lookup.market):: text))
    AND (upper((CASE WHEN ((ecgp.category IS NULL) OR ((ecgp.category):: text = ('' :: character varying):: text)) THEN 'NA' :: character varying ELSE ecgp.category END):: text) = upper((CASE WHEN ((lookup.input_category IS NULL) OR ((lookup.input_category):: text = ('' :: character varying):: text)) THEN 'NA' :: character varying ELSE lookup.input_category END):: text)))
    AND (upper((CASE WHEN ((ecgp.dimension1 IS NULL) OR ((ecgp.dimension1):: text = ('' :: character varying):: text)) THEN 'NA' :: character varying ELSE ecgp.dimension1 END):: text) = upper((CASE WHEN ((lookup.input_sub_category IS NULL) OR ((lookup.input_sub_category):: text = ('' :: character varying):: text)) THEN 'NA' :: character varying ELSE lookup.input_sub_category END):: text))))

    LEFT JOIN pka ON ((((ltrim(((CASE WHEN ((rgp.ean IS NULL) OR (((rgp.ean):: character varying):: text = ('' :: character varying):: text)) THEN (CASE WHEN (ecgp.upc IS NULL) THEN 'NA' :: character varying ELSE ecgp.upc END):: text ELSE rgp.ean END):: character varying):: text,('0' :: character varying):: text) = ltrim((pka.ean_upc):: text,('0' :: character varying):: text)) 
    AND (upper((pka.ctry_nm):: text) = upper((rgcust.market):: text)))AND (pka.row_number = 1)))

    LEFT JOIN benchmarks_jj ON (((((ltrim(((CASE WHEN ((rgp.ean IS NULL) OR (((rgp.ean):: character varying):: text = ('' :: character varying):: text)) THEN (CASE WHEN (ecgp.upc IS NULL) THEN 'NA' :: character varying ELSE ecgp.upc END):: text ELSE rgp.ean END):: character varying):: text,('0' :: character varying):: text) = ltrim(benchmarks_jj.jj_upc,('0' :: character varying):: text)) 
    AND (upper((rgcust.market):: text) = upper((benchmarks_jj.market):: text)))
    AND (ecgp.report_date >= benchmarks_jj.valid_from))
    AND (ecgp.report_date <= benchmarks_jj.valid_to)))

    LEFT JOIN benchmarks_comp ON ((((ltrim(((CASE WHEN((rgp.ean IS NULL) OR (((rgp.ean)::character varying)::text=(''::character varying)::text)) THEN (CASE WHEN(ecgp.upc IS NULL) THEN'NA'::character varying ELSE ecgp.upc END)::text ELSE rgp.ean END)::character varying)::text,('0'::character varying)::text)=ltrim(benchmarks_comp.comp_upc,('0'::character varying)::text))
    AND(upper((rgcust.market)::text)=upper((benchmarks_comp.market)::text))
    AND(ecgp.report_date>=benchmarks_comp.valid_from))
    AND(ecgp.report_date<=benchmarks_comp.valid_to)))

    LEFT JOIN greenlight ON (((ltrim(((CASE WHEN ((rgp.ean IS NULL) OR (((rgp.ean)::character varying)::text = (''::character varying)::text)) THEN (CASE WHEN (ecgp.upc IS NULL) THEN 'NA'::character varying ELSE ecgp.upc END)::text ELSE rgp.ean END)::character varying)::text, ('0'::character varying)::text) = ltrim(greenlight.ean_num, ('0'::character varying)::text)) 
    AND (upper((rgcust.cluster)::text) = upper((greenlight.cluster)::text))
    AND (upper((rgcust.market)::text) = upper((greenlight.ctry_nm)::text))))

    LEFT JOIN edw_calendar_dim cal_dim ON cal_dim.cal_day = ecgp.report_date

    LEFT JOIN crncy ON (upper((rgcust.market)::text) = upper((crncy.market)::text))

    LEFT JOIN rolling_asp ON (((ecgp.report_date=rolling_asp.report_date)
    AND ((ecgp.region)::text=(rolling_asp.region)::text)
    AND ((ecgp.online_store)::text=(rolling_asp.online_store)::text)
    AND ((ecgp.rpc)::text=(rolling_asp.rpc)::text)))

    LEFT JOIN rolling_mrp1 ON (((ecgp.report_date=rolling_mrp1.report_date)
    AND ((ecgp.region)::text=(rolling_mrp1.region)::text)
    AND ((rgcust.group_customer)::text=(rolling_mrp1.platform)::text)
    AND ((ecgp.rpc)::text=(rolling_mrp1.rpc)::text)
    AND ((ecgp.online_store)::text=(rolling_mrp1.online_store)::text)))

    LEFT JOIN rolling_mrp2 ON (((ecgp.report_date=rolling_mrp2.report_date)
    AND ((ecgp.region)::text=(rolling_mrp2.region)::text)
    AND ((ecgp.online_store)::text=(rolling_mrp2.online_store)::text)
    AND ((ecgp.rpc)::text=(rolling_mrp2.rpc)::text)))

    WHERE ((((date_part(year,ecgp.report_date)>((date_part(year,convert_timezone('UTC',current_timestamp())::date)-2)))
    AND (upper((CASE WHEN(lookup.harmonized_category IS NULL) THEN ecgp.category ELSE lookup.harmonized_category END)::text) IS NOT NULL)) 
    AND ((ecgp.region)::text<>'CN'::character varying::text)))

    UNION ALL

    SELECT
    (upper((mkt_mirror.supplier)::text))::character varying AS data_source,
    'PRICE TRACKER SKU DATA' AS kpi,
    mkt_mirror.cluster,
    mkt_mirror.market,
    'Offline Customer' AS channel,
    'Offline Customer' AS retail_environment,
    mkt_mirror.parent_customer,
    'Offline Customer' AS distributor,
    mkt_mirror.manufacturer,
    mkt_mirror.is_competitor AS competitor,
    mkt_mirror.platform,
    'Offline Customer' AS store,
    'Offline Customer' AS sub_store_1,
    'Offline Customer' AS sub_store_2,
    mkt_mirror.report_date,
    calendar.cal_wk AS cal_week,
    ((((calendar.cal_yr)::character varying)::text || ('0'::character varying)::text) || right((calendar.fisc_per)::character varying::text, 2))::integer AS fisc_yr_per,
    mkt_mirror.source_prod_hier_l1,
    mkt_mirror.source_prod_hier_l2,
    mkt_mirror.source_prod_hier_l3,
    (upper((cat_map.stronghold)::text))::character varying AS source_prod_hier_l4,
    mkt_mirror.source_prod_hier_l5,
    pka.gcph_franchise AS prod_hier_l1,
    pka.pka_franchise_description AS prod_hier_l2,
    (upper((cat_map.harmonized_category)::text))::character varying AS prod_hier_l3,
    pka.gcph_subcategory AS prod_hier_l4,
    (upper((pka.pka_brand_description)::text))::character varying AS prod_hier_l5,
    pka.pka_subbranddesc AS prod_hier_l6,
    pka.pka_variantdesc AS prod_hier_l7,
    pka.pka_subvariantdesc AS prod_hier_l8,
    mkt_mirror.product AS prod_hier_l9,
    (regexp_substr(upper((pka.pka_package)::text), '[A-Z]+'::character varying::text))::character varying AS prod_hier_l10,
    pka.pka_rootcode,
    pka.pka_productdesc,
    NULL AS material_number,
    (mkt_mirror.ean_upc)::character varying AS ean_upc,
    mkt_mirror.utag AS rpc,
    CASE WHEN (
        CASE WHEN (
        regexp_substr(regexp_substr(upper((pka.pka_sizedesc)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text) = ''::character varying::text
        ) THEN NULL::character varying::text
        ELSE regexp_substr(regexp_substr(upper((pka.pka_sizedesc)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text)
        END IS NOT NULL
    ) THEN (
        (CASE WHEN (
        regexp_substr(regexp_substr(upper((pka.pka_sizedesc)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text) = ''::character varying::text
        ) THEN NULL::character varying::text
        ELSE regexp_substr(regexp_substr(upper((pka.pka_sizedesc)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text)
        END)::numeric(18, 0) *
        (regexp_substr(upper((pka.pka_package)::text), '[0-9]+'::character varying::text)::integer::numeric)::numeric(18, 0)
    )::double precision
    WHEN (mkt_mirror.source_packsize IS NOT NULL) THEN mkt_mirror.source_packsize
    ELSE NULL::double precision
    END AS base_packsize,
    (regexp_substr(upper((pka.pka_package)::text), '[0-9]+'::character varying::text))::integer AS packsize_multiplier,
    benchmarks_jj.jj_packsize AS market_input_packsize,
    CASE WHEN (
        (mkt_mirror.is_competitor)::text = 'FALSE'::character varying::text
        AND benchmarks_jj.jj_packsize IS NOT NULL
    ) THEN (benchmarks_jj.jj_packsize)::double precision
    WHEN (
        (mkt_mirror.is_competitor)::text = 'FALSE'::character varying::text
        AND (CASE WHEN (
        regexp_substr(regexp_substr(upper((pka.pka_sizedesc)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text) = ''::character varying::text
        ) THEN NULL::character varying::text
        ELSE regexp_substr(regexp_substr(upper((pka.pka_sizedesc)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text)
        END)::double precision IS NOT NULL
    ) THEN (
        (CASE WHEN (
        regexp_substr(regexp_substr(upper((pka.pka_sizedesc)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text) = ''::character varying::text
        ) THEN NULL::character varying::text
        ELSE regexp_substr(regexp_substr(upper((pka.pka_sizedesc)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text)
        END)::double precision *
        regexp_substr(upper((pka.pka_package)::text), '[0-9]+'::character varying::text)::double precision
    )
    WHEN (
        (mkt_mirror.is_competitor)::text = 'TRUE'::character varying::text
        AND benchmarks_comp.comp_packsize IS NOT NULL
    ) THEN (benchmarks_comp.comp_packsize)::double precision
    WHEN (mkt_mirror.source_packsize IS NOT NULL) THEN mkt_mirror.source_packsize
    ELSE (
        (CASE WHEN (
        regexp_substr(regexp_substr(upper((mkt_mirror.product)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text) = ''::character varying::text
        ) THEN NULL::character varying::text
        ELSE regexp_substr(regexp_substr(upper((mkt_mirror.product)::text), '[0-9]+'::character varying::text), '[0-9]+'::character varying::text)
        END)::double precision *
        CASE WHEN (
        regexp_count(upper((mkt_mirror.product)::text), '[0-9]+'::character varying::text)::double precision = 1::double precision
        ) THEN 1.0
        ELSE NULL::numeric::numeric(18, 0)
        END
    )::double precision
    END AS final_packsize,
    NULL AS msl_flag,
    CASE WHEN (upper((greenlight.greenlight_sku_flag)::text) = 'Y'::character varying::text) THEN 1
    ELSE CASE WHEN ((mkt_mirror.is_competitor)::text = 'TRUE'::character varying::text) THEN NULL::integer ELSE 0 END
    END AS greenlight_flag,
    crncy.from_crncy,
    crncy.to_crncy,
    NULL AS exch_rate_version,
    NULL AS msrp_lcy,
    NULL AS msrp_usd,
    NULL AS mrp_lcy,
    NULL AS mrp_usd,
    NULL AS mrp_type,
    mkt_mirror.asp_lcy,
    mkt_mirror.asp_usd,
    NULL AS observed_price_lcy,
    NULL AS observed_price_usd,
    NULL AS bcp_lcy,
    NULL AS bcp_usd,
    benchmarks_jj.jj_target AS target_index,
    benchmarks_jj.variance,
    CASE WHEN ((mkt_mirror.is_competitor)::text = 'FALSE'::character varying::text) THEN benchmarks_jj.benchmark_flag
    ELSE benchmarks_comp.benchmark_flag
    END AS benchmark_flag,
    NULL AS cust_promo_flag,
    NULL AS price_promo_flag,
    NULL AS promo_desc,
    NULL AS additional_info
    FROM mkt_mirror LEFT JOIN pka1 pka ON (((upper((pka.ctry_nm):: text) = upper((CASE WHEN ((mkt_mirror.market):: text = ('China Selfcare' :: character varying):: text) THEN 'China Personal Care' :: character varying WHEN ((mkt_mirror.market):: text = ('Japan DCL' :: character varying):: text) THEN 'Japan' :: character varying ELSE mkt_mirror.market END):: text) ) 
    AND ((pka.ean_upc):: text = (CASE WHEN ((upper((mkt_mirror.manufacturer):: text)):: character varying(300) IN (SELECT DISTINCT itg_query_parameters.parameter_value AS manufacturer FROM itg_query_parameters  WHERE (((itg_query_parameters.country_code):: text = ('APAC' :: character varying):: text) 
    AND ((itg_query_parameters.parameter_name):: text = ('price_tracker_mfr' :: character varying):: text)))) THEN (mkt_mirror.ean_upc):: character varying ELSE 'NA' :: character varying END):: text)))

    LEFT JOIN crncy ON (((upper((CASE WHEN ((mkt_mirror.market)::text = ('China Selfcare'::character varying)::text) THEN 'China Personal Care'::character varying WHEN ((mkt_mirror.market)::text = ('Japan DCL'::character varying)::text) THEN 'Japan'::character varying ELSE crncy.market END)::text) = upper((mkt_mirror.market)::text))))

    LEFT JOIN greenlight ON (((ltrim(greenlight.ean_num, ('0'::character varying)::text) = (CASE WHEN (mkt_mirror.ean_upc IS NULL) OR (mkt_mirror.ean_upc = (''::character varying)::text) THEN ('NA'::character varying)::text ELSE mkt_mirror.ean_upc END)::character varying)::text) 
    AND ((mkt_mirror.market)::text = upper((greenlight.ctry_nm)::text)))

    LEFT JOIN benchmarks_comp ON (((((((CASE WHEN ((mkt_mirror.ean_upc IS NULL) OR (mkt_mirror.ean_upc = ('' :: character varying):: text)) THEN ('NA' :: character varying):: text ELSE mkt_mirror.ean_upc END):: character varying):: text = ltrim(benchmarks_comp.comp_upc,('0' :: character varying):: text)) 
    AND (upper(trim(mkt_mirror.market):: text) = upper(trim(benchmarks_comp.market):: text))) 
    AND (mkt_mirror.report_date >= benchmarks_comp.valid_from))
    AND (mkt_mirror.report_date <= benchmarks_comp.valid_to)))

    LEFT JOIN benchmarks_jj ON (((((((CASE WHEN ((mkt_mirror.ean_upc IS NULL) OR (mkt_mirror.ean_upc = ('' :: character varying):: text)) THEN ('NA' :: character varying):: text ELSE mkt_mirror.ean_upc END):: character varying):: text = ltrim( benchmarks_jj.jj_upc, ('0' :: character varying):: text))
    AND (upper(trim(mkt_mirror.market):: text) = upper(trim(benchmarks_jj.market):: text)))
    AND (mkt_mirror.report_date >= benchmarks_jj.valid_from))
    AND (mkt_mirror.report_date <= benchmarks_jj.valid_to)))

    LEFT JOIN itg_lookup_category_mapping cat_map ON ((((upper((cat_map.market)::text) = upper((mkt_mirror.market)::text)) 
    AND (upper((cat_map.input_category)::text) = upper((mkt_mirror.source_prod_hier_l1)::text))) 
    AND (upper((CASE WHEN (cat_map.input_sub_category IS NULL) OR ((cat_map.input_sub_category)::text = (''::character varying)::text) THEN 'NA'::character varying ELSE cat_map.input_sub_category END)::text) = upper((CASE WHEN (mkt_mirror.source_prod_hier_l2 IS NULL) OR ((mkt_mirror.source_prod_hier_l2)::text = (''::character varying)::text) THEN 'NA'::character varying ELSE mkt_mirror.source_prod_hier_l2 END)::text))))

    LEFT JOIN edw_calendar_dim calendar ON mkt_mirror.report_date = calendar.cal_day


    UNION ALL

    SELECT 
    'Yimian Digital Shelf' AS data_source, 
    'PRICE TRACKER SKU DATA' AS kpi, 
    cn_digital_shelf.cluster, 
    cn_digital_shelf.market, 
    cn_digital_shelf.channel, 
    cn_digital_shelf.retail_environment, 
    'E-Commerce Customer' AS parent_customer, 
    NULL AS distributor, 
    cn_digital_shelf.manufacturer, 
    (cn_digital_shelf.is_competitor)::character varying AS competitor, 
    cn_digital_shelf.platform, 
    cn_digital_shelf.online_store AS store, 
    cn_digital_shelf.substore AS sub_store_1, 
    cn_digital_shelf.substore_name_en AS sub_store_2, 
    cn_digital_shelf.report_date, 
    calendar.cal_wk AS cal_week, 
    ((((calendar.cal_yr)::character varying::text || ('0'::character varying)::text) || right(((calendar.cal_mo_1)::character varying)::text, 2)))::integer AS fisc_yr_per, 
    cn_digital_shelf.category AS source_prod_hier_l1, 
    cn_digital_shelf.subcat1 AS source_prod_hier_l2, 
    cn_digital_shelf.subcat2 AS source_prod_hier_l3, 
    (
        upper(
        (cat_map.stronghold)::text
        )
    )::character varying AS source_prod_hier_l4, 
    cn_digital_shelf.brand AS source_prod_hier_l5, 
    pka.gcph_franchise AS prod_hier_l1, 
    pka.pka_franchise_description AS prod_hier_l2, 
    (
        upper(
        (cat_map.harmonized_category)::text
        )
    )::character varying AS prod_hier_l3, 
    pka.gcph_subcategory AS prod_hier_l4, 
    (
        upper(
        (pka.pka_brand_description)::text
        )
    )::character varying AS prod_hier_l5, 
    pka.pka_subbranddesc AS prod_hier_l6, 
    pka.pka_variantdesc AS prod_hier_l7, 
    pka.pka_subvariantdesc AS prod_hier_l8, 
    cn_digital_shelf.std_sku_name_en AS prod_hier_l9, 
    (
        CASE WHEN (
        regexp_substr(
            upper(
            (pka.pka_package)::text
            ), 
            ('[A-Z]+'::character varying)::text
        ) IS NULL
        ) THEN (
        CASE WHEN (
            (cn_digital_shelf.is_bundle)::text = ('TRUE'::character varying)::text
        ) THEN 'Bundle'::character varying ELSE NULL::character varying END
        )::text ELSE regexp_substr(
        upper(
            (pka.pka_package)::text
        ), 
        ('[A-Z]+'::character varying)::text
        ) END
    )::character varying AS prod_hier_l10, 
    pka.pka_rootcode, 
    pka.pka_productdesc, 
    cn_digital_shelf.materialnumber AS material_number, 
    cn_digital_shelf.ean_upc, 
    cn_digital_shelf.rpc, 
    cn_digital_shelf.base_packsize, 
    cn_digital_shelf.packsize_multiplier, 
    CASE WHEN (
        cn_digital_shelf.is_competitor = ('TRUE'::character varying)::text
    ) THEN benchmarks_comp.comp_packsize ELSE benchmarks_jj.jj_packsize END AS market_input_packsize, 
    CASE WHEN (
        (
        cn_digital_shelf.is_competitor = ('TRUE'::character varying)::text
        ) AND (
        benchmarks_comp.comp_packsize IS NOT NULL
        )
    ) THEN benchmarks_comp.comp_packsize WHEN (
        (
        cn_digital_shelf.is_competitor = ('FALSE'::character varying)::text
        ) AND (
        benchmarks_jj.jj_packsize IS NOT NULL
        )
    ) THEN benchmarks_jj.jj_packsize ELSE cn_digital_shelf.final_packsize END AS final_packsize, 
    NULL AS msl_flag, 
    CASE WHEN (
        upper(
        (greenlight.greenlight_sku_flag)::text
        ) = ('Y'::character varying)::text
    ) THEN 1 ELSE NULL::integer END AS greenlight_flag, 
    crncy.from_crncy, 
    crncy.to_crncy, 
    NULL AS exch_rate_version, 
    cn_digital_shelf.msrp_lcy, 
    (
        cn_digital_shelf.msrp_lcy * crncy.ex_rt
    ) AS msrp_usd, 
    cn_digital_shelf.mrp_lcy, 
    (
        cn_digital_shelf.mrp_lcy * crncy.ex_rt
    ) AS mrp_usd, 
    'Definite' AS mrp_type, 
    asp.asp_lcy, 
    (asp.asp_lcy * crncy.ex_rt) AS asp_usd, 
    cn_digital_shelf.observed_price_lcy, 
    (
        cn_digital_shelf.observed_price_lcy * crncy.ex_rt
    ) AS observed_price_usd, 
    cn_digital_shelf.bcp_lcy, 
    (
        cn_digital_shelf.bcp_lcy * crncy.ex_rt
    ) AS bcp_usd, 
    benchmarks_jj.jj_target AS target_index, 
    benchmarks_jj.variance, 
    CASE WHEN (
        cn_digital_shelf.is_competitor = ('TRUE'::character varying)::text
    ) THEN benchmarks_comp.benchmark_flag ELSE benchmarks_jj.benchmark_flag END AS benchmark_flag, 
    cn_digital_shelf.cust_promo_flag, 
    cn_digital_shelf.price_promo_flag, 
    (cn_digital_shelf.promo_text)::character varying AS promo_desc, 
    NULL AS additional_info 
    FROM cn_digital_shelf LEFT JOIN pka2 pka ON ((upper((pka.ctry_nm)::text)=upper((cn_digital_shelf.market)::text) 
    AND pka.ean_upc=(CASE WHEN (cn_digital_shelf.is_competitor=('FALSE'::character varying)::text) THEN cn_digital_shelf.ean_upc ELSE 'NA'::character varying END)::text))

    LEFT JOIN crncy1 crncy ON ((upper((crncy.market)::text)=upper((cn_digital_shelf.market)::text)))

    LEFT JOIN greenlight ON (((ltrim(greenlight.ean_num,('0'::character varying)::text)=CASE WHEN((cn_digital_shelf.ean_upc IS NULL) OR ((cn_digital_shelf.ean_upc)::text=(''::character varying)::text)) THEN 'NA'::character varying ELSE cn_digital_shelf.ean_upc END)::text) 
    AND ((cn_digital_shelf.market)::text=upper((greenlight.ctry_nm)::text)))

    LEFT JOIN benchmarks_comp1 benchmarks_comp ON ((((CASE WHEN((cn_digital_shelf.ean_upc IS NULL) OR ((cn_digital_shelf.ean_upc)::text = (''::character varying)::text)) THEN 'NA'::character varying ELSE cn_digital_shelf.ean_upc END)::text = ltrim(benchmarks_comp.comp_upc,('0'::character varying)::text)) 
    AND (upper((cn_digital_shelf.market)::text)=upper((benchmarks_comp.market)::text)) 
    AND (cn_digital_shelf.report_date>=benchmarks_comp.valid_from)) 
    AND (cn_digital_shelf.report_date<=benchmarks_comp.valid_to))

    LEFT JOIN benchmarks_jj1 benchmarks_jj ON ((((CASE WHEN((cn_digital_shelf.ean_upc IS NULL) OR ((cn_digital_shelf.ean_upc)::text = (''::character varying)::text)) THEN 'NA'::character varying ELSE cn_digital_shelf.ean_upc END)::text = ltrim(benchmarks_jj.jj_upc,('0'::character varying)::text)) 
    AND (upper((cn_digital_shelf.market)::text) = upper((benchmarks_jj.market)::text)) 
    AND (cn_digital_shelf.report_date>=benchmarks_jj.valid_from)) 
    AND (cn_digital_shelf.report_date<=benchmarks_jj.valid_to))

    LEFT JOIN itg_lookup_category_mapping cat_map ON ((((upper((cat_map.market)::text) = upper((cn_digital_shelf.market)::text)) 
    AND (upper((cat_map.input_category)::text) = upper((cn_digital_shelf.subcat1)::text)) 
    AND (upper((cat_map.input_sub_category)::text) = upper((cn_digital_shelf.subcat2)::text)))))

    LEFT JOIN edw_calendar_dim calendar ON cn_digital_shelf.report_date = calendar.cal_day

    LEFT JOIN asp ON (((cn_digital_shelf.report_date=asp.report_date) 
    AND ((cn_digital_shelf.online_store)::text=(asp.online_store)::text) 
    AND ((cn_digital_shelf.rpc)::text=(asp.rpc)::text)))
)
select * from final