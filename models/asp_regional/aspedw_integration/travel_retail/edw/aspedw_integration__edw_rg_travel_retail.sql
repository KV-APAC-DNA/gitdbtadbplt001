--Import CTE
with itg_rg_travel_retail_sellout as (
    select * from {{ref('aspitg_integration__itg_rg_travel_retail_sellout')}}
),
v_intrm_reg_crncy_exch_fiscper as (
    select * from {{ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper')}}
),
itg_rg_travel_retail_channel_mapping as(
    select * from {{ref('aspitg_integration__itg_rg_travel_retail_channel_mapping')}}
),
itg_rg_travel_retail_product_mapping as (
    select * from {{ref('aspitg_integration__itg_rg_travel_retail_product_mapping')}}
),
edw_material_dim as (
    select * from {{ref('aspedw_integration__edw_material_dim')}}
),
edw_copa_trans_fact as (
    select * from {{ref('aspedw_integration__edw_copa_trans_fact')}}
),
edw_customer_base_dim as (
    select * from {{ref('aspedw_integration__edw_customer_base_dim')}}
),
edw_customer_sales_dim as (
    select * from {{ref('aspedw_integration__edw_customer_sales_dim')}}
),
edw_code_descriptions as (
    select * from {{ref('aspedw_integration__edw_code_descriptions')}}
),
itg_rg_travel_retail_nts_target as (
    select * from {{ref('aspitg_integration__itg_rg_travel_retail_nts_target')}}
),

--Logical CTE
sellout as (
    SELECT 'SELLOUT' AS IDENTIFIER,
            trs.ctry_cd,
            trs.crncy_cd,
            CASE
                WHEN UPPER(trs.ctry_cd) = 'KR' THEN 'Korea'
                WHEN UPPER(trs.ctry_cd) = 'HK' THEN 'Hong Kong'
                WHEN UPPER(trs.ctry_cd) = 'CN' THEN 'China'
                WHEN UPPER(trs.ctry_cd) = 'SG' THEN 'Singapore'
                WHEN UPPER(trs.ctry_cd) = 'TW' THEN 'Taiwan'
                WHEN UPPER(trs.ctry_cd) = 'CM' THEN 'Cambodia'
            END AS country_name,
            UPPER(trs.retailer_name) AS retailer_name,
            trs.year,
            trs.month,
            trs.year_month,
            UPPER(trs.dcl_code) AS dcl_code,
            dc.sap_code AS matl_num,
            pk.pka_sub_brand_cd AS sub_brand,
            pk.pka_sub_brand_desc AS sub_brand_desc,
            pk.pka_variant_cd AS variant,
            pk.pka_variant_desc AS variant_desc,
            pk.pka_sub_variant_cd AS sub_variant,
            pk.pka_sub_variant_desc AS sub_variant_desc,
            pk.pka_ingredient_cd AS ingredient,
            pk.pka_ingredient_desc AS ingredient_desc,
            pk.pka_cover_cd AS cover,
            pk.pka_cover_desc AS cover_desc,
            pk.pka_form_cd AS form,
            pk.pka_form_desc AS form_desc,
            pk.pka_product_key_description AS product_key_desc,
            'NA' AS target_type,
            UPPER(trs.door_name) AS doors,
            cm.cust_num,
            cm.sales_location,
            cm.sales_channel,
            dc.srp_usd,
            CASE
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'CHINA NATIONAL SERVICE CORPORATION'
                AND UPPER(trs.door_name) = 'SYDT'
                AND cm.sales_location = 'Downtown' THEN COALESCE(SUM(trs.store_sls_qty), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'CHINA NATIONAL SERVICE CORPORATION'
                AND UPPER(trs.door_name) = 'SYDT'
                AND cm.sales_location = 'E-commerce' THEN COALESCE(
                    SUM(trs.ecommerce_sls_qty + trs.membership_sls_qty),
                    0
                )
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'DUFRY'
                AND UPPER(trs.door_name) = 'DUFRY HAINAN'
                AND cm.sales_location = 'Downtown' THEN COALESCE(SUM(trs.store_sls_qty), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'DUFRY'
                AND UPPER(trs.door_name) = 'DUFRY HAINAN'
                AND cm.sales_location = 'E-commerce' THEN COALESCE(SUM(trs.ecommerce_sls_qty), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'DFS'
                AND UPPER(trs.door_name) = 'HNN MH OFFLINE STORE 0904'
                AND cm.sales_location = 'Downtown' THEN COALESCE(SUM(trs.store_sls_qty), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'DFS'
                AND UPPER(trs.door_name) = 'HAINAN E-SHOP 0903'
                AND cm.sales_location = 'E-commerce' THEN COALESCE(SUM(trs.ecommerce_sls_qty), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'LAGADERE'
                AND UPPER(trs.door_name) = 'LAGADERE HAINAN'
                AND cm.sales_location = 'Downtown' THEN COALESCE(SUM(trs.store_sls_qty), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'LAGADERE'
                AND UPPER(trs.door_name) = 'LAGADERE HAINAN'
                AND cm.sales_location = 'E-commerce' THEN COALESCE(SUM(trs.ecommerce_sls_qty), 0)
                ELSE COALESCE(SUM(trs.sls_qty), 0)
            END AS sellout_qty,
            CASE
                WHEN UPPER(trs.ctry_cd) = 'SG' THEN COALESCE(SUM(trs.sls_amt * exch.ex_rt), 0)
                WHEN UPPER(trs.ctry_cd) IN ('CM', 'CN')
                AND UPPER(trs.retailer_name) = 'CDFG' THEN COALESCE(SUM(trs.sls_qty * dc.srp_usd), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'CHINA NATIONAL SERVICE CORPORATION'
                AND UPPER(trs.door_name) = 'SYDT'
                AND cm.sales_location = 'Downtown' THEN COALESCE(SUM(trs.store_sls_qty * dc.srp_usd), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'CHINA NATIONAL SERVICE CORPORATION'
                AND UPPER(trs.door_name) = 'SYDT'
                AND cm.sales_location = 'E-commerce' THEN COALESCE(
                    SUM(
                        (
                            trs.ecommerce_sls_qty + trs.membership_sls_qty
                        ) * dc.srp_usd
                    ),
                    0
                )
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'DUFRY'
                AND UPPER(trs.door_name) = 'DUFRY HAINAN'
                AND cm.sales_location = 'Downtown' THEN COALESCE(SUM(trs.store_sls_qty * dc.srp_usd), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'DUFRY'
                AND UPPER(trs.door_name) = 'DUFRY HAINAN'
                AND cm.sales_location = 'E-commerce' THEN COALESCE(SUM(trs.ecommerce_sls_qty * dc.srp_usd), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'DFS'
                AND UPPER(trs.door_name) = 'HNN MH OFFLINE STORE 0904'
                AND cm.sales_location = 'Downtown' THEN COALESCE(SUM(trs.store_sls_qty * dc.srp_usd), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'DFS'
                AND UPPER(trs.door_name) = 'HAINAN E-SHOP 0903'
                AND cm.sales_location = 'E-commerce' THEN COALESCE(SUM(trs.ecommerce_sls_qty * dc.srp_usd), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'LAGADERE'
                AND UPPER(trs.door_name) = 'LAGADERE HAINAN'
                AND cm.sales_location = 'Downtown' THEN COALESCE(SUM(trs.store_sls_qty * dc.srp_usd), 0)
                WHEN UPPER(trs.ctry_cd) = 'CN'
                AND UPPER(trs.retailer_name) = 'LAGADERE'
                AND UPPER(trs.door_name) = 'LAGADERE HAINAN'
                AND cm.sales_location = 'E-commerce' THEN COALESCE(SUM(trs.ecommerce_sls_qty * dc.srp_usd), 0)
                ELSE COALESCE(SUM(trs.sls_amt), 0)
            END AS sellout_value_usd,
            SUM(0) AS sellin_qty,
            SUM(0) AS sellin_value_usd,
            COALESCE(SUM(trs.stock_qty), 0) AS stock_qty,
            CASE
                WHEN UPPER(trs.ctry_cd) = 'KR' THEN COALESCE(SUM(trs.stock_amt), 0)
                WHEN UPPER(trs.ctry_cd) IN ('HK', 'CN', 'CM') THEN COALESCE(SUM(trs.stock_qty * dc.srp_usd), 0)
            END AS stock_value_usd,
            SUM(0) AS nts_tgt_usd
        FROM itg_rg_travel_retail_sellout AS trs
            LEFT JOIN v_intrm_reg_crncy_exch_fiscper AS exch ON trs.year_month = SUBSTRING(exch.fisc_per, 1, 4) || SUBSTRING(exch.fisc_per, 6, 7)
            AND UPPER(trs.crncy_cd) = UPPER(exch.from_crncy)
            AND UPPER(exch.to_crncy) = 'USD'
            LEFT JOIN itg_rg_travel_retail_channel_mapping AS cm ON UPPER(trs.ctry_cd) = UPPER(cm.ctry_cd)
            AND UPPER(trs.door_name) = UPPER(cm.door_name)
            AND UPPER(trs.retailer_name) = UPPER(cm.retailer_name)
            LEFT JOIN itg_rg_travel_retail_product_mapping AS dc ON UPPER(trs.dcl_code) = UPPER(dc.dcl_code)
            AND UPPER(trs.ctry_cd) = UPPER(dc.ctry_cd)
            LEFT JOIN edw_material_dim AS pk ON dc.sap_code = LTRIM(pk.matl_num, 0)
        GROUP BY trs.ctry_cd,
            trs.crncy_cd,
            UPPER(trs.retailer_name),
            trs.year,
            trs.month,
            trs.year_month,
            UPPER(trs.dcl_code),
            dc.sap_code,
            pk.pka_sub_brand_cd,
            pk.pka_sub_brand_desc,
            pk.pka_variant_cd,
            pk.pka_variant_desc,
            pk.pka_sub_variant_cd,
            pk.pka_sub_variant_desc,
            pk.pka_ingredient_cd,
            pk.pka_ingredient_desc,
            pk.pka_cover_cd,
            pk.pka_cover_desc,
            pk.pka_form_cd,
            pk.pka_form_desc,
            pk.pka_product_key_description,
            UPPER(trs.door_name),
            cm.cust_num,
            cm.sales_location,
            cm.sales_channel,
            dc.srp_usd
        HAVING (
                COALESCE(SUM(trs.sls_qty), 0) <> 0
                OR COALESCE(SUM(trs.stock_qty), 0) <> 0
            )
),

sellin as (
    SELECT 'SELLIN' AS IDENTIFIER,
            cm.ctry_cd,
            CASE
                WHEN UPPER(cm.ctry_cd) = 'KR' THEN 'KRW'
                WHEN UPPER(cm.ctry_cd) = 'HK' THEN 'HKD'
                WHEN UPPER(cm.ctry_cd) = 'CN' THEN 'RMB'
                WHEN UPPER(cm.ctry_cd) = 'SG' THEN 'SGD'
                WHEN UPPER(cm.ctry_cd) = 'TW' THEN 'TWD'
                WHEN UPPER(cm.ctry_cd) = 'CM' THEN 'KHR'
            END AS crncy_cd,
            cm.country_name,
            UPPER(cm.retailer_name) AS retailer_name,
            CAST(SUBSTRING(si.fisc_yr_per, 1, 4) AS INT) AS YEAR,
            CAST(SUBSTRING(si.fisc_yr_per, 6, 7) AS INT) AS MONTH,
            SUBSTRING(si.fisc_yr_per, 1, 4) || SUBSTRING(si.fisc_yr_per, 6, 7) AS year_month,
            UPPER(dc.dcl_code) AS dcl_code,
            si.matl_num,
            pk.pka_sub_brand_cd AS sub_brand,
            pk.pka_sub_brand_desc AS sub_brand_desc,
            pk.pka_variant_cd AS variant,
            pk.pka_variant_desc AS variant_desc,
            pk.pka_sub_variant_cd AS sub_variant,
            pk.pka_sub_variant_desc AS sub_variant_desc,
            pk.pka_ingredient_cd AS ingredient,
            pk.pka_ingredient_desc AS ingredient_desc,
            pk.pka_cover_cd AS cover,
            pk.pka_cover_desc AS cover_desc,
            pk.pka_form_cd AS form,
            pk.pka_form_desc AS form_desc,
            pk.pka_product_key_description AS product_key_desc,
            'NA' AS target_type,
            'NA' AS doors,
            si.cust_num,
            cm.sales_location,
            cm.sales_channel,
            dc.srp_usd,
            SUM(0) AS sellout_qty,
            SUM(0) AS sellout_value_usd,
            COALESCE(SUM(sellin_qty), 0) AS sellin_qty,
            COALESCE(SUM(sellin_value_usd), 0) AS sellin_value_usd,
            SUM(0) AS stock_qty,
            SUM(0) AS stock_value_usd,
            SUM(0) AS nts_tgt_usd
        FROM (
                SELECT SUBSTRING(copa.caln_day, 1, 6) AS CAL_MO,
                    copa.fisc_yr_per,
                    cd.cust_num,
                    cd.ctry_key AS ctry_cd,
                    LTRIM(copa.matl_num, 0) AS matl_num,
                    SUM(copa.qty) AS sellin_qty,
                    SUM(copa.amt_obj_crncy * exch.ex_rt) AS sellin_value_usd
                FROM edw_copa_trans_fact AS copa
                    LEFT JOIN v_intrm_reg_crncy_exch_fiscper AS exch ON copa.obj_crncy_co_obj = exch.from_crncy
                    AND copa.fisc_yr_per = exch.fisc_per
                    JOIN (
                        SELECT DISTINCT LTRIM(cust_num, 0) AS cust_num,
                            cust_nm,
                            ctry_key
                        FROM edw_customer_base_dim
                        WHERE LTRIM(cust_num, 0) IN (
                                SELECT DISTINCT LTRIM(cust_num, 0) AS cust_num
                                FROM edw_customer_sales_dim
                                WHERE sub_chnl_key IN (
                                        SELECT DISTINCT code
                                        FROM edw_code_descriptions
                                        WHERE (
                                                UPPER(code_type) = 'SUB CHANNEL KEY'
                                                AND UPPER(code_desc) LIKE '%TRAVEL RETAIL%'
                                            )
                                    )
                            )
                    ) AS cd ON LTRIM(copa.cust_num, 0) = cd.cust_num
                WHERE UPPER(copa.acct_hier_shrt_desc) = 'NTS'
                    AND UPPER(exch.to_crncy) = 'USD'
                GROUP BY SUBSTRING(copa.caln_day, 1, 6),
                    copa.fisc_yr_per,
                    cd.cust_num,
                    cd.ctry_key,
                    LTRIM(copa.matl_num, 0)
            ) AS si
            LEFT JOIN edw_material_dim AS pk ON si.matl_num = LTRIM(pk.matl_num, 0)
            LEFT JOIN (
                SELECT ctry_cd,
                    country_name,
                    cust_num,
                    retailer_name,
                    sales_location,
                    sales_channel,
                    rn
                FROM (
                        SELECT DISTINCT ctry_cd,
                            country_name,
                            cust_num,
                            retailer_name,
                            sales_location,
                            sales_channel,
                            ROW_number() OVER (
                                PARTITION BY cust_num,
                                retailer_name,
                                sales_channel
                                order by null
                            ) AS rn
                        FROM itg_rg_travel_retail_channel_mapping
                    )
                WHERE rn = 1
            ) AS cm ON (si.cust_num) = (cm.cust_num)
            LEFT JOIN (
                SELECT ctry_cd,
                    dcl_code,
                    sap_code,
                    sales_channel,
                    srp_usd
                FROM (
                        SELECT ctry_cd,
                            dcl_code,
                            sap_code,
                            sales_channel,
                            srp_usd,
                            ROW_number() OVER (
                                PARTITION BY COALESCE(sap_code, 'NA'),
                                ctry_cd
                                ORDER BY dcl_code
                            ) AS rn
                        FROM itg_rg_travel_retail_product_mapping
                    )
                WHERE rn = 1
            ) AS dc ON si.matl_num = COALESCE(dc.sap_code, 'NA')
            AND UPPER(cm.ctry_cd) = UPPER(dc.ctry_cd)
        GROUP BY cm.ctry_cd,
            cm.country_name,
            UPPER(cm.retailer_name),
            si.fisc_yr_per,
            UPPER(dc.dcl_code),
            si.matl_num,
            pk.pka_sub_brand_cd,
            pk.pka_sub_brand_desc,
            pk.pka_variant_cd,
            pk.pka_variant_desc,
            pk.pka_sub_variant_cd,
            pk.pka_sub_variant_desc,
            pk.pka_ingredient_cd,
            pk.pka_ingredient_desc,
            pk.pka_cover_cd,
            pk.pka_cover_desc,
            pk.pka_form_cd,
            pk.pka_form_desc,
            pk.pka_product_key_description,
            si.cust_num,
            cm.sales_location,
            cm.sales_channel,
            dc.srp_usd
),

sellin_copa as (
    SELECT 'SELLIN' AS IDENTIFIER,
            si.ctry_cd,
            CASE
                WHEN UPPER(si.ctry_cd) = 'KR' THEN 'KRW'
                WHEN UPPER(si.ctry_cd) = 'HK' THEN 'HKD'
                WHEN UPPER(si.ctry_cd) = 'CN' THEN 'RMB'
                WHEN UPPER(si.ctry_cd) = 'SG' THEN 'SGD'
                WHEN UPPER(si.ctry_cd) = 'TW' THEN 'TWD'
                WHEN UPPER(si.ctry_cd) = 'CM' THEN 'KHR'
            END AS crncy_cd,
            CASE
                WHEN UPPER(si.ctry_cd) = 'KR' THEN 'Korea'
                WHEN UPPER(si.ctry_cd) = 'HK' THEN 'Hong Kong'
                WHEN UPPER(si.ctry_cd) = 'CN' THEN 'China'
                WHEN UPPER(si.ctry_cd) = 'SG' THEN 'Singapore'
                WHEN UPPER(si.ctry_cd) = 'TW' THEN 'Taiwan'
                WHEN UPPER(si.ctry_cd) = 'MC' THEN 'Macau'
                WHEN UPPER(si.ctry_cd) = 'CM' THEN 'Cambodia'
            END AS country_name,
            si.cust_nm AS retailer_name,
            CAST(SUBSTRING(si.fisc_yr_per, 1, 4) AS INT) AS YEAR,
            CAST(SUBSTRING(si.fisc_yr_per, 6, 7) AS INT) AS MONTH,
            SUBSTRING(si.fisc_yr_per, 1, 4) || SUBSTRING(si.fisc_yr_per, 6, 7) AS year_month,
            'NA' AS dcl_code,
            si.matl_num,
            pk.pka_sub_brand_cd AS sub_brand,
            pk.pka_sub_brand_desc AS sub_brand_desc,
            pk.pka_variant_cd AS variant,
            pk.pka_variant_desc AS variant_desc,
            pk.pka_sub_variant_cd AS sub_variant,
            pk.pka_sub_variant_desc AS sub_variant_desc,
            pk.pka_ingredient_cd AS ingredient,
            pk.pka_ingredient_desc AS ingredient_desc,
            pk.pka_cover_cd AS cover,
            pk.pka_cover_desc AS cover_desc,
            pk.pka_form_cd AS form,
            pk.pka_form_desc AS form_desc,
            pk.pka_product_key_description AS product_key_desc,
            'NA' AS target_type,
            si.cust_nm AS doors,
            si.cust_num,
            si.sales_location,
            'Brand Stores' AS sales_channel,
            '0' AS srp_usd,
            SUM(0) AS sellout_qty,
            SUM(0) AS sellout_value_usd,
            COALESCE(SUM(si.sellin_qty), 0) AS sellin_qty,
            COALESCE(SUM(si.sellin_value_usd), 0) AS sellin_value_usd,
            SUM(0) AS stock_qty,
            SUM(0) AS stock_value_usd,
            SUM(0) AS nts_tgt_usd
        FROM (
                SELECT SUBSTRING(copa.caln_day, 1, 6) AS CAL_MO,
                    copa.fisc_yr_per,
                    cd.cust_num,
                    cd.cust_nm,
                    cd.ctry_key AS ctry_cd,
                    cd.retailer_name,
                    cd.sales_location,
                    LTRIM(copa.matl_num, 0) AS matl_num,
                    SUM(copa.qty) AS sellin_qty,
                    SUM(copa.amt_obj_crncy * exch.ex_rt) AS sellin_value_usd
                FROM edw_copa_trans_fact AS copa
                    LEFT JOIN v_intrm_reg_crncy_exch_fiscper AS exch ON copa.obj_crncy_co_obj = exch.from_crncy
                    AND copa.fisc_yr_per = exch.fisc_per
                    JOIN (
                        SELECT DISTINCT LTRIM(cbd.cust_num, 0) AS cust_num,
                            cbd.cust_nm,
                            cbd.ctry_key,
                            MAX(csd.retailer_name) AS retailer_name,
                            MAX(csd.sales_location) AS sales_location
                        FROM edw_customer_base_dim AS cbd,
                            (
                                SELECT DISTINCT CUST_NUM,
                                    CODE_DESC AS retailer_name,
                                    NULL AS sales_location
                                FROM (
                                        SELECT DISTINCT LTRIM(csd1.cust_num, 0) AS cust_num,
                                            csd1.bnr_key
                                        FROM edw_customer_sales_dim AS csd1,
                                            (
                                                SELECT DISTINCT code
                                                FROM edw_code_descriptions
                                                WHERE (
                                                        UPPER(code_type) = 'PARENT CUSTOMER KEY'
                                                        AND UPPER(code_desc) = 'DCL BRAND STORE'
                                                    )
                                            ) AS edc1
                                        WHERE csd1.sub_chnl_key = edc1.code
                                            OR csd1.prnt_cust_key = edc1.code
                                    ) AS cust1,
                                    edw_code_descriptions AS cd
                                WHERE cust1.bnr_key = cd.code
                                UNION ALL
                                SELECT DISTINCT CUST_NUM,
                                    NULL AS retailer_name,
                                    CODE_DESC AS sales_location
                                FROM (
                                        SELECT DISTINCT LTRIM(csd2.cust_num, 0) AS cust_num,
                                            csd2.sub_chnl_key
                                        FROM edw_customer_sales_dim AS csd2,
                                            (
                                                SELECT DISTINCT code
                                                FROM edw_code_descriptions
                                                WHERE (
                                                        UPPER(code_type) = 'PARENT CUSTOMER KEY'
                                                        AND UPPER(code_desc) = 'DCL BRAND STORE'
                                                    )
                                            ) AS edc2
                                        WHERE csd2.sub_chnl_key = edc2.code
                                            OR csd2.prnt_cust_key = edc2.code
                                    ) AS cust2,
                                    edw_code_descriptions AS cd
                                WHERE cust2.sub_chnl_key = cd.code
                            ) AS csd
                        WHERE LTRIM(cbd.cust_num, 0) = csd.cust_num
                        GROUP BY LTRIM(cbd.cust_num, 0),
                            cbd.cust_nm,
                            cbd.ctry_key
                    ) AS cd ON LTRIM(copa.cust_num, 0) = cd.cust_num
                WHERE UPPER(copa.acct_hier_shrt_desc) = 'NTS'
                    AND UPPER(exch.to_crncy) = 'USD'
                GROUP BY SUBSTRING(copa.caln_day, 1, 6),
                    copa.fisc_yr_per,
                    cd.cust_num,
                    cd.cust_nm,
                    cd.ctry_key,
                    cd.retailer_name,
                    cd.sales_location,
                    LTRIM(copa.matl_num, 0)
            ) AS si
            LEFT JOIN edw_material_dim AS pk ON si.matl_num = LTRIM(pk.matl_num, 0)
        GROUP BY si.ctry_cd,
            UPPER(si.retailer_name),
            si.fisc_yr_per,
            si.matl_num,
            pk.pka_sub_brand_cd,
            pk.pka_sub_brand_desc,
            pk.pka_variant_cd,
            pk.pka_variant_desc,
            pk.pka_sub_variant_cd,
            pk.pka_sub_variant_desc,
            pk.pka_ingredient_cd,
            pk.pka_ingredient_desc,
            pk.pka_cover_cd,
            pk.pka_cover_desc,
            pk.pka_form_cd,
            pk.pka_form_desc,
            pk.pka_product_key_description,
            si.cust_num,
            si.cust_nm,
            si.sales_location
),

sellout_copa as (
    SELECT 'SELLOUT' AS IDENTIFIER,
            si.ctry_cd,
            CASE
                WHEN UPPER(si.ctry_cd) = 'KR' THEN 'KRW'
                WHEN UPPER(si.ctry_cd) = 'HK' THEN 'HKD'
                WHEN UPPER(si.ctry_cd) = 'CN' THEN 'RMB'
                WHEN UPPER(si.ctry_cd) = 'SG' THEN 'SGD'
                WHEN UPPER(si.ctry_cd) = 'TW' THEN 'TWD'
                WHEN UPPER(si.ctry_cd) = 'CM' THEN 'KHR'
            END AS crncy_cd,
            CASE
                WHEN UPPER(si.ctry_cd) = 'KR' THEN 'Korea'
                WHEN UPPER(si.ctry_cd) = 'HK' THEN 'Hong Kong'
                WHEN UPPER(si.ctry_cd) = 'CN' THEN 'China'
                WHEN UPPER(si.ctry_cd) = 'SG' THEN 'Singapore'
                WHEN UPPER(si.ctry_cd) = 'TW' THEN 'Taiwan'
                WHEN UPPER(si.ctry_cd) = 'MC' THEN 'Macau'
                WHEN UPPER(si.ctry_cd) = 'CM' THEN 'Cambodia'
            END AS country_name,
            si.cust_nm AS retailer_name,
            CAST(SUBSTRING(si.fisc_yr_per, 1, 4) AS INT) AS YEAR,
            CAST(SUBSTRING(si.fisc_yr_per, 6, 7) AS INT) AS MONTH,
            SUBSTRING(si.fisc_yr_per, 1, 4) || SUBSTRING(si.fisc_yr_per, 6, 7) AS year_month,
            'NA' AS dcl_code,
            si.matl_num,
            pk.pka_sub_brand_cd AS sub_brand,
            pk.pka_sub_brand_desc AS sub_brand_desc,
            pk.pka_variant_cd AS variant,
            pk.pka_variant_desc AS variant_desc,
            pk.pka_sub_variant_cd AS sub_variant,
            pk.pka_sub_variant_desc AS sub_variant_desc,
            pk.pka_ingredient_cd AS ingredient,
            pk.pka_ingredient_desc AS ingredient_desc,
            pk.pka_cover_cd AS cover,
            pk.pka_cover_desc AS cover_desc,
            pk.pka_form_cd AS form,
            pk.pka_form_desc AS form_desc,
            pk.pka_product_key_description AS product_key_desc,
            'NA' AS target_type,
            si.cust_nm AS doors,
            si.cust_num,
            si.sales_location,
            'Brand Stores' AS sales_channel,
            '0' AS srp_usd,
            COALESCE(SUM(si.sellin_qty), 0) AS sellout_qty,
            COALESCE(SUM(si.sellin_value_usd), 0) AS sellout_value_usd,
            SUM(0) AS sellin_qty,
            SUM(0) AS sellin_value_usd,
            SUM(0) AS stock_qty,
            SUM(0) AS stock_value_usd,
            SUM(0) AS nts_tgt_usd
        FROM (
                SELECT SUBSTRING(copa.caln_day, 1, 6) AS CAL_MO,
                    copa.fisc_yr_per,
                    cd.cust_num,
                    cd.cust_nm,
                    cd.ctry_key AS ctry_cd,
                    cd.retailer_name,
                    cd.sales_location,
                    LTRIM(copa.matl_num, 0) AS matl_num,
                    SUM(copa.qty) AS sellin_qty,
                    SUM(copa.amt_obj_crncy * exch.ex_rt) AS sellin_value_usd
                FROM edw_copa_trans_fact AS copa
                    LEFT JOIN v_intrm_reg_crncy_exch_fiscper AS exch ON copa.obj_crncy_co_obj = exch.from_crncy
                    AND copa.fisc_yr_per = exch.fisc_per
                    JOIN (
                        SELECT DISTINCT LTRIM(cbd.cust_num, 0) AS cust_num,
                            cbd.cust_nm,
                            cbd.ctry_key,
                            MAX(csd.retailer_name) AS retailer_name,
                            MAX(csd.sales_location) AS sales_location
                        FROM edw_customer_base_dim AS cbd,
                            (
                                SELECT DISTINCT CUST_NUM,
                                    CODE_DESC AS retailer_name,
                                    NULL AS sales_location
                                FROM (
                                        SELECT DISTINCT LTRIM(csd1.cust_num, 0) AS cust_num,
                                            csd1.bnr_key
                                        FROM edw_customer_sales_dim AS csd1,
                                            (
                                                SELECT DISTINCT code
                                                FROM edw_code_descriptions
                                                WHERE (
                                                        UPPER(code_type) = 'PARENT CUSTOMER KEY'
                                                        AND UPPER(code_desc) = 'DCL BRAND STORE'
                                                    )
                                            ) AS edc1
                                        WHERE csd1.sub_chnl_key = edc1.code
                                            OR csd1.prnt_cust_key = edc1.code
                                    ) AS cust1,
                                    edw_code_descriptions AS cd
                                WHERE cust1.bnr_key = cd.code
                                UNION ALL
                                SELECT DISTINCT CUST_NUM,
                                    NULL AS retailer_name,
                                    CODE_DESC AS sales_location
                                FROM (
                                        SELECT DISTINCT LTRIM(csd2.cust_num, 0) AS cust_num,
                                            csd2.sub_chnl_key
                                        FROM edw_customer_sales_dim AS csd2,
                                            (
                                                SELECT DISTINCT code
                                                FROM edw_code_descriptions
                                                WHERE (
                                                        UPPER(code_type) = 'PARENT CUSTOMER KEY'
                                                        AND UPPER(code_desc) = 'DCL BRAND STORE'
                                                    )
                                            ) AS edc2
                                        WHERE csd2.sub_chnl_key = edc2.code
                                            OR csd2.prnt_cust_key = edc2.code
                                    ) AS cust2,
                                    edw_code_descriptions AS cd
                                WHERE cust2.sub_chnl_key = cd.code
                            ) AS csd
                        WHERE LTRIM(cbd.cust_num, 0) = csd.cust_num
                        GROUP BY LTRIM(cbd.cust_num, 0),
                            cbd.cust_nm,
                            cbd.ctry_key
                    ) AS cd ON LTRIM(copa.cust_num, 0) = cd.cust_num
                WHERE UPPER(copa.acct_hier_shrt_desc) = 'NTS'
                    AND UPPER(exch.to_crncy) = 'USD'
                GROUP BY SUBSTRING(copa.caln_day, 1, 6),
                    copa.fisc_yr_per,
                    cd.cust_num,
                    cd.cust_nm,
                    cd.ctry_key,
                    cd.retailer_name,
                    cd.sales_location,
                    LTRIM(copa.matl_num, 0)
            ) AS si
            LEFT JOIN edw_material_dim AS pk ON si.matl_num = LTRIM(pk.matl_num, 0)
        GROUP BY si.ctry_cd,
            UPPER(si.retailer_name),
            si.fisc_yr_per,
            si.matl_num,
            pk.pka_sub_brand_cd,
            pk.pka_sub_brand_desc,
            pk.pka_variant_cd,
            pk.pka_variant_desc,
            pk.pka_sub_variant_cd,
            pk.pka_sub_variant_desc,
            pk.pka_ingredient_cd,
            pk.pka_ingredient_desc,
            pk.pka_cover_cd,
            pk.pka_cover_desc,
            pk.pka_form_cd,
            pk.pka_form_desc,
            pk.pka_product_key_description,
            si.cust_num,
            si.cust_nm,
            si.sales_location
),

target as (
    SELECT 'TARGET' as identifier,
            tgt.ctry_cd,
            tgt.crncy_cd,
            tgt.country_name,
            'NA' as retailer_name,
            tgt.year,
            tgt.month,
            tgt.year_month,
            'NA' as dcl_code,
            'NA' as matl_num,
            'NA' as sub_brand,
            'NA' as sub_brand_desc,
            'NA' as variant,
            'NA' as variant_desc,
            'NA' as sub_variant,
            'NA' as sub_variant_desc,
            'NA' as ingredient,
            'NA' as ingredient_desc,
            'NA' as cover,
            'NA' as cover_desc,
            'NA' as form,
            'NA' as form_desc,
            'NA' as product_key_desc,
            upper(tgt.target_type) as target_type,
            'NA' as doors,
            'NA' as cust_num,
            'NA' as sales_location,
            tgt.sales_channel,
            '0' as srp_usd,
            sum(0) as sellout_qty,
            sum(0) as sellout_value_usd,
            sum(0) as sellin_qty,
            sum(0) as sellin_value_usd,
            sum(0) as stock_qty,
            sum(0) as stock_value_usd,
            CASE
                when upper(target_type) = 'RF' then coalesce(sum(tgt.nts_tgt), 0)
                when upper(sales_channel) = 'BRAND STORES' then coalesce(sum(tgt.nts_tgt), 0)
                else coalesce(sum(tgt.nts_tgt * 1000000), 0)
            end as nts_tgt_usd
        from itg_rg_travel_retail_nts_target as tgt
        group by tgt.ctry_cd,
            tgt.crncy_cd,
            tgt.country_name,
            tgt.year,
            tgt.month,
            tgt.year_month,
            upper(tgt.target_type),
            tgt.sales_channel
        having coalesce(sum(tgt.nts_tgt), 0) <> 0
),

combined as (
    select * from sellout
    union all
    select * from sellin
    union all
    select * from sellin_copa
    union all
    select * from sellout_copa
    union all
    select * from target
),

transformed as (
    select identifier::varchar(7) as identifier,
    ctry_cd::varchar(150) as ctry_cd,
    crncy_cd::varchar(3) as crncy_cd,
    country_name::varchar(30) as country_name,
    retailer_name::varchar(150) as retailer_name,
    year::number(18,0) as "year",
    month::number(18,0) as "month",
    year_month::varchar(22) as year_month,
    dcl_code::varchar(50) as dcl_code,
    matl_num::varchar(30) as matl_num,
    sub_brand::varchar(4) as sub_brand,
    sub_brand_desc::varchar(30) as sub_brand_desc,
    variant::varchar(4) as variant,
    variant_desc::varchar(30) as variant_desc,
    sub_variant::varchar(4) as sub_variant,
    sub_variant_desc::varchar(30) as sub_variant_desc,
    ingredient::varchar(4) as ingredient,
    ingredient_desc::varchar(30) as ingredient_desc,
    cover::varchar(4) as cover,
    cover_desc::varchar(30) as cover_desc,
    form::varchar(4) as form,
    form_desc::varchar(30) as form_desc,
    product_key_desc::varchar(255) as product_key_desc,
    target_type::varchar(15) as target_type,
    doors::varchar(100) as doors,
    cust_num::varchar(30) as cust_num,
    initcap(sales_location)::varchar(100) as sales_location,
    sales_channel::varchar(100) as sales_channel,
    srp_usd::number(21,5) as srp_usd,
    sum(sellout_qty)::number(38,5) as sellout_qty,
    sum(sellout_value_usd)::number(38,15) as sellout_value_usd,
    sum(sellin_qty)::number(38,5) as sellin_qty,
    sum(sellin_value_usd)::number(38,15) as sellin_value_usd,
    sum(stock_qty)::number(38,15) as stock_qty,
    sum(stock_value_usd)::number(38,5) as stock_value_usd,
    sum(nts_tgt_usd)::number(38,5) as nts_tgt_usd
    from combined
    group by identifier,
    ctry_cd,
    crncy_cd,
    country_name,
    retailer_name,
    "year",
    "month",
    year_month,
    dcl_code,
    matl_num,
    sub_brand,
    sub_brand_desc,
    variant,
    variant_desc,
    sub_variant,
    sub_variant_desc,
    ingredient,
    ingredient_desc,
    cover,
    cover_desc,
    form,
    form_desc,
    product_key_desc,
    target_type,
    doors,
    cust_num,
    sales_location,
    sales_channel,
    srp_usd
),
--Final CTE
final as (
    select * from transformed
)

select * from final