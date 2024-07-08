with wks_mt_sellin_dir_indir_agg_tbl as(
    select * from {{ ref('indwks_integration__wks_mt_sellin_dir_indir_agg_tbl') }}
),
wks_mt_sellout_offtake_tbl as(
    select * from {{ ref('indwks_integration__wks_mt_sellout_offtake_tbl') }}
),
transformed as(
    SELECT NVL(sellin.fisc_yr::text,sellout.fisc_year) AS fisc_yr
        ,NVL(sellin.mth_mm::text,sellout.period) AS mth_mm
        ,NVL(sellin.account_name_offtake,sellout.account_name) AS common_account_name
        ,CASE WHEN UPPER(NVL(sellin.account_name_offtake,sellout.account_name)) IN ('DMART ALL INDIA','APOLLO ALL INDIA','RELIANCE ALL INDIA','ABRL ALL INDIA') THEN 'Key Account'
                WHEN UPPER(NVL(sellin.account_name_offtake,sellout.account_name)) IN ('AMAZON','BIGBASKET','FLIPKART','NYKAA') THEN 'E-Commerce'
                ELSE 'NA'
        END AS common_channel_name
        ,sellin.channel_name AS channel_name_sellin
        ,sellin.franchise_name AS franchise_name_sellin
        ,sellin.brand_name AS brand_name_sellin
        ,sellin.variant_name AS variant_name_sellin
        ,sellin.product_category_name AS product_category_name_sellin
        ,sellin.mothersku_name AS mothersku_name_sellin
        ,sellin.invoice_quantity AS invoice_quantity_sellin
        ,sellin.invoice_value AS invoice_value_sellin
        ,sellout.data_source AS data_source_sellout
        ,sellout.pos_offtake_level AS pos_offtake_level_sellout
        ,sellout.account_name AS account_name_sellout
        ,sellout.mother_sku_name AS mother_sku_name_sellout
        ,sellout.brand_name AS brand_name_sellout
        ,sellout.franchise_name AS franchise_name_sellout
        ,sellout.internal_category AS internal_category_sellout
        ,sellout.internal_subcategory AS internal_subcategory_sellout
        ,sellout.external_category AS external_category_sellout
        ,sellout.external_subcategory AS external_subcategory_sellout
        ,sellout.product_category_name AS product_category_name_sellout
        ,sellout.variant_name AS variant_name_sellout
        ,sellout.external_mothersku_code AS external_mothersku_code_sellout
        ,sellout.external_mothersku_name AS external_mothersku_name_sellout
        ,sellout.sls_qty AS sls_qty_sellout
        ,sellout.sls_val_lcy AS sls_val_lcy_sellout
        ,UPPER(NVL(sellin.mothersku_name,sellout.mother_sku_name)) AS internal_mothersku_name
        ,UPPER(NVL(sellin.brand_name,sellout.internal_brand_name)) AS internal_brand_name
        ,UPPER(NVL(sellin.franchise_name,sellout.internal_franchise_name)) AS internal_franchise_name
    FROM wks_mt_sellin_dir_indir_agg_tbl sellin
    FULL OUTER JOIN wks_mt_sellout_offtake_tbl sellout
                ON sellin.mth_mm = sellout.period
            AND sellin.account_name_offtake = sellout.account_name
            AND UPPER(sellin.mothersku_name) = UPPER(sellout.mother_sku_name)
)
select * from transformed