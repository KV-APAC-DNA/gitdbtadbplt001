with v_rpt_pos_offtake as(
    select * from {{ ref('indedw_integration__v_rpt_pos_offtake') }}
),
wks_mt_paramet_accounts as(
    select * from {{ ref('indwks_integration__wks_mt_paramet_accounts') }}
),
edw_ecommerce_offtake as(
    select * from {{ ref('indedw_integration__edw_ecommerce_offtake') }}
),
edw_product_dim as(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
union1 as(
    SELECT 'POS_OFFTAKE' AS data_source
      ,pos.period::text as period
      ,pos.fisc_year 
      ,pos.fis_month AS fisc_month
      ,pos."level" AS pos_offtake_level
      ,pos.account_name
      ,pos.mother_sku_name
      ,pos.brand_name
      ,pos.franchise_name
      ,pos.internal_category
      ,pos.internal_subcategory
      ,pos.external_category
      ,pos.external_subcategory
      ,pos.product_category_name
      ,pos.variant_name
      ,pos.article_cd AS external_mothersku_code
      ,pos.article_name AS external_mothersku_name
      ,pos.franchise_name AS internal_franchise_name
      ,pos.brand_name AS internal_brand_name
      ,pos.variant_name AS internal_variant_name
      ,SUM(pos.sls_qty) AS sls_qty
      ,SUM(pos.sls_val_lcy) AS sls_val_lcy
    FROM v_rpt_pos_offtake pos
    WHERE UPPER(account_name) IN (SELECT UPPER(account_name_as_per_offtake_data_code)
                                FROM wks_mt_paramet_accounts
                                GROUP BY 1)
    AND fisc_year >= EXTRACT(YEAR FROM current_timestamp()::timestamp_ntz(9)) - 2      -- to be parameterized
    GROUP BY pos.period::text
            ,pos.fisc_year
            ,pos.fis_month
            ,pos."level"
            ,pos.account_name
            ,pos.mother_sku_name
            ,pos.brand_name
            ,pos.franchise_name
            ,pos.internal_category
            ,pos.internal_subcategory
            ,pos.external_category
            ,pos.external_subcategory
            ,pos.product_category_name
            ,pos.variant_name
            ,pos.article_cd
            ,pos.article_name
            ,pos.franchise_name
            ,pos.brand_name
            ,pos.variant_name
),
union2 as(
    SELECT 'ECOM_OFFTAKE' AS data_source
      ,TO_CHAR(TO_DATE(transaction_date,'YYYY-MM-DD'),'YYYYMM') AS period
      ,TO_CHAR(TO_DATE(transaction_date,'YYYY-MM-DD'),'YYYY') AS fisc_year
      ,TO_CHAR(TO_DATE(transaction_date,'YYYY-MM-DD'),'MM') AS fisc_month
      ,NULL AS pos_offtake_level
      ,account_name
      ,generic_product_code AS mother_sku_name
      ,retailer_brand AS brand_name
      ,NULL AS franchise_name
      ,NULL AS internal_category
      ,NULL AS internal_subcategory
      ,NULL AS external_category
      ,NULL AS external_subcategory
      ,NULL AS product_category_name
      ,NULL AS variant_name
      ,account_sku_code AS external_mothersku_code
      ,retailer_product_name AS external_mothersku_name
      ,pd.franchise_name AS internal_franchise_name
      ,pd.brand_name AS internal_brand_name
      ,pd.variant_name AS internal_variant_name  
      ,SUM(sales_qty) AS sls_qty
      ,SUM(sales_value) AS sls_val_lcy
    FROM edw_ecommerce_offtake ecom
    LEFT JOIN (SELECT mothersku_name, 
                    franchise_name,
                    brand_name,
                    variant_name
            FROM edw_product_dim
            WHERE NVL(delete_flag,'XYZ') <> 'N'
            GROUP BY 1,2,3,4) pd
        ON UPPER(ecom.generic_product_code) = UPPER(pd.mothersku_name)
    WHERE UPPER(account_name) IN (SELECT UPPER(account_name_as_per_offtake_data_code)
                                FROM wks_mt_paramet_accounts
                                GROUP BY 1)
    AND EXTRACT(YEAR FROM TO_DATE(transaction_date,'YYYY-MM-DD')) >= EXTRACT(YEAR FROM current_timestamp()::timestamp_ntz(9)) - 2      -- to be parameterized
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

),
transformed as(
    select * from union1
    union all
    select * from union2
)
select * from transformed