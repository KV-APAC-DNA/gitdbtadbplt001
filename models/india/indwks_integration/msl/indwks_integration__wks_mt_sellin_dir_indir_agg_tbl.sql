with wks_mt_sellin_tbl as(
    select * from {{ ref('indwks_integration__wks_mt_sellin_tbl') }}
),
transformed as(
    SELECT fisc_yr
      ,mth_mm
      ,franchise_name
      ,brand_name
      ,variant_name
      ,product_category_name
      ,mothersku_name
      ,channel_name
      ,account_name_offtake
      ,SUM(invoice_quantity) AS invoice_quantity
      ,SUM(invoice_value) AS invoice_value
    FROM wks_mt_sellin_tbl
    GROUP BY fisc_yr
            ,mth_mm
            ,franchise_name
            ,brand_name
            ,variant_name
            ,product_category_name
            ,mothersku_name
            ,channel_name
            ,account_name_offtake
)
select * from transformed