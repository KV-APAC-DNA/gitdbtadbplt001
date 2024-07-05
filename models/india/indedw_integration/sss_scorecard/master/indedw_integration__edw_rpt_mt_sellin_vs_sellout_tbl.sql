with wks_mt_sellin_vs_sellout_pre_rpt_tbl as(
    select * from {{ ref('indwks_integration__wks_mt_sellin_vs_sellout_pre_rpt_tbl') }}
),
final as(
    SELECT fisc_yr::varchar(12) as fisc_yr
      ,mth_mm::varchar(14) as mth_mm
      ,common_account_name::varchar(255) as common_account_name
      ,common_channel_name::varchar(50) as common_channel_name
      ,channel_name_sellin::varchar(255) as channel_name_sellin
      ,franchise_name_sellin::varchar(255) as franchise_name_sellin
      ,brand_name_sellin::varchar(255) as brand_name_sellin
      ,variant_name_sellin::varchar(255) as variant_name_sellin
      ,product_category_name_sellin::varchar(225) as product_category_name_sellin
      ,mothersku_name_sellin::varchar(225) as mothersku_name_sellin
      ,invoice_quantity_sellin::number(38,4) as invoice_quantity_sellin
      ,invoice_value_sellin::number(38,4) as invoice_value_sellin
      ,data_source_sellout::varchar(20) as data_source_sellout
      ,pos_offtake_level_sellout::varchar(20) as pos_offtake_level_sellout
      ,account_name_sellout::varchar(255) as account_name_sellout
      ,mother_sku_name_sellout::varchar(255) as mother_sku_name_sellout
      ,brand_name_sellout::varchar(255) as brand_name_sellout
      ,franchise_name_sellout::varchar(255) as franchise_name_sellout
      ,internal_category_sellout::varchar(255) as internal_category_sellout
      ,internal_subcategory_sellout::varchar(255) as internal_subcategory_sellout
      ,external_category_sellout::varchar(255) as external_category_sellout
      ,external_subcategory_sellout::varchar(255) as external_subcategory_sellout
      ,product_category_name_sellout::varchar(255) as product_category_name_sellout
      ,variant_name_sellout::varchar(255) as variant_name_sellout
      ,external_mothersku_code_sellout::varchar(255) as external_mothersku_code_sellout
      ,external_mothersku_name_sellout::varchar(255) as external_mothersku_name_sellout
      ,sls_qty_sellout::number(38,4) as sls_qty_sellout
      ,sls_val_lcy_sellout::number(38,4) as sls_val_lcy_sellout
      ,factorized_sls_val_lcy_sellout::number(38,4) as factorized_sls_val_lcy_sellout
      ,sales_factor_sellout::number(10,2) as sales_factor_sellout
      ,sales_factor_ref_month_sellout::varchar(14) as sales_factor_ref_month_sellout
      ,internal_mothersku_name::varchar(255) as internal_mothersku_name
      ,internal_brand_name::varchar(255) as internal_brand_name
      ,internal_franchise_name::varchar(255) as internal_franchise_name
      ,current_timestamp()::timestamp_ntz(9) as crt_dttm
      ,current_timestamp()::timestamp_ntz(9) as updt_dttm
FROM wks_mt_sellin_vs_sellout_pre_rpt_tbl
)
select * from final
