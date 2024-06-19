with wks_mt_sellin_vs_sellout_tbl as(
    select * from {{ ref('indwks_integration__wks_mt_sellin_vs_sellout_tbl') }}
),
wks_mt_sellin_vs_sellout_fctr_tbl_fnl as(
    select * from {{ ref('indwks_integration__wks_mt_sellin_vs_sellout_fctr_tbl_fnl') }}
),
transformed as(
SELECT fisc_yr
      ,mth_mm
      ,common_account_name
      ,common_channel_name
      ,channel_name_sellin
      ,franchise_name_sellin
      ,brand_name_sellin
      ,variant_name_sellin
      ,product_category_name_sellin
      ,mothersku_name_sellin
      ,invoice_quantity_sellin
      ,invoice_value_sellin
      ,data_source_sellout
      ,pos_offtake_level_sellout
      ,account_name_sellout
      ,mother_sku_name_sellout
      ,brand_name_sellout
      ,franchise_name_sellout
      ,internal_category_sellout
      ,internal_subcategory_sellout
      ,external_category_sellout
      ,external_subcategory_sellout
      ,product_category_name_sellout
      ,variant_name_sellout
      ,external_mothersku_code_sellout
      ,external_mothersku_name_sellout    
      ,sls_qty_sellout
      ,sls_val_lcy_sellout
      ,(tmp.sls_val_lcy_sellout)/(fctr.factor)::numeric(10,2) AS factorized_sls_val_lcy_sellout
      ,fctr.factor::numeric(10,2) AS sales_factor_sellout
      ,fctr.mth_mm_fi AS sales_factor_ref_month_sellout
      ,internal_mothersku_name
      ,internal_brand_name
      ,internal_franchise_name
FROM wks_mt_sellin_vs_sellout_tbl tmp
LEFT JOIN wks_mt_sellin_vs_sellout_fctr_tbl_fnl fctr
       ON tmp.mth_mm = fctr.mth_mm_cal
       AND tmp.common_account_name = fctr.account_name
       AND tmp.data_source_sellout IN ('ECOM_OFFTAKE','POS_OFFTAKE')
)
select * from transformed