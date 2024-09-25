with v_ecomm_sales_inv_acc_ch_grp as (
    select * from {{ ref('indedw_integration__v_ecomm_sales_inv_acc_ch_grp') }}
),

result as (
SELECT v_ecomm_sales_inv_acc_ch_grp.data_source,
    v_ecomm_sales_inv_acc_ch_grp.fisc_yr,
    v_ecomm_sales_inv_acc_ch_grp.mth_mm,
    v_ecomm_sales_inv_acc_ch_grp.qtr,
    v_ecomm_sales_inv_acc_ch_grp.month,
    v_ecomm_sales_inv_acc_ch_grp.region_name,
    v_ecomm_sales_inv_acc_ch_grp.zone_name,
    v_ecomm_sales_inv_acc_ch_grp.territory_name,
    v_ecomm_sales_inv_acc_ch_grp.customer_code,
    v_ecomm_sales_inv_acc_ch_grp.customer_name,
    v_ecomm_sales_inv_acc_ch_grp.udc_keyaccountname,
    v_ecomm_sales_inv_acc_ch_grp.franchise_name,
    v_ecomm_sales_inv_acc_ch_grp.brand_name,
    v_ecomm_sales_inv_acc_ch_grp.variant_name,
    v_ecomm_sales_inv_acc_ch_grp.product_category_name,
    v_ecomm_sales_inv_acc_ch_grp.mothersku_name,
    v_ecomm_sales_inv_acc_ch_grp.product_name,
    v_ecomm_sales_inv_acc_ch_grp.product_code,
    v_ecomm_sales_inv_acc_ch_grp.channel_name_inv,
    v_ecomm_sales_inv_acc_ch_grp.channel_name_sales,
    v_ecomm_sales_inv_acc_ch_grp.bill_type,
    v_ecomm_sales_inv_acc_ch_grp.account_name,
    sum(v_ecomm_sales_inv_acc_ch_grp.invoice_quantity) AS invoice_quantity,
    sum(v_ecomm_sales_inv_acc_ch_grp.invoice_value) AS invoice_value
FROM v_ecomm_sales_inv_acc_ch_grp
WHERE (
        (
            (
                (COALESCE(upper(v_ecomm_sales_inv_acc_ch_grp.channel_name_inv), upper(v_ecomm_sales_inv_acc_ch_grp.channel_name_sales)) = 'E-COMMERCE'::TEXT)
                OR (COALESCE(upper(v_ecomm_sales_inv_acc_ch_grp.channel_name_inv), upper(v_ecomm_sales_inv_acc_ch_grp.channel_name_sales)) = 'KEY ACCOUNT'::TEXT)
                )
            AND ((v_ecomm_sales_inv_acc_ch_grp.data_source)::TEXT = 'INVOICE_CUBE'::TEXT)
            )
        AND (v_ecomm_sales_inv_acc_ch_grp.fisc_yr >= 2021)
        )
GROUP BY v_ecomm_sales_inv_acc_ch_grp.data_source,
    v_ecomm_sales_inv_acc_ch_grp.fisc_yr,
    v_ecomm_sales_inv_acc_ch_grp.mth_mm,
    v_ecomm_sales_inv_acc_ch_grp.qtr,
    v_ecomm_sales_inv_acc_ch_grp.month,
    v_ecomm_sales_inv_acc_ch_grp.region_name,
    v_ecomm_sales_inv_acc_ch_grp.zone_name,
    v_ecomm_sales_inv_acc_ch_grp.territory_name,
    v_ecomm_sales_inv_acc_ch_grp.customer_code,
    v_ecomm_sales_inv_acc_ch_grp.customer_name,
    v_ecomm_sales_inv_acc_ch_grp.udc_keyaccountname,
    v_ecomm_sales_inv_acc_ch_grp.franchise_name,
    v_ecomm_sales_inv_acc_ch_grp.brand_name,
    v_ecomm_sales_inv_acc_ch_grp.variant_name,
    v_ecomm_sales_inv_acc_ch_grp.product_category_name,
    v_ecomm_sales_inv_acc_ch_grp.mothersku_name,
    v_ecomm_sales_inv_acc_ch_grp.product_name,
    v_ecomm_sales_inv_acc_ch_grp.product_code,
    v_ecomm_sales_inv_acc_ch_grp.channel_name_inv,
    v_ecomm_sales_inv_acc_ch_grp.channel_name_sales,
    v_ecomm_sales_inv_acc_ch_grp.bill_type,
    v_ecomm_sales_inv_acc_ch_grp.account_name
),

final as (
    select 
        data_source::varchar(12) as data_source,
        fisc_yr::number(18,0) as fisc_yr,
        mth_mm::number(18,0) as mth_mm,
        qtr::number(18,0) as qtr,
        month::varchar(16777216) as month,
        region_name::varchar(16777216) as region_name,
        zone_name::varchar(16777216) as zone_name,
        territory_name::varchar(16777216) as territory_name,
        customer_code::varchar(50) as customer_code,
        customer_name::varchar(16777216) as customer_name,
        udc_keyaccountname::varchar(16777216) as udc_keyaccountname,
        franchise_name::varchar(16777216) as franchise_name,
        brand_name::varchar(16777216) as brand_name,
        variant_name::varchar(16777216) as variant_name,
        product_category_name::varchar(16777216) as product_category_name,
        mothersku_name::varchar(16777216) as mothersku_name,
        product_name::varchar(16777216) as product_name,
        product_code::varchar(50) as product_code,
        channel_name_inv::varchar(16777216) as channel_name_inv,
        channel_name_sales::varchar(16777216) as channel_name_sales,
        bill_type::varchar(30) as bill_type,
        account_name::varchar(16777216) as account_name,
        invoice_quantity::number(38,4) as invoice_quantity,
        invoice_value::number(38,8) as invoice_value
    from result
)

select * from final
