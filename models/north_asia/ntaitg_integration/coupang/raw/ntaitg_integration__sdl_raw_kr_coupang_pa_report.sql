{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_pa_report') }}
),
final as (
    select 
    date ,
Bidding_Type ,
Sales_method ,
Ad_types ,
Campaign_ID ,
Campaign_Name ,
Ad_groups ,
Ad_execution_product_name ,
Ad_execution_option_ID ,
Ad_con_revenue_gen_product_nm ,
Ad_Con_Revenue_Gen_Product_Option_ID ,
Ad_Impression_Area ,
keyword ,
Impression_Count ,
Click_Count ,
Ad_Cost ,
Ctr ,
Total_orders_1d ,
Direct_orders_1d ,
Indirect_orders_1d ,
Total_sales_1d ,
Direct_sales_quantity_1d ,
Indirect_Sales_Quantity_1d ,
Total_Conversion_Sales_1d ,
Direct_conversion_sales_1d ,
Indirect_conversion_sales_1d ,
Total_orders_14d ,
Direct_orders_14d ,
Indirect_orders_14d ,
Total_sales_14d ,
Direct_Sales_Quantity_14d ,
Indirect_Sales_Quantity_14d ,
Total_Conversion_Sales_14d ,
Direct_conversion_sales_14d ,
Indirect_conversion_sales_14d ,
Total_ad_return_1d ,
Direct_ad_return_1d ,
Indirect_ad_return_1d ,
Total_ad_return_14d ,
Direct_ad_return_14d ,
Indirect_Ad_Return_14d ,
Campaign_Start_Date ,
Campaign_end_Date  ,
null as file_name,
current_timestamp as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
    {% endif %}
)
select * from final