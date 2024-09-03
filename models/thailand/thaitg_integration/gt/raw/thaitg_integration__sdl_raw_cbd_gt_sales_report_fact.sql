{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_cbd_gt_sales_report_fact') }}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_sales_report_fact__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_sales_report_fact__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_sales_report_fact__test_format') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_sales_report_fact__test_format_2') }}
        )
),
final as (
    select 
        bu,
        client,
        sub_client,
        product_code,
        product_name,
        billing_no,
        billing_date,
        batch_no,
        expiry_date,
        customer_code,
        customer_name,
        distribution_channel,
        customer_group,
        province,
        sales_qty,
        foc_qty,
        net_price,
        net_sales,
        sales_rep_no,
        order_no,
        return_reason,
        payment_term,
        filename as filename,
        run_id,
        crt_dttm 
    from source
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final