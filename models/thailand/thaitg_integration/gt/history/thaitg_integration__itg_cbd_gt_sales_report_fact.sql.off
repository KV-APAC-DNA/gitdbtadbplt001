
{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="DELETE FROM {{this}} WHERE (UPPER(bu),billing_date) IN (SELECT DISTINCT UPPER(bu),billing_date FROM {{ ref('thawks_integration__wks_cbd_gt_sales_report_fact_flag_incl') }} );"
    )
}}

with source as (
    select  * from {{ ref('thawks_integration__wks_cbd_gt_sales_report_fact_flag_incl') }}
),
final as (
    SELECT 
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
        net_sales as net_sales_usd,
        sales_rep_no,
        order_no,
        return_reason,
        payment_term,
        load_flag,
        filename,
        run_id,
        crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

select * from final

