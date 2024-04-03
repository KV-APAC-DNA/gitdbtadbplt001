{#
Before running this model 
#}

{{
    config(
        pre_hook="{{build_wks_cbd_gt_sales_report_fact_pre_load()}}"
    )
}}

with wks_cbd_gt_sales_report_fact_pre_load as (
    select * from {{ source('thawks_integration', 'wks_cbd_gt_sales_report_fact_pre_load') }}
),
sdl_cbd_gt_sales_report_fact as (
    select *,
        MD5(coalesce(UPPER(product_code),'N/A') ||coalesce(to_date (billing_date , 'dd/mm/yyyy'),'9999-12-31')||coalesce(upper (batch_no),'N/A') ||coalesce(upper (customer_code),'N/A')
  ||coalesce(upper (sales_rep_no),'N/A') ||coalesce(order_no,'N/A')) AS hashkey 
     from {{ source('thasdl_raw', 'sdl_cbd_gt_sales_report_fact') }}
)

SELECT 
    wks.bu,
    wks.client,
    wks.sub_client,
    wks.product_code,
    wks.product_name,
    wks.billing_no,
    wks.billing_date,
    wks.batch_no,
    wks.expiry_date,
    wks.customer_code,
    wks.customer_name,
    wks.distribution_channel,
    wks.customer_group,
    wks.province,
    wks.sales_qty,
    wks.foc_qty,
    wks.net_price,
    wks.net_sales,
    wks.sales_rep_no,
    wks.order_no,
    wks.return_reason,
    wks.payment_term,
    'D' AS load_flag,
    wks.filename,
    wks.run_id,
    wks.crt_dttm
FROM wks_cbd_gt_sales_report_fact_pre_load wks
WHERE NOT EXISTS (SELECT 1
                  FROM sdl_cbd_gt_sales_report_fact sdl
                  WHERE wks.hashkey = sdl.hashkey)
UNION ALL
SELECT 
    sdl.bu,
    sdl.client,
    sdl.sub_client,
    sdl.product_code,
    sdl.product_name,
    sdl.billing_no,
    to_date(sdl.billing_date , 'dd/mm/yyyy') as billing_date,
    sdl.batch_no,
    to_date(sdl.expiry_date , 'dd/mm/yyyy') as expiry_date,
    sdl.customer_code,
    sdl.customer_name,
    sdl.distribution_channel,
    sdl.customer_group,
    sdl.province,
    cast (sdl.sales_qty as NUMERIC(18,4)) as sales_qty,
    cast (sdl.foc_qty as NUMERIC(18,4)) as foc_qty,
    cast (floor(sdl.net_price,4) as NUMERIC(18,4)) as net_price,
    cast (floor(sdl.net_sales,4) as NUMERIC(18,4)) as net_sales,
    sdl.sales_rep_no,
    sdl.order_no,
    sdl.return_reason,
    sdl.payment_term,
    'I' AS load_flag,
    sdl.filename,
    sdl.run_id,
    sdl.crt_dttm
FROM sdl_cbd_gt_sales_report_fact sdl
WHERE NOT EXISTS (SELECT 1
                  FROM wks_cbd_gt_sales_report_fact_pre_load wks
                  WHERE sdl.hashkey = wks.hashkey)
UNION ALL

SELECT 
    sdl.bu,
    sdl.client,
    sdl.sub_client,
    sdl.product_code,
    sdl.product_name,
    sdl.billing_no,
    to_date(sdl.billing_date , 'dd/mm/yyyy') as billing_date,
    sdl.batch_no,
    to_date(sdl.expiry_date , 'dd/mm/yyyy') as expiry_date,
    sdl.customer_code,
    sdl.customer_name,
    sdl.distribution_channel,
    sdl.customer_group,
    sdl.province,
    cast (sdl.sales_qty as NUMERIC(18,4)) as sales_qty,
    cast (sdl.foc_qty as NUMERIC(18,4)) as foc_qty,
    cast (sdl.net_price as NUMERIC(18,4)) as net_price,
    cast (sdl.net_sales as NUMERIC(18,4)) as net_sales,
    sdl.sales_rep_no,
    sdl.order_no,
    sdl.return_reason,
    sdl.payment_term,
    'U' AS load_flag,
    sdl.filename,
    sdl.run_id,
    sdl.crt_dttm
FROM sdl_cbd_gt_sales_report_fact sdl,
wks_cbd_gt_sales_report_fact_pre_load wks
              WHERE sdl.hashkey = wks.hashkey