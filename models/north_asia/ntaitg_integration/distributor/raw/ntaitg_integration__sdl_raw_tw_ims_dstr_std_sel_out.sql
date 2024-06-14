{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_sel_out') }}
),
     
final as (
    select 
        transaction_date,
        distributor_code,
        distributor_name,
        distributors_customer_code,
        distributors_customer_name,
        distributors_product_code,
        distributors_product_name,
        report_period_start_date,
        report_period_end_date,
        ean,
        uom,
        unit_price,
        sales_amount,
        sales_qty,
        return_qty,
        return_amount,
        sales_rep_code,
        sales_rep_name,
        crt_dttm,
        current_timestamp as upd_dttm,
        null as filename,
        null as run_id
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final