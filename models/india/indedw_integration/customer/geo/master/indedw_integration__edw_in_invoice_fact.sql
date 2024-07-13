{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} as edw_in_invoice_fact
                    using {{ ref('indwks_integration__wks_in_invoice_fact') }} as wks_in_invoice_fact
                    where 	wks_in_invoice_fact.customer_code = edw_in_invoice_fact.customer_code 
                    and wks_in_invoice_fact.product_code = edw_in_invoice_fact.product_code 
                    and wks_in_invoice_fact.invoice_no = edw_in_invoice_fact.invoice_no 
                    and nvl(wks_in_invoice_fact.invoice_date,'9999-01-01') = nvl(edw_in_invoice_fact.invoice_date,'9999-01-01')
                    and wks_in_invoice_fact.invoice_val = edw_in_invoice_fact.invoice_val
                    and wks_in_invoice_fact.invoice_qty = edw_in_invoice_fact.invoice_qty
                and wks_in_invoice_fact.chng_flg in ('U');
                    {% endif %}"
    )
}}
with wks_in_invoice_fact as 
(
    select * from {{ ref('indwks_integration__wks_in_invoice_fact') }}
),
final as 
(
    select 
	customer_code::varchar(10) as customer_code,
	product_code::varchar(18) as product_code,
	invoice_no::varchar(10) as invoice_no,
	invoice_date::date as invoice_date,
	invoice_val::number(38,17) as invoice_val,
	invoice_qty::float as invoice_qty,
	wt_invoice_qty::float as wt_invoice_qty,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from wks_in_invoice_fact
)
select * from final