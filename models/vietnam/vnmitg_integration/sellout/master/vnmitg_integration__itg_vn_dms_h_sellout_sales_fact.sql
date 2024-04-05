{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_id','outlet_id','order_no'],
        pre_hook= "delete from {{this}} where (dstrbtr_id, outlet_id, order_no) in ( select dstrbtr_id, outlet_id, order_no from {{ ref('vnmwks_integration__wks_itg_vn_dms_h_sellout_sales_fact') }});"
    )
}}


with wks_itg_vn_dms_h_sellout_sales_fact as(
    select * from {{ ref('vnmwks_integration__wks_itg_vn_dms_h_sellout_sales_fact') }}
),
transformed as(
    select
        dstrbtr_id::varchar(30) as dstrbtr_id,
        TRIM(cntry_code)::varchar(2) as cntry_code,
        outlet_id::varchar(30) as outlet_id,
        to_date(order_date, 'MM/DD/YYYY HH12:MI:SS AM') AS order_date,
        to_date(invoice_date, 'MM/DD/YYYY HH12:MI:SS AM') AS invoice_date,
        order_no::varchar(30) as order_no,
        invoice_no::varchar(30) as invoice_no,
        CAST(sellout_afvat_bfdisc AS DECIMAL(15, 4)) as sellout_afvat_bfdisc,
        CAST(total_discount AS DECIMAL(15, 4)) as total_discount,
        CAST(invoice_discount AS DECIMAL(15, 4)) as invoice_discount,
        CAST(sellout_afvat_afdisc AS DECIMAL(15, 4)) as sellout_afvat_afdisc,
        status::varchar(1) as status,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) AS updt_dttm,
        run_id::number(14,0) as run_id 
    FROM wks_itg_vn_dms_h_sellout_sales_fact
)
select * from transformed