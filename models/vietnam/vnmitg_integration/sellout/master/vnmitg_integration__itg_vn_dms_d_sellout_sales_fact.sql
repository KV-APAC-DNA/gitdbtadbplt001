{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_id','outlet_id','order_no','line_number'],
        pre_hook= " {% if is_incremental()%}
        delete from {{this}} where (dstrbtr_id, outlet_id, order_no, line_number) in ( select dstrbtr_id, outlet_id, order_no, line_number from {{ ref('vnmwks_integration__wks_itg_vn_dms_d_sellout_sales_fact') }}); {%endif%}"
    )
}}


with wks_itg_vn_dms_d_sellout_sales_fact as(
    select *, dense_rank() over (partition by dstrbtr_id,outlet_id,order_no,line_number order by file_name desc) rnk
    from {{ ref('vnmwks_integration__wks_itg_vn_dms_d_sellout_sales_fact') }}
    qualify rnk = 1
),
transformed as(
    SELECT
        dstrbtr_id::varchar(30) as dstrbtr_id,
        TRIM(cntry_code)::varchar(2) as cntry_code,
        outlet_id::varchar(30) as outlet_id,
        to_date(order_date, 'MM/DD/YYYY HH12:MI:SS AM') AS order_date,
        to_date(invoice_date, 'MM/DD/YYYY HH12:MI:SS AM') AS invoice_date,
        order_no::varchar(30) as order_no,
        invoice_no::varchar(30) as invoice_no,
        sales_route_id::varchar(50) as sales_route_id,
        sale_route_name::varchar(100) as sale_route_name,
        sales_group::varchar(50) as sales_group,
        salesrep_id::varchar(30) as salesrep_id,
        salesrep_name::varchar(50) as salesrep_name,
        material_code::varchar(50) as material_code,
        uom::varchar(30) as uom,
        CAST(gross_price AS DECIMAL(15, 4)) as gross_price,
        CAST(orderqty AS DECIMAL) as orderqty,
        CAST(quantity AS DECIMAL) as quantity,
        CAST(total_sellout_afvat_bfdisc AS DECIMAL(15, 4)) as total_sellout_afvat_bfdisc,
        CAST(discount AS DECIMAL(15, 4)) as discount,
        CAST(total_sellout_afvat_afdisc AS DECIMAL(15, 4)) as total_sellout_afvat_afdisc,
        line_number::varchar(10) as line_number,
        RTRIM(promotion_id, ',')::varchar(50) AS promotion_id,
        RTRIM(status, ',')::varchar(1) AS status,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        CURRENT_TIMESTAMP()::timestamp_ntz(9) AS updt_dttm,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    FROM wks_itg_vn_dms_d_sellout_sales_fact
)
select * from transformed