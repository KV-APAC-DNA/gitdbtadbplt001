with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_d_sellout_sales_fact') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_d_sellout_sales_fact__duplicate_test')}}
    )
),
trans as(
    select dstrbtr_id,
            outlet_id,
            order_no,
            line_number
    from source
    group by dstrbtr_id,
        outlet_id,
        order_no,
        line_number
    having count(*) > 1
),
final as(
    select 
        dstrbtr_id as dstrbtr_id,
        cntry_code as cntry_code,
        outlet_id as outlet_id,
        order_date as order_date,
        invoice_date as invoice_date,
        order_no as order_no,
        invoice_no as invoice_no,
        sales_route_id as sales_route_id,
        sale_route_name as sale_route_name,
        sales_group as sales_group,
        salesrep_id as salesrep_id,
        salesrep_name as salesrep_name,
        material_code as material_code,
        uom as uom,
        gross_price as gross_price,
        orderqty as orderqty,
        quantity as quantity,
        total_sellout_afvat_bfdisc as total_sellout_afvat_bfdisc,
        discount as discount,
        total_sellout_afvat_afdisc as total_sellout_afvat_afdisc,
        line_number as line_number,
        promotion_id as promotion_id,
        status as status,
        run_id as run_id,
        curr_date as crtd_dttm
    from source
    where (dstrbtr_id, outlet_id, order_no, line_number) IN (select * from trans)
)
select * from final