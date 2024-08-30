with sdl_vn_dms_d_sellout_sales_fact as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_d_sellout_sales_fact') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_d_sellout_sales_fact__duplicate_test')}}
    )
),
 cte as(
  select 
    dstrbtr_id, 
    cntry_code, 
    outlet_id, 
    order_date, 
    invoice_date, 
    order_no, 
    invoice_no, 
    sales_route_id, 
    sale_route_name, 
    sales_group, 
    salesrep_id, 
    salesrep_name, 
    material_code, 
    uom, 
    gross_price, 
    orderqty, 
    quantity, 
    total_sellout_afvat_bfdisc, 
    discount, 
    total_sellout_afvat_afdisc, 
    line_number, 
    promotion_id, 
    status, 
    run_id,
    curr_date,
    file_name
  from sdl_vn_dms_d_sellout_sales_fact 
  where 
    (
      dstrbtr_id, outlet_id, order_no, line_number
    ) IN (
      select 
        dstrbtr_id, 
        outlet_id, 
        order_no, 
        line_number 
      from sdl_vn_dms_d_sellout_sales_fact
      group by 
        dstrbtr_id, 
        outlet_id, 
        order_no, 
        line_number 
      having 
        count(*) > 1
    )
), 
cte2 as(
  (
    select 
      dstrbtr_id, 
      cntry_code, 
      outlet_id, 
      order_date, 
      invoice_date, 
      order_no, 
      invoice_no, 
      sales_route_id, 
      sale_route_name, 
      sales_group, 
      salesrep_id, 
      salesrep_name, 
      material_code, 
      uom, 
      gross_price, 
      orderqty, 
      quantity, 
      total_sellout_afvat_bfdisc, 
      discount, 
      total_sellout_afvat_afdisc, 
      line_number, 
      promotion_id, 
      status, 
      run_id,
      curr_date,
      file_name
    from sdl_vn_dms_d_sellout_sales_fact minus 
    select 
      dstrbtr_id, 
      cntry_code, 
      outlet_id, 
      order_date, 
      invoice_date, 
      order_no, 
      invoice_no, 
      sales_route_id, 
      sale_route_name, 
      sales_group, 
      salesrep_id, 
      salesrep_name, 
      material_code, 
      uom, 
      gross_price, 
      orderqty, 
      quantity, 
      total_sellout_afvat_bfdisc, 
      discount, 
      total_sellout_afvat_afdisc, 
      line_number, 
      promotion_id, 
      status, 
      run_id,
      curr_date,
      file_name
    from cte
  ) 
  union all 
  select 
    dstrbtr_id, 
    cntry_code, 
    outlet_id, 
    order_date, 
    invoice_date, 
    order_no, 
    invoice_no, 
    sales_route_id, 
    sale_route_name, 
    sales_group, 
    salesrep_id, 
    salesrep_name, 
    material_code, 
    uom, 
    gross_price, 
    orderqty, 
    quantity, 
    total_sellout_afvat_bfdisc, 
    discount, 
    total_sellout_afvat_afdisc, 
    line_number, 
    promotion_id, 
    status, 
    run_id,
    curr_date,
    file_name
  from cte 
  where status = 'V'
) 
select * from cte2
