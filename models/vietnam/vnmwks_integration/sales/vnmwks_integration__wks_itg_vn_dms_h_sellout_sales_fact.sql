with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_h_sellout_sales_fact') }}
),
cte as(
    SELECT
      dstrbtr_id,
      cntry_code,
      outlet_id,
      order_date,
      invoice_date,
      order_no,
      invoice_no,
      sellout_afvat_bfdisc,
      total_discount,
      invoice_discount,
      sellout_afvat_afdisc,
      status,
      run_id,
      curr_date
    FROM source
    WHERE
      (dstrbtr_id, outlet_id, order_no) IN (
        SELECT
          dstrbtr_id,
          outlet_id,
          order_no
        FROM source
        GROUP BY
          dstrbtr_id,
          outlet_id,
          order_no
        HAVING
          COUNT(*) > 1
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
        sellout_afvat_bfdisc,
        total_discount,
        invoice_discount,
        sellout_afvat_afdisc,
        status,
        run_id,
        curr_date
    from source minus
    select 
        dstrbtr_id,
        cntry_code,
        outlet_id,
        order_date,
        invoice_date,
        order_no,
        invoice_no,
        sellout_afvat_bfdisc,
        total_discount,
        invoice_discount,
        sellout_afvat_afdisc,
        status,
        run_id,
        curr_date
    from cte
  ) 
  union all 
    SELECT
        dstrbtr_id,
        cntry_code,
        outlet_id,
        order_date,
        invoice_date,
        order_no,
        invoice_no,
        sellout_afvat_bfdisc,
        total_discount,
        invoice_discount,
        sellout_afvat_afdisc,
        status,
        run_id,
        curr_date
    FROM cte
    WHERE
    status = 'V'
) 
select * from cte2
