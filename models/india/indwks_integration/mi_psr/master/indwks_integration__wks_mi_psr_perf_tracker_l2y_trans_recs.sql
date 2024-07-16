with v_rpt_rt_sales as
(
    select * from {{ ref('indedw_integration__v_rpt_rt_sales') }}
),
final as
(
    SELECT t1.superstockiest_code, t1.rtr_code, t1.psr_code, t1.retailer_code
    FROM v_rpt_rt_sales t1
    INNER JOIN (SELECT retailer_code, MAX(order_date) AS max_order_date
                FROM v_rpt_rt_sales
                WHERE EXTRACT(YEAR FROM order_date) >= 2022
                GROUP BY 1) t2
            ON t1.retailer_code = t2.retailer_code
        AND t1.order_date = t2.max_order_date
    GROUP BY 1,2,3,4
)
select * from final