with 
rx_cx_sales_presc_ideal_ratio as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_sales_presc_ideal_ratio') }}
),
final as 
(
    SELECT tmp.rx_product,
       tmp.year,
       tmp.quarter,
       tmp.region_cohort,
       ratio_percentile.ratio_percentile_25 AS lower_cut,
       ratio_percentile.ratio_percentile_75 AS upper_cut,
       sales_percentile.sales_percentile_25,
       COUNT(tmp.urc) AS outlet_count
FROM  rx_cx_sales_presc_ideal_ratio tmp
INNER JOIN (SELECT rx_product,
                   region_cohort,
                   percentile_cont(0.25) within group (order by ratio asc) as ratio_percentile_25,
                   percentile_cont(0.75) within group (order by ratio asc) as ratio_percentile_75
            FROM  rx_cx_sales_presc_ideal_ratio
            GROUP BY rx_product,
                     region_cohort
           ) ratio_percentile
        ON tmp.rx_product = ratio_percentile.rx_product
       AND tmp.region_cohort = ratio_percentile.region_cohort
INNER JOIN (SELECT rx_product,
                   region_cohort,
                   percentile_cont(0.25) within group (order by lysq_qty asc) as sales_percentile_25
            FROM  rx_cx_sales_presc_ideal_ratio
            GROUP BY rx_product,
                     region_cohort
           ) sales_percentile
        ON tmp.rx_product = sales_percentile.rx_product
       AND tmp.region_cohort = sales_percentile.region_cohort
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7
)
select * from final