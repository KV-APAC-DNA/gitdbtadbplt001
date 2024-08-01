with 
rx_cx_prod_reg_cutoffs as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_prod_reg_cutoffs') }}
),
itg_rx_cx_prod_reg_std_cutoffs as 
(
    select * from {{ ref('hcpitg_integration__itg_rx_cx_prod_reg_std_cutoffs') }}
),
final as 
(
    SELECT 
       COALESCE(cur_cutoff.rx_product,std_cutoff.rx_product) AS rx_product,
       COALESCE(cur_cutoff.year,std_cutoff.year) AS year,
       COALESCE(cur_cutoff.quarter,std_cutoff.quarter) AS quarter,
       COALESCE(cur_cutoff.region_cohort,std_cutoff.region_cohort) AS region_cohort,
       COALESCE(cur_cutoff.lower_cut,std_cutoff.lower_cut) AS lower_cut,
       COALESCE(cur_cutoff.upper_cut,std_cutoff.upper_cut) AS upper_cut,
       COALESCE(cur_cutoff.sales_percentile_25,std_cutoff.sales_percentile_25) AS sales_percentile_25,
       COALESCE(cur_cutoff.outlet_count,std_cutoff.outlet_count) AS outlet_count
FROM itg_rx_cx_prod_reg_std_cutoffs std_cutoff
LEFT JOIN rx_cx_prod_reg_cutoffs cur_cutoff
       ON std_cutoff.rx_product = cur_cutoff.rx_product
      AND std_cutoff.region_cohort = cur_cutoff.region_cohort
)
select * from final