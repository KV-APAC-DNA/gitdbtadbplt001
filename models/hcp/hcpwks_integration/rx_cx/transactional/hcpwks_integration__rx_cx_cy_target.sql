with 
rx_cx_consist_ratio_region_cohort as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_consist_ratio_region_cohort') }}
),
rx_cx_prod_reg_cutoffs_stable as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_prod_reg_cutoffs_stable') }}
),
final as 
(
    SELECT tmp.urc,
       tmp.rx_product,
       tmp.year,
       tmp.quarter,
       tmp.lysq_ach_NR,
       tmp.lysq_qty,
       tmp.lysq_presc,
       (CASE WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) != 0 THEN 1
             WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) = 0 THEN 1
             WHEN ratio = 0 THEN lysq_presc
             WHEN ratio <= lower_cut AND NVL(lysq_presc,0) != 0 AND NVL(lysq_qty,0) != 0 THEN lysq_presc
             WHEN ratio >= upper_cut AND NVL(lysq_presc != 0,0) AND NVL(lysq_qty != 0,0) THEN (lysq_qty/upper_cut)
             WHEN ratio > lower_cut AND ratio < upper_cut THEN lysq_presc
        END) AS target_presc,
       (CASE WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) != 0 THEN lysq_qty
             WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) = 0 THEN sales_percentile_25
             WHEN ratio = 0 THEN sales_percentile_25
             WHEN ratio <= lower_cut AND NVL(lysq_presc,0) != 0 AND NVL(lysq_qty,0) != 0 THEN (lower_cut * lysq_presc)
             WHEN ratio >= upper_cut AND lysq_presc != 0 AND lysq_qty != 0 THEN lysq_qty
             WHEN ratio > lower_cut AND ratio < upper_cut THEN lysq_qty
        END) AS target_qty,
       (CASE WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) != 0 THEN 1
             WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) = 0 THEN 3
             WHEN ratio = 0 THEN 2
             WHEN ratio <= lower_cut AND NVL(lysq_presc,0) != 0 AND NVL(lysq_qty,0) != 0 THEN 4
             WHEN ratio >= upper_cut AND lysq_presc != 0 AND lysq_qty != 0 THEN 5
             WHEN ratio > lower_cut AND ratio < upper_cut THEN 6
        END)::number(18,0) AS "case",
       (CASE WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) != 0 THEN 'convert'
             WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) = 0 THEN 'convert'
             WHEN ratio = 0 THEN 'maintain'
             WHEN ratio <= lower_cut AND NVL(lysq_presc,0) != 0 AND NVL(lysq_qty,0) != 0 THEN 'maintain'
             WHEN ratio >= upper_cut AND NVL(lysq_presc != 0,0) AND NVL(lysq_qty != 0,0) THEN 'need prescriptions'
             WHEN ratio > lower_cut AND ratio < upper_cut THEN 'maintain'
        END) AS prescription_action,
       (CASE WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) != 0 THEN 'maintain'
             WHEN NVL(lysq_presc,0) = 0 AND NVL(lysq_qty,0) = 0 THEN 'basic sell-in'
             WHEN ratio = 0 THEN 'basic sell-in'
             WHEN ratio <= lower_cut AND NVL(lysq_presc,0) != 0 AND NVL(lysq_qty,0) != 0 THEN 'grow sales'
             WHEN ratio >= upper_cut AND NVL(lysq_presc != 0,0) AND NVL(lysq_qty != 0,0) THEN 'maintain'
             WHEN ratio > lower_cut AND ratio < upper_cut THEN 'maintain'
        END) AS sales_action
FROM   rx_cx_consist_ratio_region_cohort tmp
LEFT JOIN rx_cx_prod_reg_cutoffs_stable cutoff
       ON tmp.rx_product = cutoff.rx_product
      AND tmp.region_cohort = cutoff.region_cohort
)
select * from final