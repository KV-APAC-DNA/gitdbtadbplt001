with 
rx_cx_consist_ratio_region_cohort as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_consist_ratio_region_cohort') }}
),
final as 
(
    SELECT urc,
       rx_product,
       year,
       quarter,
       lysq_ach_NR,
       lysq_qty,
       lysq_presc,    
       outlet_consistency_tag,
       region_cohort,
       ratio
FROM  rx_cx_consist_ratio_region_cohort reg
WHERE outlet_consistency_tag = ('CONSISTENT OUTLET')
)
select * from final