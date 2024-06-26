with 
itg_hcp360_in_ventasys_rtlmaster as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_rtlmaster
), --hcp
rx_cx_consist_ratio_region_cohort as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_consist_ratio_region_cohort') }}
),
final as 
(
    SELECT inn.urc,
       inn.rx_product
FROM (SELECT DISTINCT urc, 'ORSL (Core)' AS rx_product
      FROM itg_hcp360_in_ventasys_rtlmaster tmp1
      WHERE tmp1.urc IS NOT NULL
      UNION 
      SELECT DISTINCT urc, 'ORSL (Plus)' AS rx_product
      FROM itg_hcp360_in_ventasys_rtlmaster tmp2
      WHERE tmp2.urc IS NOT NULL
      UNION 
      SELECT DISTINCT urc, 'ORSL (Rehydrate)' AS rx_product
      FROM itg_hcp360_in_ventasys_rtlmaster tmp3
      WHERE tmp3.urc IS NOT NULL
      ) inn
EXCEPT
SELECT tmp.urc, tmp.rx_product
FROM  rx_cx_consist_ratio_region_cohort tmp
GROUP BY 1,2
)
select * from final