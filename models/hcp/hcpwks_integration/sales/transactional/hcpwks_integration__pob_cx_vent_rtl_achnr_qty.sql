with pob_cx_sales_achnr_qty as
(
    select * from {{ ref('hcpwks_integration__pob_cx_sales_achnr_qty') }}
),
itg_hcp360_in_ventasys_rtlmaster as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_rtlmaster
), --hcp ref
final as 
(
    SELECT urc,
       prod_vent,
       ach_NR,
       qty,
       fisc_yr,
       qtr,
       month,
       week,
       invoice_date
FROM pob_cx_sales_achnr_qty 
WHERE urc IN (SELECT DISTINCT urc::text
              FROM   itg_hcp360_in_ventasys_rtlmaster
              WHERE urc IS NOT NULL)
)
select * from final