with 
itg_hcp360_in_ventasys_rtlmaster as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_rtlmaster
),
final as 
(
    SELECT *
FROM (SELECT urc,
             v_custid_rtl,
             is_active,
             ROW_NUMBER() OVER (PARTITION BY urc ORDER BY v_custid_rtl DESC) AS rnk
      FROM itg_hcp360_in_ventasys_rtlmaster
      WHERE urc IS NOT NULL)
WHERE rnk = 1
)
select * from final