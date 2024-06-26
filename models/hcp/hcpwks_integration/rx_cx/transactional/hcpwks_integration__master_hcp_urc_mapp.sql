with 
hcp_master_data as 
(
    select * from {{ ref('hcpwks_integration__hcp_master_data') }}
),
itg_hcp360_in_ventasys_hcprtl as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_hcprtl
), --hcp
itg_hcp360_in_ventasys_rtlmaster as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_rtlmaster
), --hcp
final as 
(
    SELECT inn.*
FROM (SELECT hcp_mast.hcp,
             hcp_mast.hcp_name,
             hcp_mast.emp_name,
             hcp_mast.emp_id,
             hcp_mast.region_vent,
             hcp_mast.territory_vent,
             hcp_mast.zone_vent,
             rtlm.urc,
             row_number() over(partition by rtlm.urc order by emp_id) AS rnk
      FROM hcp_master_data hcp_mast
      INNER JOIN itg_hcp360_in_ventasys_hcprtl hcprtl
              ON hcp_mast.hcp = hcprtl.v_custid_dr
      INNER JOIN (SELECT urc,
                         v_custid_rtl,
                         ROW_NUMBER() OVER (PARTITION BY urc ORDER BY v_custid_rtl DESC) AS rnk
                  FROM itg_hcp360_in_ventasys_rtlmaster
                  WHERE urc IS NOT NULL 
                    AND TRIM(urc) <> ''
                  ) rtlm 
              ON hcprtl.v_custid_rtl = rtlm.v_custid_rtl
             AND rtlm.rnk = 1
      ) inn
WHERE inn.rnk = 1
)
select * from final