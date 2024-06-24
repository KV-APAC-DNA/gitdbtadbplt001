with 
rx_cx_hcp_presc as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_hcp_presc') }}
),
itg_hcp360_in_ventasys_hcprtl as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_hcprtl
), --hcp
final as 
(
    SELECT tbl.fisc_yr,
       tbl.qtr,
       tbl.month,
       hcprtl.v_custid_rtl,
       tbl.product,
       SUM(tbl.presc_fair_share) AS rx_units
FROM (SELECT fisc_yr,
             qtr,
             month,
             hcp_id,
             product,
             (hcp_presc / cnt_tbl.vent_ret_count) AS presc_fair_share
      FROM rx_cx_hcp_presc hcp
      INNER JOIN (SELECT v_custid_dr,
                         COUNT(v_custid_rtl) AS vent_ret_count
                  FROM itg_hcp360_in_ventasys_hcprtl
                  GROUP BY v_custid_dr
                  ) cnt_tbl
              ON hcp.hcp_id = cnt_tbl.v_custid_dr
      ) tbl
INNER JOIN itg_hcp360_in_ventasys_hcprtl hcprtl
        ON tbl.hcp_id = hcprtl.v_custid_dr
GROUP BY tbl.fisc_yr,
         tbl.qtr,
         tbl.month,
         hcprtl.v_custid_rtl,
         tbl.product
)
select * from final