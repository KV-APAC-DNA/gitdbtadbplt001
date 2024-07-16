with itg_hcp360_in_ventasys_rtlmaster as
(
    select * from DEV_DNA_CORE.SNAPINDITG_INTEGRATION.ITG_HCP360_IN_VENTASYS_RTLMASTER
),
itg_hcp360_in_ventasys_hcprtl as
(
    select * from DEV_DNA_CORE.SNAPINDITG_INTEGRATION.ITG_HCP360_IN_VENTASYS_HCPRTL
),
transformed as
(
SELECT urc,
       v_custid_rtl,
       cust_name,
       cust_endtered_date,
       TO_CHAR(CURRENT_DATE,'YYYYMM') AS period,
       'NA' AS flex1,
       'NA' AS flex2
FROM (SELECT urc,
             v_custid_rtl,
             cust_name,
             cust_endtered_date,
             ROW_NUMBER() OVER (PARTITION BY urc ORDER BY v_custid_rtl DESC) AS rnk    -- eliminate duplicate URC - V_CUSTID_RTL mapping
      FROM itg_hcp360_in_ventasys_rtlmaster)
WHERE urc IS NOT NULL
AND   rnk = 1
AND   v_custid_rtl IN (SELECT v_custid_rtl
                       FROM itg_hcp360_in_ventasys_hcprtl
                       GROUP BY 1)
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7
),
final as 
(   
    select * from transformed
)
select * from final 
