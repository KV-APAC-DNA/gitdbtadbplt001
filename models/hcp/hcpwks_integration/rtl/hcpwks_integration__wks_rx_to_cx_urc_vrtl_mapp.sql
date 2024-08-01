with itg_hcp360_in_ventasys_rtlmaster as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_rtlmaster') }}
),
itg_hcp360_in_ventasys_hcprtl as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcprtl') }}
),
final as(
    SELECT 
       urc,
       v_custid_rtl,
       cust_name,
       cust_endtered_date,
       TO_CHAR(convert_timezone('UTC', current_timestamp())::timestamp_ntz(9),'YYYYMM') AS period,
       'NA' AS flex1,
       'NA' AS flex2
FROM (SELECT urc,
             v_custid_rtl,
             cust_name,
             cust_endtered_date,
             ROW_NUMBER() OVER (PARTITION BY urc ORDER BY v_custid_rtl DESC) AS rnk   
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
)
select * from final