with 
rx_cx_rxrtl_qtly_agg as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_rxrtl_qtly_agg') }}
),
itg_hcp360_in_ventasys_rtlmaster as 
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_rtlmaster') }}
), 
final as 
(
    SELECT  
        rtl.urc,
        tmp.rx_product,
        SUM(tmp.rx_units) AS rx_units,
        tmp.year,
        tmp.quarter,
        count(NULLIF(round(rx_units,2),0.00)) AS presc_mnth_cnt
FROM  rx_cx_rxrtl_qtly_agg tmp
INNER JOIN (SELECT urc,
                   v_custid_rtl,
                   ROW_NUMBER() OVER (PARTITION BY urc ORDER BY v_custid_rtl DESC) AS rnk
            FROM itg_hcp360_in_ventasys_rtlmaster
            ) rtl
        ON tmp.v_custid_rtl = rtl.v_custid_rtl
       AND rtl.rnk = 1
WHERE rtl.urc IS NOT NULL 
  AND TRIM(rtl.urc) <> ''
GROUP BY rtl.urc,
         tmp.rx_product,
         tmp.year,
         tmp.quarter
)
select * from final