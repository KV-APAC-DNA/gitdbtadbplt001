with wks_mi_psr_ret_dim as 
(
    select * from {{ ref('indwks_integration__wks_mi_psr_ret_dim') }}
),
v_rpt_sales_details as
(
    select * from {{ ref('indedw_integration__v_rpt_sales_details') }}
),
final as
(
    SELECT ret.*, lat_cust.latest_customer_code
    FROM wks_mi_psr_ret_dim ret
    LEFT JOIN (SELECT rtruniquecode,
                    latest_customer_code
            FROM v_rpt_sales_details
                WHERE  UPPER(Channel_Name) IN ('SUB-STOCKIEST','URBAN SUBD','RURAL WHOLESALE','PHARMA RESELLER SUBD') and status_desc = 'Active'
            GROUP BY rtruniquecode,latest_customer_code) lat_cust
        ON ret.rtruniquecode = lat_cust.rtruniquecode
)
select * from final