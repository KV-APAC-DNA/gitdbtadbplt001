with edw_retailer_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
v_retailer_udc_map as
(
    select * from {{ ref('indedw_integration__v_retailer_udc_map') }}
),
transformed as
(
    SELECT ret.urc,
        udc.udc_orslcac2021 AS udc_orslcac2021_flag
    FROM (SELECT customer_code,
                retailer_code,
                rtruniquecode AS urc,
                row_number() OVER (PARTITION BY customer_code,retailer_code ORDER BY end_date DESC) AS rn
        FROM edw_retailer_dim) ret
    INNER JOIN (SELECT customer_code_udc,
                    retailer_code_udc,
                    udc_orslcac2021
                FROM v_retailer_udc_map
                WHERE udc_orslcac2021 = 'YES'
                GROUP BY 1,2,3) udc
            ON ret.customer_code = udc.customer_code_udc
        AND ret.retailer_code = udc.retailer_code_udc
        AND ret.rn = 1
    GROUP BY 1,2
),
final as 
(   
    select * from transformed
)
select * from final
