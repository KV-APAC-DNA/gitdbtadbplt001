with 
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
itg_in_rcustomerroute as 
(
    select * from {{ ref('inditg_integration__itg_in_rcustomerroute') }}
),
itg_in_rroute as 
(
    select * from {{ ref('inditg_integration__itg_in_rroute') }}
),
itg_in_rsalesmanroute as 
(
    select * from {{ ref('inditg_integration__itg_in_rsalesmanroute') }}
),
itg_in_rsalesman as 
(
    select * from {{ ref('inditg_integration__itg_in_rsalesman') }}
),
final as 
(
    SELECT s.smcode as salesman_code_sales,
       s.smname as salesman_name_sales,
       s.distcode,
       ret.rtruniquecode as urc,
       null as skuhiervaluecode,
       ROW_NUMBER() OVER (PARTITION BY URC order by null) AS rnk
FROM edw_retailer_dim ret,
     itg_in_rcustomerroute cr,
     itg_in_rroute r,
     itg_in_rsalesmanroute rs,
     itg_in_rsalesman s
WHERE cr.routetype::CHARACTER(10) = 'S'::char
AND   cr.distcode::TEXT = r.distcode::TEXT
AND   cr.rmcode::TEXT = r.rmcode::TEXT
AND   rs.distrcode::TEXT = r.distcode::TEXT
AND   rs.routecode::TEXT = r.rmcode::TEXT
AND   s.distcode::TEXT = rs.distrcode::TEXT
AND   s.smcode::TEXT = rs.salesmancode::TEXT
AND   s.status::CHARACTER(10) = 'Y'::char
AND   ret.retailer_code =  "substring"(cr.rtrcode::text, 7)
AND   ret.customer_code = cr.distcode
AND   ret.actv_flg = 'Y'
)
select * from final