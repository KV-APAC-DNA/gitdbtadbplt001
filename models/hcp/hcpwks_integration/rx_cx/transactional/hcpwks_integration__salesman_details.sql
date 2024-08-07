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
AND   rtrim(cr.distcode::TEXT) = rtrim(r.distcode::TEXT)
AND   rtrim(cr.rmcode::TEXT) = rtrim(r.rmcode::TEXT)
AND   rtrim(rs.distrcode::TEXT) = rtrim(r.distcode::TEXT)
AND   rtrim(rs.routecode::TEXT) = rtrim(r.rmcode::TEXT)
AND   rtrim(s.distcode::TEXT) = rtrim(rs.distrcode::TEXT)
AND   rtrim(s.smcode::TEXT) = rtrim(rs.salesmancode::TEXT)
AND   s.status::CHARACTER(10) = 'Y'::char
AND   rtrim(ret.retailer_code) =  rtrim("substring"(cr.rtrcode::text, 7))
AND   rtrim(ret.customer_code) = rtrim(cr.distcode)
AND   ret.actv_flg = 'Y'
)
select * from final