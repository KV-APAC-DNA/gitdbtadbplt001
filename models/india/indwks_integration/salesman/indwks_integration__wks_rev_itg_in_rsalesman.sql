with itg_in_rcustomerroute as
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
    SELECT cr.distcode,
       "substring"(cr.rtrcode::TEXT,7) AS rtrcode,
       cr.rmcode,
       r.rmname,
       s.smcode,
       s.smname,
       s.status,
       s.uniquesalescode
FROM itg_in_rcustomerroute cr,
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
)
select * from final
