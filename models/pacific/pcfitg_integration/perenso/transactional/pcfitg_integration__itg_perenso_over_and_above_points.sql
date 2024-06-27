-- REQ000000180060: SSR O&A display points content change
with source as
(
SELECT 'Full gondola end'AS display_type, 140 AS points UNION ALL
SELECT 'Half gondola end'AS display_type, 90 AS points UNION ALL
SELECT 'Full wing'AS display_type, 80 AS points UNION ALL
SELECT 'Half wing'AS display_type, 50 AS points UNION ALL
SELECT 'Full shelf'AS display_type, 40 AS points UNION ALL
SELECT 'Half shelf'AS display_type, 25 AS points UNION ALL
SELECT 'Dump bin'AS display_type, 50 AS points UNION ALL
SELECT 'Half dump bin'AS display_type, 25 AS points UNION ALL
SELECT 'Floor stack'AS display_type, 55 AS points UNION ALL
SELECT 'Tower'AS display_type, 70 AS points UNION ALL
SELECT 'Counter unit'AS display_type, 40 AS points UNION ALL
SELECT 'Hang Cell'AS display_type, 25 AS points UNION ALL
SELECT 'Clip Strip'AS display_type, 25 AS points
),
final as
(
select 
    display_type::varchar(256) as oa_display_type,
    points::number(10,3) as points,
    NULL::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final