with cit81salem as (
    select * from {{ ref('jpndcledw_integration__cit81salem') }}
),

final as (
SELECT cit81salem.saleno,
    cit81salem.juchgyono,
    sum(cit81salem.meisainukikingaku) AS tyouseimaekingaku
FROM cit81salem
GROUP BY cit81salem.saleno,
    cit81salem.juchgyono
   )

select * from final