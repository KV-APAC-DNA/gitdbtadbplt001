with itg_pop6_planned_visits as
(
    select * from {{ ref('ntaitg_integration__itg_pop6_planned_visits') }}
),


final as
(
    SELECT 
        itg_pop6_planned_visits.cntry_cd,
        itg_pop6_planned_visits.src_file_date,
        itg_pop6_planned_visits.planned_visit_date,
        itg_pop6_planned_visits.popdb_id,
        itg_pop6_planned_visits.pop_code,
        itg_pop6_planned_visits.pop_name,
        itg_pop6_planned_visits.address,
        itg_pop6_planned_visits.username,
        itg_pop6_planned_visits.user_full_name
    FROM itg_pop6_planned_visits
)

select * from final