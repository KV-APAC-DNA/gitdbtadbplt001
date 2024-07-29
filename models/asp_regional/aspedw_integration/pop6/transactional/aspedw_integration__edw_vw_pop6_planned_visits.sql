with itg_pop6_planned_visits as
(
    select * from {{ ref('aspitg_integration__itg_pop6_planned_visits') }}
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
        itg_pop6_planned_visits.user_full_name,        
    FROM itg_pop6_planned_visits
    qualify row_number() over(partition by cntry_cd,src_file_date,planned_visit_date,popdb_id,pop_code,pop_name,address,username,user_full_name order by run_id desc) = 1
)
select * from final