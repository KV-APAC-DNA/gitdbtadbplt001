with itg_pop6_rir_data as 
(
    select * from {{ ref('ntaitg_integration__itg_pop6_rir_data') }}
),
itg_pop6_executed_visits as 
(
    select * from {{ ref('ntaitg_integration__itg_pop6_executed_visits') }}
),
itg_pop6_product_lists_pops as 
(
    select * from {{ ref('ntaitg_integration__itg_pop6_product_lists_pops') }}
),
itg_pop6_product_lists_products as 
(
    select * from {{ ref('ntaitg_integration__itg_pop6_product_lists_products') }}
),
visit as 
(
    SELECT visit.cntry_cd,
        visit.src_file_date,
        visit.visit_id,
        visit.visit_date,
        visit.check_in_datetime,
        visit.check_out_datetime,
        visit.popdb_id,
        visit.pop_code,
        visit.pop_name,
        visit.address,
        visit.check_in_longitude,
        visit.check_in_latitude,
        visit.check_out_longitude,
        visit.check_out_latitude,
        visit.check_in_photo,
        visit.check_out_photo,
        visit.username,
        visit.user_full_name,
        visit.superior_username,
        visit.superior_name,
        visit.planned_visit,
        visit.cancelled_visit,
        visit.cancellation_reason,
        visit.cancellation_note,
        visit.rn
    FROM 
        (
            SELECT 
                itg_pop6_executed_visits.cntry_cd,
                itg_pop6_executed_visits.src_file_date,
                itg_pop6_executed_visits.visit_id,
                itg_pop6_executed_visits.visit_date,
                itg_pop6_executed_visits.check_in_datetime,
                itg_pop6_executed_visits.check_out_datetime,
                itg_pop6_executed_visits.popdb_id,
                itg_pop6_executed_visits.pop_code,
                itg_pop6_executed_visits.pop_name,
                itg_pop6_executed_visits.address,
                itg_pop6_executed_visits.check_in_longitude,
                itg_pop6_executed_visits.check_in_latitude,
                itg_pop6_executed_visits.check_out_longitude,
                itg_pop6_executed_visits.check_out_latitude,
                itg_pop6_executed_visits.check_in_photo,
                itg_pop6_executed_visits.check_out_photo,
                itg_pop6_executed_visits.username,
                itg_pop6_executed_visits.user_full_name,
                itg_pop6_executed_visits.superior_username,
                itg_pop6_executed_visits.superior_name,
                itg_pop6_executed_visits.planned_visit,
                itg_pop6_executed_visits.cancelled_visit,
                itg_pop6_executed_visits.cancellation_reason,
                itg_pop6_executed_visits.cancellation_note,
                row_number() OVER(
                    PARTITION BY itg_pop6_executed_visits.visit_id,
                    itg_pop6_executed_visits.popdb_id
                    ORDER BY itg_pop6_executed_visits.run_id DESC
                ) AS rn
            FROM itg_pop6_executed_visits
            WHERE 
                (
                        (itg_pop6_executed_visits.cntry_cd)::text = 'TH'::text
                    AND 
                        itg_pop6_executed_visits.visit_id IN (
                            SELECT DISTINCT itg_pop6_rir_data.visit_id
                            FROM itg_pop6_rir_data
                        )
                )
        ) visit
    WHERE (visit.rn = 1)
),
final as
(    
    SELECT 
        visit.cntry_cd,
        visit.visit_id,
        visit.visit_date,
        visit.check_in_datetime,
        visit.check_out_datetime,
        visit.popdb_id,
        visit.pop_code,
        visit.pop_name,
        visit.address,
        visit.check_in_longitude,
        visit.check_in_latitude,
        visit.check_out_longitude,
        visit.check_out_latitude,
        visit.check_in_photo,
        visit.check_out_photo,
        visit.username,
        visit.user_full_name,
        visit.superior_username,
        visit.superior_name,
        visit.planned_visit,
        visit.cancelled_visit,
        visit.cancellation_reason,
        visit.cancellation_note,
        plpops.product_list,
        plprods.productdb_id,
        plprods.sku
    FROM 
    visit,
        itg_pop6_product_lists_pops plpops,
        itg_pop6_product_lists_products plprods
    WHERE upper((plpops.popdb_id)::text) = upper((visit.popdb_id)::text)
    AND plpops.prod_grp_date = visit.visit_date
    AND plprods.prod_grp_date = visit.visit_date
    AND upper((plprods.product_list)::text) = upper((plpops.product_list)::text)
    AND upper((plpops.product_list)::text) LIKE '%MSL%'::text
)
select * from final