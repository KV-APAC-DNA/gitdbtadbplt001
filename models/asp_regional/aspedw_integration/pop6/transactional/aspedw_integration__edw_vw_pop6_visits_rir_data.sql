with itg_pop6_rir_data as 
(
    select * from {{ ref('aspitg_integration__itg_pop6_rir_data') }}
),
itg_pop6_executed_visits as 
(
    select * from {{ ref('aspitg_integration__itg_pop6_executed_visits') }}
),
edw_vw_pop6_products as 
(
    select * from {{ ref('aspedw_integration__edw_vw_pop6_products') }}
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
FROM (SELECT cntry_cd,
             src_file_date,
             visit_id,
             visit_date,
             check_in_datetime,
             check_out_datetime,
             popdb_id,
             pop_code,
             pop_name,
             address,
             check_in_longitude,
             check_in_latitude,
             check_out_longitude,
             check_out_latitude,
             check_in_photo,
             check_out_photo,
             username,
             user_full_name,
             superior_username,
             superior_name,
             planned_visit,
             cancelled_visit,
             cancellation_reason,
             cancellation_note,
             row_number() OVER (PARTITION BY visit_id,popdb_id ORDER BY run_id DESC) AS rn
      FROM itg_pop6_executed_visits where cntry_cd = 'TH'  
      ) visit
WHERE visit.rn = 1
),
final as
(    
    select rir.cntry_cd,
        rir.visit_id,
        rir.sku_id,
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
	   visit.cancellation_note ,
       prd.company,
       prd.ps_category,
       prd.ps_segment,
       rir.related_attribute,
       rir.layer,
       rir.total_layer,
       rir.facing_of_this_layer
 from itg_pop6_rir_data rir
 left join visit on visit.visit_id = rir.visit_id
 left join edw_vw_pop6_products prd on prd.productdb_id = rir.sku_id
)
select * from final