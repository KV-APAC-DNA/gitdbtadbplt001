with itg_la_gt_route_detail as (
	select * from dev_dna_core.snaposeitg_integration.itg_la_gt_route_detail
),
itg_la_gt_route_header as (
	select * from dev_dna_core.snaposeitg_integration.itg_la_gt_route_header
),
final as (
select 
  rd.route_id, 
  rd.customer_id as store_id, 
  rd.route_no, 
  rd.saleunit as distributor_id, 
  rd.ship_to, 
  rd.contact_person, 
  rh.route_name, 
  rh.route_desc, 
  rh.is_active, 
  rh.routesale as sales_rep_id, 
  rh.route_code, 
  rh.description as route_code_desc, 
  case when (
    left (
      trim((rd.customer_id):: text), 
      1) = ('_' ::varchar):: text
  ) then 'N' ::varchar else 'Y' ::varchar end as valid_flag, 
  rd.flag as route_detail_flag, 
  rh.effective_start_date, 
  rh.effective_end_date 
from 
  
    itg_la_gt_route_detail rd 
    left join itg_la_gt_route_header rh on (
     rd.route_id:: text = rh.route_id:: text
and upper(rd.saleunit):: text = upper(rh.saleunit):: text
and (case 
     when 
            upper(rd.flag):: text <> 'D' :: text
    and 
            rd.created_date::timestamp_ntz(9)
               <= 
            rh.effective_start_date::timestamp_ntz(9)
    then 1 
              when 
           upper(rd.flag):: text = 'D':: text
              and 
                  rd.created_date::timestamp_ntz(9) <= 
                  rh.effective_start_date::timestamp_ntz(9)
            and 
                rd.last_modified_date::timestamp_ntz(9)
              > rh.effective_start_date::timestamp_ntz(9)
             
           then 1 else 0 end = 1
        )
      
    
  

))
select * from final 