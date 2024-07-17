with 
edw_hcp360_in_ventasys_employee_dim as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_employee_dim') }}
), 
edw_hcp360_in_ventasys_hcp_dim as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_hcp_dim') }}
), 
final as 
(
    SELECT 
       hcp.hcp_id as hcp,
       hcp.customer_name AS hcp_name,
       emp.name AS emp_name,
       emp.employee_id AS emp_id,
       emp.region AS region_vent,
       emp.territory AS territory_vent,
       emp.zone AS zone_vent
FROM edw_hcp360_in_ventasys_hcp_dim hcp
  LEFT JOIN (SELECT emp_terrid,name,employee_id,region,territory,zone, row_number() over(partition by emp_terrid order by join_date desc) AS rnk
             FROM   edw_hcp360_in_ventasys_employee_dim
             WHERE  is_active = 'Y' ) emp
          ON hcp.territory_id = emp.emp_terrid
         AND emp.rnk = 1 
WHERE  hcp.valid_to > current_date)
select * from final