with edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_japan_base_detail as(
    select * from {{ ref('jpnwks_integration__wks_japan_base_detail') }}   
),
final as(
    Select sap_parent_customer_key,sap_parent_customer_desc,month,
      so_qty,so_value,inv_qty,inv_value,
      case when  month >(SELECT
       third_month
FROM (SELECT cal_mnth_id,
             LAG(cal_mnth_id,2) OVER (ORDER BY cal_mnth_id) third_month
      FROM (SELECT DISTINCT cal_YEAR,
                   cal_mnth_id
            FROM EDW_VW_OS_TIME_DIM
            WHERE cal_mnth_id <= (SELECT DISTINCT cal_MNTH_ID
                              FROM EDW_VW_OS_TIME_DIM
                              WHERE cal_date = to_date(current_timestamp()))))
WHERE cal_mnth_id = (SELECT DISTINCT cal_MNTH_ID
                 FROM EDW_VW_OS_TIME_DIM
                 WHERE cal_date = to_date(current_timestamp())))
				 
          and ( nvl(so_value,0)=0 or nvl(inv_value,0)=0) then
          'Y' 
       else 
          'N' 
      end as propagate_flag,
      max(month) over( partition by sap_parent_customer_key) latest_month 
from (
      Select 
              sap_parent_customer_key,
              sap_parent_customer_desc,
              month,
              sum(so_qty)so_qty,
              sum(so_value)so_value,
              sum(inv_qty)inv_qty,
              sum(inv_value)inv_value 
      from  wks_japan_base_detail 
      where 
   month <= ( SELECT DISTINCT  cal_MNTH_ID  FROM EDW_VW_OS_TIME_DIM where cal_date = to_date(current_timestamp()) )
      
      group by sap_parent_customer_key,sap_parent_customer_desc,month

)
)
select * from final



