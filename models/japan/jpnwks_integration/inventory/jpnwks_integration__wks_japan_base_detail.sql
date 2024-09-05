with wks_japan_lastnmonths as(
    select * from {{ ref('jpnwks_integration__wks_japan_lastnmonths') }}
),
wks_japan_base as(
    select * from {{ ref('jpnwks_integration__wks_japan_base') }}
),
EDW_VW_OS_TIME_DIM as(
    select * from {{ ref('sgpedw_integration__EDW_VW_OS_TIME_DIM') }}
),
final as(
    Select 
sap_parent_customer_key , sap_parent_customer_desc, 
       matl_num, month, matl_num as base_matl_num ,     replicated_flag
       ,sum(so_qty)so_qty
       ,sum(so_val)so_value
       ,sum(closing_stock)inv_qty
       ,sum(closing_stock_val)inv_value
       ,sum(sell_in_qty)sell_in_qty
       ,sum(sell_in_value)sell_in_value
       ,sum(last_3months_so)last_3months_so       
       ,sum(last_6months_so)last_6months_so    
       ,sum(last_12months_so)last_12months_so    
       ,sum(last_3months_so_value )last_3months_so_value
       ,sum(last_6months_so_value )last_6months_so_value
       ,sum(last_12months_so_value)last_12months_so_value
       

from 
(
select agg.sap_parent_customer_key , agg.sap_parent_customer_desc, 
       agg.matl_num, agg.month, base.matl_num as base_matl_num 
       , base.so_qty
       , base.so_val
       , base.closing_stock
       , base.closing_stock_val
       , base.si_sls_qty as sell_in_qty
       , base.si_gts_val as sell_in_value
       , agg.last_3months_so         
       , agg.last_6months_so         
       , agg.last_12months_so        
       , agg.last_3months_so_value   
       , agg.last_6months_so_value  
       , agg.last_12months_so_value   
       , case when  (base.matl_num is NULL ) then
             'Y' 
            else 
            'N' end as replicated_flag 
from       
   wks_japan_lastnmonths agg
  ,wks_japan_base  base
 where agg.sap_parent_customer_key = base.prnt_key   (+)  
   and  agg.sap_parent_customer_desc = base.prnt_cust_desc (+)
   and  agg.matl_num  = base.matl_num   (+)
   and  agg.month = base.mnth_id  (+) 
   and  agg.month <= ( select distinct  cal_mnth_id as mnth_id
                                   from edw_vw_os_time_dim 
                                  where cal_date = to_date(convert_timezone('UTC', current_timestamp())) )
)
group by sap_parent_customer_key,sap_parent_customer_desc,matl_num,month,replicated_flag
)
select *from final