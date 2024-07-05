with wks_HK_base_detail as (
select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_HK_BASE_DETAIL
),
HK_propagate_from_to as (
select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.HK_PROPAGATE_FROM_TO
),
wks_HK_siso_propagate_to_details as (
select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_HK_SISO_PROPAGATE_TO_DETAILS
),
wks_HK_siso_propagate_to_existing_dtls as (
select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_HK_SISO_PROPAGATE_TO_EXISTING_DTLS
),
union_1 as (
SELECT sap_parent_customer_key,
       month,
       matl_num,
       so_qty,
       so_value,
       inv_qty,
       inv_value,
       sell_in_qty,
       sell_in_value,
       last_3months_so,
       last_6months_so,
       last_12months_so,
       last_3months_so_value,
       last_6months_so_value,
       last_12months_so_value,
	   last_36months_so_value,
       'N' as propagate_flag,
       cast(null as integer) propagate_from,
       cast(null as varchar(100)) as reason,
       replicated_flag,
       cast (null as numeric(38,5)) existing_so_qty,               
       cast (null as numeric(38,5)) existing_so_value,               
       cast (null as numeric(38,5)) existing_inv_qty,             
       cast (null as numeric(38,5)) existing_inv_value,                  
       cast (null as numeric(38,5)) existing_sell_in_qty,               
       cast (null as numeric(38,5)) existing_sell_in_value,             
       cast (null as numeric(38,5)) existing_last_3months_so,           
       cast (null as numeric(38,5)) existing_last_6months_so,            
       cast (null as numeric(38,5)) existing_last_12months_so,            
       cast (null as numeric(38,5)) existing_last_3months_so_value,
       cast (null as numeric(38,5)) existing_last_6months_so_value,
       cast (null as numeric(38,5)) existing_last_12months_so_value     
FROM wks_HK_base_detail
 WHERE (sap_parent_customer_key, month) not in ( select sap_parent_customer_key, propagate_to 
                                                   from HK_propagate_from_to p_from_to )),
union_2 as (
select propagated.sap_parent_customer_key,
       propagated.month,                     
       propagated.matl_num,                       
       propagated.so_qty,                          
       propagated.so_value,                     
       propagated.inv_qty,                   
       propagated.inv_value,                     
       propagated.sell_in_qty,               
       propagated.sell_in_value,                 
       propagated.last_3months_so,               
       propagated.last_6months_so,           
       propagated.last_12months_so,          
       propagated.last_3months_so_value,    
       propagated.last_6months_so_value,     
       propagated.last_12months_so_value,
	   propagated.last_36months_so_value,	   
       propagated.Propagation_Flag,               
       cast (propagated.propagate_from as integer),                                          
       propagated.reason,  
       propagated.replicated_flag,                                 
       existing.so_qty,                             
       existing.so_value,                                   
       existing.inv_qty,                             
       existing.inv_value,                               
       existing.sell_in_qty,                            
       existing.sell_in_value,                         
       existing.last_3months_so,                             
       existing.last_6months_so,                       
       existing.last_12months_so,                     
       existing.last_3months_so_value,             
       existing.last_6months_so_value,                     
       existing.last_12months_so_value                      
  from wks_HK_siso_propagate_to_details  propagated ,
       wks_HK_siso_propagate_to_existing_dtls existing
 where existing.sap_parent_customer_key = propagated.sap_parent_customer_key
   and existing.matl_num = propagated.matl_num
   and existing.month = propagated.month
),
union_3 as (
select propagated.sap_parent_customer_key,
       propagated.month,
       propagated.matl_num,
       propagated.so_qty,
       propagated.so_value,
       propagated.inv_qty,
       propagated.inv_value,
       propagated.sell_in_qty,
       propagated.sell_in_value,
       propagated.last_3months_so,
       propagated.last_6months_so,
       propagated.last_12months_so,
       propagated.last_3months_so_value,
       propagated.last_6months_so_value,
       propagated.last_12months_so_value,
	   propagated.last_36months_so_value,
       propagated.Propagation_Flag,
       cast(propagated.propagate_from as integer),
       propagated.reason,
       replicated_flag,
       cast (null as numeric(38,5)) so_qty,
       cast (null as numeric(38,5)) so_value,
       cast (null as numeric(38,5)) inv_qty,
       cast (null as numeric(38,5)) inv_value,
       cast (null as numeric(38,5)) sell_in_qty,
       cast (null as numeric(38,5)) sell_in_value,
       cast (null as numeric(38,5)) last_3months_so,
       cast (null as numeric(38,5)) last_6months_so,
       cast (null as numeric(38,5)) last_12months_so,
       cast (null as numeric(38,5)) last_3months_so_value,
       cast (null as numeric(38,5)) last_6months_so_value,
       cast (null as numeric(38,5)) last_12months_so_value 
  from wks_HK_siso_propagate_to_details propagated 
 where not exists ( select 1
                      from  wks_HK_siso_propagate_to_existing_dtls existing
                     where  existing.sap_parent_customer_key = propagated.sap_parent_customer_key
                       and  existing.matl_num = propagated.matl_num 
                       and  existing.month = propagated.month
                   )),
final as (
select * from union_1
union all
select * from union_2
union all
select * from union_3
)
select * from final

