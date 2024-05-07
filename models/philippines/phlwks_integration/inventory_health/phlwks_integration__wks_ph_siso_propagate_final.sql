
with wks_ph_siso_propagate_to_details as (
select * from {{ ref('phlwks_integration__wks_ph_siso_propagate_to_details') }}
),
wks_ph_siso_propagate_to_existing_dtls as (
select * from {{ ref('phlwks_integration__wks_ph_siso_propagate_to_details') }}
),
union_1 as (
select propagated.sap_parent_customer_key,
       propagated.dstrbtr_grp_cd,
       propagated.dstr_cd_nm,
       propagated.parent_customer_cd,
       propagated.sls_grp_desc ,
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
  from wks_PH_siso_propagate_to_details  propagated ,
       wks_PH_siso_propagate_to_existing_dtls existing
 where existing.sap_parent_customer_key = propagated.sap_parent_customer_key 
    and  existing.dstrbtr_grp_cd = propagated.dstrbtr_grp_cd                           
    and  existing.dstr_cd_nm     = propagated.dstr_cd_nm                          
    and  existing.parent_customer_cd = propagated.parent_customer_cd                      
 --   and  existing.sls_grp_desc    = propagated.sls_grp_desc 
   and existing.matl_num = propagated.matl_num
   and existing.month = propagated.month
   ),
   union_2 as (
   select propagated.sap_parent_customer_key,
       propagated.dstrbtr_grp_cd,
       propagated.dstr_cd_nm,
       propagated.parent_customer_cd,
       propagated.sls_grp_desc ,
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
  from wks_PH_siso_propagate_to_details propagated 
 where not exists ( select 1
                      from  wks_PH_siso_propagate_to_existing_dtls existing
                     where  existing.sap_parent_customer_key = propagated.sap_parent_customer_key 
                       and  existing.dstrbtr_grp_cd = propagated.dstrbtr_grp_cd                           
                    and  existing.dstr_cd_nm     = propagated.dstr_cd_nm                          
                        and  existing.parent_customer_cd = propagated.parent_customer_cd                      
        --                and  existing.sls_grp_desc    = propagated.sls_grp_desc                      
                       and  existing.matl_num = propagated.matl_num 
                       and  existing.month = propagated.month)
                   
   ),
final as (
select * from union_1
union all
select * from union_2
)
select * from final 