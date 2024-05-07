
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
       cast (propagated.propagate_from as integer) as propagate_from,                                          
       propagated.reason,  
       propagated.replicated_flag,                                 
       existing.so_qty as existing_so_qty,                             
       existing.so_value as existing_so_value,                                   
       existing.inv_qty as existing_inv_qty,                             
       existing.inv_value as existing_inv_value,                               
       existing.sell_in_qty as existing_sell_in_qty,                            
       existing.sell_in_value as existing_sell_in_value,                         
       existing.last_3months_so as existing_last_3months_so,                    
       existing.last_6months_so as existing_last_6months_so,                    
       existing.last_12months_so as existing_last_12months_so,                    
       existing.last_3months_so_value as existing_last_3months_so_value,         
       existing.last_6months_so_value as existing_last_6months_so_value,         
       existing.last_12months_so_value as existing_last_12months_so_value  
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
       cast(propagated.propagate_from as integer) as propagate_from,
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
transformed as (
select * from union_1
union all
select * from union_2
),
final as (
select 
sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
dstr_cd_nm::varchar(308) as dstr_cd_nm,
parent_customer_cd::varchar(50) as parent_customer_cd,
sls_grp_desc::varchar(255) as sls_grp_desc,
month::varchar(23) as month,
matl_num::varchar(255) as matl_num,
so_qty::number(38,6) as so_qty,
so_value::number(38,12) as so_value,
inv_qty::number(38,4) as inv_qty,
inv_value::number(38,8) as inv_value,
sell_in_qty::number(38,5) as sell_in_qty,
sell_in_value::number(38,5) as sell_in_value,
last_3months_so::number(38,6) as last_3months_so,
last_6months_so::number(38,6) as last_6months_so,
last_12months_so::number(38,6) as last_12months_so,
last_3months_so_value::number(38,12) as last_3months_so_value,
last_6months_so_value::number(38,12) as last_6months_so_value,
last_12months_so_value::number(38,12) as last_12months_so_value,
Propagation_Flag::varchar(1) as propagate_flag,
propagate_from::number(18,0) as propagate_from,
reason::varchar(100) as reason,
replicated_flag::varchar(1) as replicated_flag,
existing_so_qty::number(38,5) as existing_so_qty,
existing_so_value::number(38,5) as existing_so_value,
existing_inv_qty::number(38,5) as existing_inv_qty,
existing_inv_value::number(38,5) as existing_inv_value,
existing_sell_in_qty::number(38,5) as existing_sell_in_qty,
existing_sell_in_value::number(38,5) as existing_sell_in_value,
existing_last_3months_so::number(38,5) as existing_last_3months_so,
existing_last_6months_so::number(38,5) as existing_last_6months_so,
existing_last_12months_so::number(38,5) as existing_last_12months_so,
existing_last_3months_so_value::number(38,5) as existing_last_3months_so_value,
existing_last_6months_so_value::number(38,5) as existing_last_6months_so_value,
existing_last_12months_so_value::number(38,5) as existing_last_12months_so_value
from transformed)
select * from final