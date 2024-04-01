with wks_th_watsons_base_detail as (
  select * from {{ ref('thawks_integration__wks_th_watsons_base_detail') }}
),
wks_th_watsons_propagate_from_to as (
  select * from {{ ref('thawks_integration__wks_th_watsons_propagate_from_to') }}
),
wks_th_watsons_siso_propagate_to_details as (
  select * from {{ ref('thawks_integration__wks_th_watsons_siso_propagate_to_details') }}
),
wks_th_watsons_siso_propagate_to_existing_dtls as (
  select * from {{ ref('thawks_integration__wks_th_watsons_siso_propagate_to_existing_dtls') }}
),
transformed as (
  select 
    sap_parent_customer_key, 
    sap_parent_customer_desc, 
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
    last_15months_so_value, 
    last_18months_so_value, 
    last_21months_so_value, 
    last_24months_so_value, 
    last_27months_so_value, 
    last_30months_so_value, 
    last_33months_so_value, 
    last_36months_so_value, 
    'n' as propagate_flag, 
    cast(null as integer) propagate_from, 
    cast(
      null as varchar(100)
    ) as reason, 
    replicated_flag, 
    cast (
      null as numeric(38, 5)
    ) existing_so_qty, 
    cast (
      null as numeric(38, 5)
    ) existing_so_value, 
    cast (
      null as numeric(38, 5)
    ) existing_inv_qty, 
    cast (
      null as numeric(38, 5)
    ) existing_inv_value, 
    cast (
      null as numeric(38, 5)
    ) existing_sell_in_qty, 
    cast (
      null as numeric(38, 5)
    ) existing_sell_in_value, 
    cast (
      null as numeric(38, 5)
    ) existing_last_3months_so, 
    cast (
      null as numeric(38, 5)
    ) existing_last_6months_so, 
    cast (
      null as numeric(38, 5)
    ) existing_last_12months_so, 
    cast (
      null as numeric(38, 5)
    ) existing_last_3months_so_value, 
    cast (
      null as numeric(38, 5)
    ) existing_last_6months_so_value, 
    cast (
      null as numeric(38, 5)
    ) existing_last_12months_so_value 
  from 
    wks_th_watsons_base_detail 
  where 
    (
      sap_parent_customer_key, 
      --dstrbtr_grp_cd,si_chnl_cd,si_sub_chnl_cd,account_grp , 
      month
    ) not in (
      select 
        sap_parent_customer_key, 
        --dstrbtr_grp_cd,si_chnl_cd,si_sub_chnl_cd,account_grp ,
        propagate_to 
      from 
        wks_th_watsons_propagate_from_to p_from_to
    ) 
  union all 
  select 
    propagated.sap_parent_customer_key, 
    propagated.sap_parent_customer_desc, 
    --        propagated.si_chnl_cd,
    --        propagated.si_sub_chnl_cd,
    --        propagated.account_grp ,
    cast(propagated.month as integer), 
    --       propagated.base_third_month ,
    --       propagated.base_sixth_month ,
    --       propagated.base_twelfth_month ,                   
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
    propagated.last_15months_so_value, 
    propagated.last_18months_so_value, 
    propagated.last_21months_so_value, 
    propagated.last_24months_so_value, 
    propagated.last_27months_so_value, 
    propagated.last_30months_so_value, 
    propagated.last_33months_so_value, 
    propagated.last_36months_so_value, 
    propagated.propagation_flag, 
    cast (
      propagated.propagate_from as integer
    ), 
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
  from 
    wks_th_watsons_siso_propagate_to_details propagated, 
    wks_th_watsons_siso_propagate_to_existing_dtls existing 
  where 
    existing.sap_parent_customer_key = propagated.sap_parent_customer_key --    and  existing.dstrbtr_grp_cd = propagated.dstrbtr_grp_cd                           
    --    and  existing.si_chnl_cd     = propagated.si_chnl_cd                          
    --    and  existing.si_sub_chnl_cd = propagated.si_sub_chnl_cd                      
    --    and  existing.account_grp    = propagated.account_grp 
    and existing.matl_num = propagated.matl_num 
    and existing.month = propagated.month 
  union all 
  select 
    propagated.sap_parent_customer_key, 
    propagated.sap_parent_customer_desc, 
    --        propagated.si_chnl_cd,
    --        propagated.si_sub_chnl_cd,
    --        propagated.account_grp ,
    cast(propagated.month as integer), 
    --        propagated.base_third_month ,
    --        propagated.base_sixth_month ,
    --        propagated.base_twelfth_month , 
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
    propagated.last_15months_so_value, 
    propagated.last_18months_so_value, 
    propagated.last_21months_so_value, 
    propagated.last_24months_so_value, 
    propagated.last_27months_so_value, 
    propagated.last_30months_so_value, 
    propagated.last_33months_so_value, 
    propagated.last_36months_so_value, 
    propagated.propagation_flag, 
    cast(
      propagated.propagate_from as integer
    ), 
    propagated.reason, 
    replicated_flag, 
    cast (
      null as numeric(38, 5)
    ) so_qty, 
    cast (
      null as numeric(38, 5)
    ) so_value, 
    cast (
      null as numeric(38, 5)
    ) inv_qty, 
    cast (
      null as numeric(38, 5)
    ) inv_value, 
    cast (
      null as numeric(38, 5)
    ) sell_in_qty, 
    cast (
      null as numeric(38, 5)
    ) sell_in_value, 
    cast (
      null as numeric(38, 5)
    ) last_3months_so, 
    cast (
      null as numeric(38, 5)
    ) last_6months_so, 
    cast (
      null as numeric(38, 5)
    ) last_12months_so, 
    cast (
      null as numeric(38, 5)
    ) last_3months_so_value, 
    cast (
      null as numeric(38, 5)
    ) last_6months_so_value, 
    cast (
      null as numeric(38, 5)
    ) last_12months_so_value 
  from 
    wks_th_watsons_siso_propagate_to_details propagated 
  where 
    not exists (
      select 
        1 
      from 
        wks_th_watsons_siso_propagate_to_existing_dtls existing 
      where 
        existing.sap_parent_customer_key = propagated.sap_parent_customer_key --                        and  existing.dstrbtr_grp_cd = propagated.dstrbtr_grp_cd                           
        --                        and  existing.si_chnl_cd     = propagated.si_chnl_cd                          
        --                        and  existing.si_sub_chnl_cd = propagated.si_sub_chnl_cd                      
        --                        and  existing.account_grp    = propagated.account_grp                      
        and existing.matl_num = propagated.matl_num 
        and existing.month = propagated.month
    )
) ,
final as (
select 
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
    month::number(18,0) as month,
    matl_num::varchar(1500) as matl_num,
    so_qty::number(38,4) as so_qty,
    so_value::number(38,8) as so_value,
    inv_qty::number(38,4) as inv_qty,
    inv_value::number(38,8) as inv_value,
    sell_in_qty::number(38,4) as sell_in_qty,
    sell_in_value::number(38,4) as sell_in_value,
    last_3months_so::number(38,4) as last_3months_so,
    last_6months_so::number(38,4) as last_6months_so,
    last_12months_so::number(38,4) as last_12months_so,
    last_3months_so_value::number(38,8) as last_3months_so_value,
    last_6months_so_value::number(38,8) as last_6months_so_value,
    last_12months_so_value::number(38,8) as last_12months_so_value,
    last_15months_so_value::number(38,8) as last_15months_so_value,
    last_18months_so_value::number(38,8) as last_18months_so_value,
    last_21months_so_value::number(38,8) as last_21months_so_value,
    last_24months_so_value::number(38,8) as last_24months_so_value,
    last_27months_so_value::number(38,8) as last_27months_so_value,
    last_30months_so_value::number(38,8) as last_30months_so_value,
    last_33months_so_value::number(38,8) as last_33months_so_value,
    last_36months_so_value::number(38,8) as last_36months_so_value,
    propagate_flag::varchar(1) as propagate_flag,
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
from transformed
)

select * from final
