with wks_thailand_base_detail as (
  select * from {{ ref('thawks_integration__wks_thailand_base_detail') }}
), 
thailand_propagate_from_to as (
  select * from {{ ref('thawks_integration__thailand_propagate_from_to') }}
), 
wks_thailand_siso_propagate_to_details as (
  select * from {{ ref('thawks_integration__wks_thailand_siso_propagate_to_details') }}
), 
wks_thailand_siso_propagate_to_existing_dtls as (
  select * from {{ ref('thawks_integration__wks_thailand_siso_propagate_to_existing_dtls') }}
), 
wks_thailand_siso_propagate_to_details as (
  select * from {{ ref('thawks_integration__wks_thailand_siso_propagate_to_details') }}
), 
wks_th_watsons_siso_propagate_final_tbl as (
  select * from {{ ref('thawks_integration__wks_th_watsons_siso_propagate_final_tbl') }}
), 
transformed as (
  select 
    dstr_nm, 
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
    last_36months_so_value, 
    'N' as propagate_flag, 
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
    wks_thailand_base_detail 
  where 
    (sap_parent_customer_key, month) not in (
      select 
        sap_parent_customer_key, 
        propagate_to 
      from 
        thailand_propagate_from_to p_from_to
    ) 
  union all 
  select 
    propagated.dstr_nm, 
    propagated.sap_parent_customer_key, 
    propagated.sap_parent_customer_desc, 
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
    wks_thailand_siso_propagate_to_details propagated, 
    wks_thailand_siso_propagate_to_existing_dtls existing 
  where 
    existing.sap_parent_customer_key = propagated.sap_parent_customer_key 
    and existing.matl_num = propagated.matl_num 
    and existing.month = propagated.month 
  union all 
  select 
    propagated.dstr_nm, 
    propagated.sap_parent_customer_key, 
    propagated.sap_parent_customer_desc, 
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
    wks_thailand_siso_propagate_to_details propagated 
  where 
    not exists (
      select 
        1 
      from 
        wks_thailand_siso_propagate_to_existing_dtls existing 
      where 
        existing.sap_parent_customer_key = propagated.sap_parent_customer_key 
        and existing.matl_num = propagated.matl_num 
        and existing.month = propagated.month
    ) 
  union all 
  select 
    'AS Watson''s' as dstr_nm, 
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
    so_13wks, 
    so_26_wks, 
    so_54_wks, 
    so_value_13wks, 
    so_value_26, 
    so_value_54_wks, 
    so_value_l36_mnth, 
    propagate_flag, 
    propagate_from, 
    reason, 
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
    wks_th_watsons_siso_propagate_final_tbl
),
final as (
    select 
    dstr_nm::varchar(50) as dstr_nm,
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
    month::number(18,0) as month,
    matl_num::varchar(50) as matl_num,
    so_qty::float as so_qty,
    so_value::float as so_value,
    inv_qty::float as inv_qty,
    inv_value::float as inv_value,
    sell_in_qty::number(38,6) as sell_in_qty,
    sell_in_value::number(38,5) as sell_in_value,
    last_3months_so::float as last_3months_so,
    last_6months_so::float as last_6months_so,
    last_12months_so::float as last_12months_so,
    last_3months_so_value::float as last_3months_so_value,
    last_6months_so_value::float as last_6months_so_value,
    last_12months_so_value::float as last_12months_so_value,
    last_36months_so_value::float as last_36months_so_value,
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