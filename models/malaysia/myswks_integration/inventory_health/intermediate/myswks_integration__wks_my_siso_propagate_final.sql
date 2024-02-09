with wks_my_base_detail as
(
    select * from {{ ref('myswks_integration__wks_my_base_detail') }}
),
my_propagate_from_to as
(
    select * from {{ ref('myswks_integration__my_propagate_from_to') }}
),
wks_my_siso_propagate_to_details as
(
    select * from {{ ref('myswks_integration__wks_my_siso_propagate_to_details') }}
),
wks_my_siso_propagate_to_existing_dtls as
(
    select * from {{ ref('myswks_integration__wks_my_siso_propagate_to_existing_dtls') }}
),
set1 as
(
    select
        distributor,
        dstrbtr_grp_cd,
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
        cast(null as int) as propagate_from,
        cast(null as varchar(100)) as reason,
        replicated_flag,
        cast(null as decimal(38, 5)) as existing_so_qty,
        cast(null as decimal(38, 5)) as existing_so_value,
        cast(null as decimal(38, 5)) as existing_inv_qty,
        cast(null as decimal(38, 5)) as existing_inv_value,
        cast(null as decimal(38, 5)) as existing_sell_in_qty,
        cast(null as decimal(38, 5)) as existing_sell_in_value,
        cast(null as decimal(38, 5)) as existing_last_3months_so,
        cast(null as decimal(38, 5)) as existing_last_6months_so,
        cast(null as decimal(38, 5)) as existing_last_12months_so,
        cast(null as decimal(38, 5)) as existing_last_3months_so_value,
        cast(null as decimal(38, 5)) as existing_last_6months_so_value,
        cast(null as decimal(38, 5)) as existing_last_12months_so_value
    from wks_my_base_detail
    where
     not (distributor, dstrbtr_grp_cd, sap_parent_customer_key, month)  in 
        (
            select
                distributor,
                dstrbtr_grp_cd,
                sap_parent_customer_key,
                propagate_to
            from my_propagate_from_to as p_from_to
        )
),
set2 as   
(
    select
        propagated.distributor,
        propagated.dstrbtr_grp_cd,
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
        cast(propagated.propagate_from as int),
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
    from wks_my_siso_propagate_to_details as propagated, 
         wks_my_siso_propagate_to_existing_dtls as existing
    where
        existing.distributor = propagated.distributor
        and existing.dstrbtr_grp_cd = propagated.dstrbtr_grp_cd
        and existing.sap_parent_customer_key = propagated.sap_parent_customer_key
        and existing.matl_num = propagated.matl_num
        and existing.month = propagated.month
),
set3 as
(
    select 
        propagated.distributor,
        propagated.dstrbtr_grp_cd,
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
  from wks_my_siso_propagate_to_details propagated 
 where not exists ( select 1
                        from  wks_my_siso_propagate_to_existing_dtls existing
                        where existing.distributor=propagated.distributor
                        and  existing.dstrbtr_grp_cd=propagated.dstrbtr_grp_cd
                        and existing.sap_parent_customer_key = propagated.sap_parent_customer_key
                        and  existing.matl_num = propagated.matl_num 
                        and  existing.month = propagated.month

                   )
),
final_set as
(   select * from set1
    union all
    select * from set2
    union all
    select * from set3
),
final as
(
    select 
        distributor::varchar(40) as distributor,
        dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
        sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
        sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
        month::number(18,0) as month,
        matl_num::varchar(100) as matl_num,
        so_qty::number(38,6) as so_qty,
        so_value::number(38,17) as so_value,
        inv_qty::number(38,4) as inv_qty,
        inv_value::number(38,13) as inv_value,
        sell_in_qty::number(38,4) as sell_in_qty,
        sell_in_value::number(38,13) as sell_in_value,
        last_3months_so::number(38,6) as last_3months_so,
        last_6months_so::number(38,6) as last_6months_so,
        last_12months_so::number(38,6) as last_12months_so,
        last_3months_so_value::number(38,17) as last_3months_so_value,
        last_6months_so_value::number(38,17) as last_6months_so_value,
        last_12months_so_value::number(38,17) as last_12months_so_value,
        last_36months_so_value::number(38,17) as last_36months_so_value,
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
    from final_set
)
select * from final
