with
wks_ph_base_detail as 
(
    select * from {{ ref('phlwks_integration__wks_ph_base_detail') }}
),
ph_propagate_from_to as 
(
    select * from {{ ref('phlwks_integration__ph_propagate_from_to') }}
),
trans as
(
    SELECT 
    p_from_to.sap_parent_customer_key,
    p_from_to.dstrbtr_grp_cd,
    p_from_to.dstr_cd_nm,
    p_from_to.parent_customer_cd,
    p_from_to.sls_grp_desc,
    propagate_to as month,
    base.matl_num,
    base.so_qty,
    base.so_value,
    base.inv_qty,
    base.inv_value,
    base.sell_in_qty,
    base.sell_in_value,
    base.last_3months_so,
    base.last_6months_so,
    base.last_12months_so,
    base.last_3months_so_value,
    base.last_6months_so_value,
    base.last_12months_so_value,
    base.last_36months_so_value,
    'Y' as Propagation_Flag,
    propagate_from,
    reason,
    base.replicated_flag
from wks_ph_base_detail base,
     ph_propagate_from_to p_from_to
where base.sap_parent_customer_key = p_from_to.sap_parent_customer_key
    and base.dstrbtr_grp_cd = p_from_to.dstrbtr_grp_cd
    and base.dstr_cd_nm = p_from_to.dstr_cd_nm
    and base.parent_customer_cd = p_from_to.parent_customer_cd 
    and base.month = p_from_to.propagate_from
),
final as
(
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
	last_36months_so_value::number(38,12) as last_36months_so_value,
	propagation_flag::varchar(1) as propagation_flag,
	propagate_from::varchar(23) as propagate_from,
	reason::varchar(29) as reason,
	replicated_flag::varchar(1) as replicated_flag
    from trans
)
select * from final