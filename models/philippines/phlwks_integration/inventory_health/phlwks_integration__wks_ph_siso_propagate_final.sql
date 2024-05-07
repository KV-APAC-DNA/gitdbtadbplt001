
with 
wks_ph_base_detail as (
select * from {{ ref('phlwks_integration__wks_ph_base_detail') }}
),
ph_propagate_from_to as (
select * from {{ ref('phlwks_integration__ph_propagate_from_to') }}
),
wks_ph_siso_propagate_to_details as (
select * from {{ ref('phlwks_integration__wks_ph_siso_propagate_to_details') }}
),
wks_ph_siso_propagate_to_existing_dtls as (
select * from {{ ref('phlwks_integration__wks_ph_siso_propagate_to_details') }}
),
union_1 as (
SELECT sap_parent_customer_key,
	dstrbtr_grp_cd,
	dstr_cd_nm,
	parent_customer_cd,
	sls_grp_desc,
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
	'N' AS propagate_flag,
	cast(NULL AS INTEGER) propagate_from,
	cast(NULL AS VARCHAR(100)) AS reason,
	replicated_flag,
	cast(NULL AS NUMERIC(38, 5)) existing_so_qty,
	cast(NULL AS NUMERIC(38, 5)) existing_so_value,
	cast(NULL AS NUMERIC(38, 5)) existing_inv_qty,
	cast(NULL AS NUMERIC(38, 5)) existing_inv_value,
	cast(NULL AS NUMERIC(38, 5)) existing_sell_in_qty,
	cast(NULL AS NUMERIC(38, 5)) existing_sell_in_value,
	cast(NULL AS NUMERIC(38, 5)) existing_last_3months_so,
	cast(NULL AS NUMERIC(38, 5)) existing_last_6months_so,
	cast(NULL AS NUMERIC(38, 5)) existing_last_12months_so,
	cast(NULL AS NUMERIC(38, 5)) existing_last_3months_so_value,
	cast(NULL AS NUMERIC(38, 5)) existing_last_6months_so_value,
	cast(NULL AS NUMERIC(38, 5)) existing_last_12months_so_value
FROM wks_PH_base_detail
WHERE (
		sap_parent_customer_key,
		dstrbtr_grp_cd,
		dstr_cd_nm,
		parent_customer_cd,
		month
		) NOT IN (
		SELECT sap_parent_customer_key,
			dstrbtr_grp_cd,
			dstr_cd_nm,
			parent_customer_cd,
			propagate_to
		FROM PH_propagate_from_to p_from_to
		)
   ),
   union_2 as (
  SELECT propagated.sap_parent_customer_key,
	propagated.dstrbtr_grp_cd,
	propagated.dstr_cd_nm,
	propagated.parent_customer_cd,
	propagated.sls_grp_desc,
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
	cast(propagated.propagate_from AS INTEGER),
	propagated.reason,
	replicated_flag,
	cast(NULL AS NUMERIC(38, 5)) so_qty,
	cast(NULL AS NUMERIC(38, 5)) so_value,
	cast(NULL AS NUMERIC(38, 5)) inv_qty,
	cast(NULL AS NUMERIC(38, 5)) inv_value,
	cast(NULL AS NUMERIC(38, 5)) sell_in_qty,
	cast(NULL AS NUMERIC(38, 5)) sell_in_value,
	cast(NULL AS NUMERIC(38, 5)) last_3months_so,
	cast(NULL AS NUMERIC(38, 5)) last_6months_so,
	cast(NULL AS NUMERIC(38, 5)) last_12months_so,
	cast(NULL AS NUMERIC(38, 5)) last_3months_so_value,
	cast(NULL AS NUMERIC(38, 5)) last_6months_so_value,
	cast(NULL AS NUMERIC(38, 5)) last_12months_so_value
FROM wks_PH_siso_propagate_to_details propagated
WHERE NOT EXISTS (
		SELECT 1
		FROM wks_PH_siso_propagate_to_existing_dtls existing
		WHERE existing.sap_parent_customer_key = propagated.sap_parent_customer_key
			AND existing.dstrbtr_grp_cd = propagated.dstrbtr_grp_cd
			AND existing.dstr_cd_nm = propagated.dstr_cd_nm
			AND existing.parent_customer_cd = propagated.parent_customer_cd
			--                and  existing.sls_grp_desc    = propagated.sls_grp_desc                      
			AND existing.matl_num = propagated.matl_num
			AND existing.month = propagated.month
		)
                   
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
LAST_36MONTHS_SO_VALUE::NUMBER(38,12) as last_36months_so_value,
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
from transformed)
select * from final