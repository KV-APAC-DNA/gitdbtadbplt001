with 
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_pacific_lastnmonths as
(
    select * from {{ ref('pcfwks_integration__wks_pacific_lastnmonths') }}
),
wks_pacific_base as
(
    select * from {{ ref('pcfwks_integration__wks_pacific_base') }}
),
temp as 
(
    select 
        agg.sap_parent_customer_key,
        agg.sap_parent_customer_desc,
        agg.matl_num,
        agg.month,
        base.matl_num as base_matl_num,
        base.so_qty,
        base.so_value,
        base.inv_qty,
        base.inv_value,
        base.sell_in_qty,
        base.sell_in_value,
        agg.last_3months_so,
        agg.last_6months_so,
        agg.last_12months_so,
        agg.last_3months_so_value,
        agg.last_6months_so_value,
        agg.last_12months_so_value,
        agg.last_36months_so_value,
        case
            when (base.matl_num is NULL) then 'Y'
            else 'N'
        end as replicated_flag
    from wks_pacific_lastnmonths agg,
        wks_pacific_base base
    where left(agg.month, 4) >= (date_part(year, current_timestamp()) - 2)
        and agg.sap_parent_customer_key = base.sap_parent_customer_key (+)
        and agg.sap_parent_customer_desc = base.sap_parent_customer_desc (+)
        and agg.matl_num = base.matl_num (+)
        and agg.month = base.month (+)
        and agg.month <= (
            select distinct mnth_id
            from edw_vw_os_time_dim
            where cal_date = current_timestamp()::date
        )
),
trans as 
(
Select 
    sap_parent_customer_key,
    sap_parent_customer_desc,
    matl_num,
    month,
    matl_num as base_matl_num,
    replicated_flag,
    sum(so_qty) as so_qty,
    sum(so_value) as so_value,
    sum(inv_qty) as inv_qty,
    sum(inv_value) as inv_value,
    sum(sell_in_qty) as sell_in_qty,
    sum(sell_in_value) as sell_in_value,
    sum(last_3months_so) as last_3months_so,
    sum(last_6months_so) as last_6months_so,
    sum(last_12months_so) as last_12months_so,
    sum(last_3months_so_value) as last_3months_so_value,
    sum(last_6months_so_value) as last_6months_so_value,
    sum(last_12months_so_value) as last_12months_so_value,
    sum(last_36months_so_value) as last_36months_so_value
from temp
    group by 
    sap_parent_customer_key,
    sap_parent_customer_desc,
    matl_num,
    month,
    replicated_flag
),
final as 
(
select 
    sap_parent_customer_key::varchar(20) as sap_parent_customer_key,
	sap_parent_customer_desc::varchar(75) as sap_parent_customer_desc,
	matl_num::varchar(100) as matl_num,
	month::varchar(23) as month,
	base_matl_num::varchar(100) as base_matl_num,
	replicated_flag::varchar(1) as replicated_flag,
	so_qty::number(38,4) as so_qty,
	so_value::number(38,4) as so_value,
	inv_qty::number(38,5) as inv_qty,
	inv_value::number(38,4) as inv_value,
	sell_in_qty::number(38,5) as sell_in_qty,
	sell_in_value::number(38,5) as sell_in_value,
	last_3months_so::number(38,4) as last_3months_so,
	last_6months_so::number(38,4) as last_6months_so,
	last_12months_so::number(38,4) as last_12months_so,
	last_3months_so_value::number(38,4) as last_3months_so_value,
	last_6months_so_value::number(38,4) as last_6months_so_value,
	last_12months_so_value::number(38,4) as last_12months_so_value,
	last_36months_so_value::number(38,4) as last_36months_so_value
from trans
)
select * from final