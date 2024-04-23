with 
pacific_propagate_to as(
    select * from {{ ref('pcfwks_integration__pacific_propagate_to') }}
),
wks_pacific_base_detail as(
    select * from {{ ref('pcfwks_integration__wks_pacific_base_detail') }}
),
p_to as
(
select *,
    case
        when propagate_flag = 'Y'
        and (
            nvl(so_value, 0) = 0
            or nvl(inv_value, 0) = 0
        ) then 'Sellout and Inventory Missing'
        when propagate_flag = 'Y'
        and nvl(so_value, 0) = 0 then 'Sellout Missing'
        when propagate_flag = 'Y'
        and nvl(inv_value, 0) = 0 then 'Inventory Missing'
        else 'Not Propagate'
    end as reason
from pacific_propagate_to
where propagate_flag = 'Y'
),
all_months as 
(
Select sap_parent_customer_key,
    sap_parent_customer_desc,
    month
from wks_pacific_base_detail
where (sap_parent_customer_key, month) not in (
        select sap_parent_customer_key,
            month
        from pacific_propagate_to
        where propagate_flag = 'Y'
    )
group by sap_parent_customer_key,
    sap_parent_customer_desc,
    month
having (
        sum(so_qty) > 0
        and sum(inv_qty) > 0
    )
),
max_month as 
(
select sap_parent_customer_key,
    sap_parent_customer_desc,
    max(month) as month
from all_months
group by sap_parent_customer_key,
    sap_parent_customer_desc
),
base as
(
select 
    d.sap_parent_customer_key,
    d.sap_parent_customer_desc,
    d.month as month,
    sum(so_qty) as so_qty,
    sum(so_value) as so_value,
    sum(inv_qty) as inv_qty,
    sum(inv_value) as inv_value
from max_month,
    wks_pacific_base_detail d
where max_month.sap_parent_customer_key = d.sap_parent_customer_key
    and max_month.month = d.month
group by d.sap_parent_customer_key,
    d.sap_parent_customer_desc,
    d.month
),
trans as 
(
select 
    p_to.sap_parent_customer_key,
    p_to.sap_parent_customer_desc,
    p_to.latest_month,
    p_to.month propagate_to,
    base.month propagate_from,
    base.so_qty,
    base.inv_qty,
    datediff(month,to_date(base.month || '01', 'yyyymmdd'), to_date(latest_month || '01', 'yyyymmdd')) as diff_month,
    p_to.reason
from p_to,base
where p_to.sap_parent_customer_key = base.sap_parent_customer_key
    and base.month < p_to.month
    and datediff(month,to_date(base.month || '01', 'yyyymmdd'), to_date(latest_month || '01', 'yyyymmdd')) <= 2
),
final as
(
    select 
    sap_parent_customer_key::varchar(20) as sap_parent_customer_key,
	sap_parent_customer_desc::varchar(75) as sap_parent_customer_desc,
	latest_month::varchar(23) as latest_month,
	propagate_to::varchar(23) as propagate_to,
	propagate_from::varchar(23) as propagate_from,
	so_qty::number(38,4) as so_qty,
	inv_qty::number(38,5) as inv_qty,
	diff_month::number(38,0) as diff_month,
	reason::varchar(29) as reason
    from trans
)
select * from final