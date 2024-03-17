with
thailand_propagate_to as 
(
    select * from {{ ref('thawks_integration__thailand_propagate_to') }}
),
wks_thailand_base_detail as 
(
    select * from {{ source('snaposewks_integration', 'wks_thailand_base_detail') }}
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
    from thailand_propagate_to
    where propagate_flag = 'Y'
),
all_months as
(
    Select 
        dstr_nm,
        sap_parent_customer_key,
        sap_parent_customer_desc,
        month
    from wks_thailand_base_detail
    where (sap_parent_customer_key, month) not in 
            (
            select sap_parent_customer_key,
                month
            from thailand_propagate_to
            where propagate_flag = 'Y'
            )
    group by dstr_nm,
        sap_parent_customer_key,
        sap_parent_customer_desc,
        month
    having (
            sum(so_qty) > 0
            and sum(inv_qty) > 0
            )
),
max_month as
(
    select dstr_nm,
        sap_parent_customer_key,
        sap_parent_customer_desc,
        max(month) as month
    from all_months
    group by dstr_nm,
        sap_parent_customer_key,
        sap_parent_customer_desc
),
base as 
(
        select 
            d.dstr_nm,
            d.sap_parent_customer_key,
            d.sap_parent_customer_desc,
            d.month as month,
            sum(so_qty) so_qty,
            sum(so_value) so_value,
            sum(inv_qty) inv_qty,
            sum(inv_value) inv_value
        from  max_month,
            wks_thailand_base_detail d
        where max_month.sap_parent_customer_key = d.sap_parent_customer_key
            and upper(max_month.dstr_nm) = upper(d.dstr_nm)
            and max_month.month = d.month
        group by d.dstr_nm,
            d.sap_parent_customer_key,
            d.sap_parent_customer_desc,
            d.month
),
final as
(
select 
    p_to.dstr_nm::varchar(50) as dstr_nm,
    p_to.sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    p_to.sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
    p_to.latest_month::numeric(18,0) as latest_month,
    p_to.month::numeric(18,0) as propagate_to,
    base.month::numeric(18,0) as propagate_from,
    base.so_qty::float as so_qty,
    base.inv_qty::float as inv_qty,
    datediff(month,to_date(base.month || '01', 'yyyymmdd'), to_date(latest_month || '01', 'yyyymmdd'))::numeric(38,0)  as diff_month,
    p_to.reason::varchar(29) as reason
from  p_to,
     base
where p_to.sap_parent_customer_key = base.sap_parent_customer_key
    and p_to.dstr_nm = base.dstr_nm
    and base.month < p_to.month
    and datediff(month,to_date(base.month || '01', 'yyyymmdd'), to_date(latest_month || '01', 'yyyymmdd')) <= 2
)

select * from final