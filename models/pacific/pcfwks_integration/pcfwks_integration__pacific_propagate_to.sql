with 
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_pacific_base_detail as
(
    select * from {{ ref('pcfwks_integration__wks_pacific_base_detail') }}
),
trans as
(
select 
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month,
    so_qty,
    so_value,
    inv_qty,
    inv_value,
    case
        when month > (
    select third_month
    from (
            select mnth_id,
                lag(mnth_id, 2) over (
                    order by mnth_id
                ) third_month
            from (
                    select distinct 
                        "year",
                        mnth_id
                    from edw_vw_os_time_dim
                    where mnth_id <= (
                            select distinct mnth_id
                            from edw_vw_os_time_dim
                            where cal_date = current_timestamp()::date
                        )
                )
        )
            where mnth_id = (
                    select distinct mnth_id
                    from edw_vw_os_time_dim
                    where cal_date = current_timestamp()::date
                )
        )
        and (
            nvl(so_value, 0) = 0
            or nvl(inv_value, 0) = 0
        ) then 'Y'
        else 'N'
    end as propagate_flag,
    max(month) over(partition by sap_parent_customer_key) as latest_month
from (
        select 
            sap_parent_customer_key,
            sap_parent_customer_desc,
            month,
            sum(so_qty) as so_qty,
            sum(so_value) as so_value,
            sum(inv_qty) as inv_qty,
            sum(inv_value) as inv_value
        from wks_pacific_base_detail
        where sap_parent_customer_desc in (
                'COLES',
                'WOOLWORTHS',
                'METCASH',
                'SYMBION',
                'CENTRAL HEALTHCARE SERVICES PTY LTD',
                'API',
                'SIGMA',
                'CHEMIST WAREHOUSE'
            )
            and month <= (
                select distinct mnth_id
                from edw_vw_os_time_dim
                where cal_date = current_timestamp()::date
            )
        group by sap_parent_customer_key,
            sap_parent_customer_desc,
            month
)),
final as 
(
select
    sap_parent_customer_key::varchar(20) as sap_parent_customer_key,
	sap_parent_customer_desc::varchar(75) as sap_parent_customer_desc,
	month::varchar(23) as month,
	so_qty::number(38,4) as so_qty,
	so_value::number(38,4) as so_value,
	inv_qty::number(38,5) as inv_qty,
	inv_value::number(38,4) as inv_value,
	propagate_flag::varchar(1) as propagate_flag,
	latest_month::varchar(23) as latest_month,
from trans
)
select * from final