with 
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_thailand_base_detail as
(
    select * from {{ ref('thawks_integration__wks_thailand_base_detail') }}
),

temp_a as
(
    select 
      mnth_id,
      lag(mnth_id, 2) over (order by mnth_id) third_month
    from (
            select distinct "year",
                  mnth_id
            from edw_vw_os_time_dim
            where mnth_id <= (
                select distinct mnth_id
                from edw_vw_os_time_dim
                where cal_date = current_timestamp()::date
                )
         )
),
temp_b as 
(
      select dstr_nm,
            sap_parent_customer_key,
            sap_parent_customer_desc,
            month,
            sum(so_qty) so_qty,
            sum(so_value) so_value,
            sum(inv_qty) inv_qty,
            sum(inv_value) inv_value
      from wks_thailand_base_detail
      where month <= (
            select distinct cal_mnth_id
            from edw_vw_os_time_dim
                where cal_date = current_timestamp()::date
            )
            group by dstr_nm,
            sap_parent_customer_key,
            sap_parent_customer_desc,
            month
    ),


trans as 
(
select 
    dstr_nm,
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month,
    so_qty,
    so_value,
    inv_qty,
    inv_value,
    case
        when month > (
            select 
                third_month
            from temp_a
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
    max(month) over(partition by sap_parent_customer_key) latest_month
from temp_b 
),
final as 
(
select 
    dstr_nm::varchar(50)  as dstr_nm,
	sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
	sap_parent_customer_desc::varchar(50)  as sap_parent_customer_desc,
	month::numeric(18,0) as month,
	so_qty::float as so_qty,
	so_value::float as so_value,
	inv_qty::float as inv_qty,
	inv_value::float as inv_value,
	propagate_flag::varchar(1) as propagate_flag,
	latest_month::numeric(18,0) as latest_month
from trans	
)

select * from final