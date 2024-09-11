with wks_korea_lastnmonths as (
    select *
    from {{ ref('ntawks_integration__wks_korea_lastnmonths') }}
), wks_korea_base as (
    select *
    from {{ ref('ntawks_integration__wks_korea_base') }}
), edw_vw_os_time_dim as (
    select *
    from { { ref('sgpedw_integration__edw_vw_os_time_dim') } }
),
temp as (
    select agg.sap_parent_customer_key,
        agg.matl_num,
        agg.month,
        base.prod_cd as base_matl_num,
        base.so_qty as so_qty,
        base.so_value as so_value,
        base.inv_qty as inv_qty,
        base.inv_value as inv_value,
        base.si_qty as sell_in_qty,
        base.si_val as sell_in_value,
        agg.last_3months_so,
        agg.last_6months_so,
        agg.last_12months_so,
        agg.last_3months_so_value,
        agg.last_6months_so_value,
        agg.last_12months_so_value,
        agg.last_36months_so_value,
        case
            when (base.prod_cd is NULL) then 'Y'
            else 'N'
        end as replicated_flag
    from wks_korea_lastnmonths agg,
        wks_korea_base base
    where LEFT (agg.month, 4) >= (
            date_part(
                year,
                convert_timezone('UTC', current_timestamp())
            ) -2
        )
        and agg.sap_parent_customer_key = base.dstr_cd (+)
        and agg.matl_num = base.prod_Cd (+)
        and agg.month = base.fisc_per (+)
        and agg.month <= (
            select distinct mnth_id as mnth_id
            from edw_vw_os_time_dim
            where cal_date = to_date(convert_timezone('UTC', current_timestamp()))
        )
),
final as (
    Select sap_parent_customer_key,
        matl_num,
        month,
        matl_num as base_matl_num,
        replicated_flag,
        sum(so_qty) so_qty,
        sum(so_value) so_value,
        sum(inv_qty) inv_qty,
        sum(inv_value) inv_value,
        sum(sell_in_qty) sell_in_qty,
        sum(sell_in_value) sell_in_value,
        sum(last_3months_so) last_3months_so,
        sum(last_6months_so) last_6months_so,
        sum(last_12months_so) last_12months_so,
        sum(last_3months_so_value) last_3months_so_value,
        sum(last_6months_so_value) last_6months_so_value,
        sum(last_12months_so_value) last_12months_so_value,
        sum(last_36months_so_value) last_36months_so_value
    from temp
    group by sap_parent_customer_key,
        matl_num,
        month,
        replicated_flag
)
select *
from final