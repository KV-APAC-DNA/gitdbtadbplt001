with wks_taiwan_lastnmonths as (
    select * from {{ ref('ntawks_integration__wks_taiwan_lastnmonths') }}
),
wks_taiwan_base as (
    select * from {{ ref('ntawks_integration__wks_taiwan_base') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(   
    Select 
        sap_parent_customer_key,
        sap_parent_customer_desc,
        bnr_key,
        bnr_desc,
        ean_num,
        month,
        ean_num as base_matl_num,
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
    from 
        (
            select 
                agg.sap_parent_customer_key,
                agg.sap_parent_customer_desc,
                agg.bnr_key,
                agg.bnr_desc,
                agg.ean_num,
                agg.month,
                base.ean_num as base_ean_num,
                base.so_qty,
                base.so_value,
                base.inv_qty,
                base.inv_value,
                base.si_qty as sell_in_qty,
                base.si_value as sell_in_value,
                agg.last_3months_so,
                agg.last_6months_so,
                agg.last_12months_so,
                agg.last_3months_so_value,
                agg.last_6months_so_value,
                agg.last_12months_so_value,
                agg.last_36months_so_value,
                case
                    when (base.ean_num is NULL) then 'Y'
                    else 'N'
                end as replicated_flag
            from wks_taiwan_lastnmonths agg,
                wks_taiwan_base base
            where LEFT (agg.month, 4) >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -2)
                and agg.sap_parent_customer_key = base.sap_parent_customer_key (+)
                and agg.sap_parent_customer_desc = base.sap_parent_customer_desc (+)
                and agg.bnr_key = base.bnr_key (+)
                and agg.bnr_desc = base.bnr_desc (+)
                and agg.ean_num = base.ean_num (+)
                and agg.month = base.cal_month (+)
                and agg.month <= (
                    select distinct cal_mnth_id as mnth_id
                    from edw_vw_os_time_dim
                    where cal_date = to_date(convert_timezone('UTC', current_timestamp()))
                )
        )
    group by sap_parent_customer_key,
        sap_parent_customer_desc,
        bnr_key,
        bnr_desc,
        ean_num,
        month,
        replicated_flag
)
select * from final