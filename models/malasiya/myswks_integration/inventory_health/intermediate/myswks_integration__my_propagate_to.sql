with wks_my_base_detail as
(
    select  * from {{ ref('myswks_integration__wks_my_base_detail') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
base as
(
    select
        distributor,
        dstrbtr_grp_cd,
        sap_parent_customer_key,
        sap_parent_customer_desc,
        month,
        sum(so_qty) as so_qty,
        sum(so_value) as so_value,
        sum(inv_qty) as inv_qty,
        sum(inv_value) as inv_value
    from wks_my_base_detail
    where
        month <= (
        select distinct cal_mnth_id from edw_vw_os_time_dim
        where
            cal_date = current_timestamp()::date
        )
    group by
        distributor,
        dstrbtr_grp_cd,
        sap_parent_customer_key,
        sap_parent_customer_desc,
        month
),
final as
    (select
    distributor,
    dstrbtr_grp_cd,
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month,
    so_qty,
    so_value,
    inv_qty,
    inv_value,
    case
    when month >(
                        select third_month
                        from
                        (
                            select mnth_id,lag(mnth_id, 2) over (order by mnth_id) as third_month
                            from
                            (select distinct "year", mnth_id from edw_vw_os_time_dim
                                where mnth_id <= (
                                    select distinct mnth_id from edw_vw_os_time_dim 
                                    where cal_date = current_timestamp()::date
                                )
                            )
                        )
                        where mnth_id = 
                        (
                        select distinct  mnth_id from edw_vw_os_time_dim
                        where cal_date = current_timestamp()::date
                        )
                    )
    and (coalesce(so_value, 0) = 0 or coalesce(inv_value, 0) = 0)
    then 'Y' else 'N'
    end as propagate_flag,
    max(month) over (partition by distributor, dstrbtr_grp_cd, sap_parent_customer_key order by null) as latest_month
    from base
)
select * from final

