with wks_pacific_base as
(
    select * from snappcfwks_integration.wks_pacific_base
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(
    select
        all_months.sap_parent_customer_key,
        all_months.sap_parent_customer_desc,
        all_months.matl_num,
        all_months.mnth_id as month,
        sum(b.so_qty) as so_qty,
        sum(b.so_value) as so_value,
        sum(b.inv_qty) as inv_qty,
        sum(b.inv_value) as inv_value,
        sum(b.sell_in_qty) as sell_in_qty,
        sum(b.sell_in_value) as sell_in_value
    from
        (
            select distinct sap_parent_customer_key,
                sap_parent_customer_desc,
                matl_num,
                mnth_id
            from
                (
                    select
                        distinct sap_parent_customer_key,
                        sap_parent_customer_desc,
                        matl_num
                    from wks_pacific_base
                )a,
                (
                    select distinct "year",mnth_id
                    from edw_vw_os_time_dim
                    where "year" > (DATE_PART(YEAR, current_timestamp) -6)
                ) b
        ) all_months,
        wks_pacific_base b
    where all_months.sap_parent_customer_key = b.sap_parent_customer_key (+)
        and all_months.sap_parent_customer_desc = b.sap_parent_customer_desc (+)
        and all_months.matl_num = b.matl_num (+)
        and mnth_id = month(+)
    group by 
    all_months.sap_parent_customer_key,
    all_months.sap_parent_customer_desc,
    all_months.matl_num,
    all_months.mnth_id
)
select * from final