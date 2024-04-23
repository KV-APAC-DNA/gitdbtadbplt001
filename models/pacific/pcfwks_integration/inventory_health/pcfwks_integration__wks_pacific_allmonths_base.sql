with wks_pacific_base as
(
    select * from {{ ref('pcfwks_integration__wks_pacific_base') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(
    select
        all_months.sap_parent_customer_key::varchar(20) as sap_parent_customer_key,
        all_months.sap_parent_customer_desc::varchar(75) as sap_parent_customer_desc,
        all_months.matl_num::varchar(100) as matl_num,
        all_months.mnth_id::varchar(23) as month,
        sum(b.so_qty)::number(38,4) as so_qty,
        sum(b.so_value)::number(38,4) as so_value,
        sum(b.inv_qty)::number(38,5) as inv_qty,
        sum(b.inv_value)::number(38,4) as inv_value,
        sum(b.sell_in_qty)::number(38,5) as sell_in_qty,
        sum(b.sell_in_value)::number(38,5) as sell_in_value
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