{{
    config(
        sql_header = "alter session set week_start= 7;"
    )
}}
with taiwan_propagate_to as(
    select * from {{ ref('ntawks_integration__taiwan_propagate_to') }}
),
wks_taiwan_base_detail as(
    select * from {{ ref('ntawks_integration__wks_taiwan_base_detail') }}
),
taiwan_propagate_to as(
    select * from {{ ref('ntawks_integration__taiwan_propagate_to') }}
),
wks_taiwan_base_detail as(
    select * from {{ ref('ntawks_integration__wks_taiwan_base_detail') }}
),
base as
(
    select 
        d.sap_parent_customer_key,
        d.sap_parent_customer_desc,
        d.bnr_key,
        d.bnr_desc,
        d.month as month,
        sum(so_qty) so_qty,
        sum(so_value) so_value,
        sum(inv_qty) inv_qty,
        sum(inv_value) inv_value
    from 
        (
            select sap_parent_customer_key,
                sap_parent_customer_desc,
                bnr_key,
                bnr_desc,
                max(month) as month
            from 
                (
                    Select sap_parent_customer_key,
                        sap_parent_customer_desc,
                        bnr_key,
                        bnr_desc,
                        month
                    from wks_taiwan_base_detail
                    where (sap_parent_customer_key, bnr_key, month) not in (
                            select sap_parent_customer_key,
                                bnr_key,
                                month
                            from taiwan_propagate_to
                            where propagate_flag = 'Y'
                        )
                    group by sap_parent_customer_key,
                        sap_parent_customer_desc,
                        bnr_key,
                        bnr_desc,
                        month
                    having (
                            sum(so_qty) > 0
                            and sum(inv_qty) > 0
                        )
                ) all_months
            group by sap_parent_customer_key,
                sap_parent_customer_desc,
                bnr_key,
                bnr_desc
        ) max_month,
        wks_taiwan_base_detail d
    where max_month.sap_parent_customer_key = d.sap_parent_customer_key
        and max_month.bnr_key = d.bnr_key
        and max_month.month = d.month
    group by d.sap_parent_customer_key,
        d.sap_parent_customer_desc,
        d.bnr_key,
        d.bnr_desc,
        d.month
),
p_to as
(
    select *,
        case
            when propagate_flag = 'Y'
            and (
                nvl(so_value, 0) = 0
                and nvl(inv_value, 0) = 0
            ) then 'Sellout and Inventory Missing'
            when propagate_flag = 'Y'
            and nvl(so_value, 0) = 0 then 'Sellout Missing'
            when propagate_flag = 'Y'
            and nvl(inv_value, 0) = 0 then 'Inventory Missing'
            else 'Not Propagate'
        end as reason
    from taiwan_propagate_to
    where propagate_flag = 'Y'
),
final as
(   
    select 
        p_to.sap_parent_customer_key,
        p_to.sap_parent_customer_desc,
        p_to.bnr_key,
        p_to.bnr_desc,
        p_to.latest_month,
        p_to.month propagate_to,
        base.month propagate_from,
        base.so_qty,
        base.inv_qty,
        datediff(
            month,
            to_date((base.month||'01'),'yyyymmdd'),
            to_date((latest_month||'01'),'yyyymmdd')
        ) diff_month,
        p_to.reason
    from p_to,base
    where p_to.sap_parent_customer_key = base.sap_parent_customer_key
        and p_to.bnr_key = base.bnr_key
        and base.month < p_to.month
        and datediff(
            month,
            to_date((base.month||'01'),'yyyymmdd'),
            to_date((latest_month||'01'),'yyyymmdd')
        ) <= 2
)
select * from final