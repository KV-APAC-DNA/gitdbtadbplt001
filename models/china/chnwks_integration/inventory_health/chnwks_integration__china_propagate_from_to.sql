with wks_china_base_detail_inv as
(
    select * from {{ ref('chnwks_integration__wks_china_base_detail_inv') }}
),
china_propagate_to as
(
    select * from {{ ref('chnwks_integration__china_propagate_to') }}
),
final as
(
    select 
    p_to.country_name, 
    p_to.sold_to, 
    p_to.sap_prnt_cust_key, 
    p_to.sap_bnr_key, 
    p_to.bu, 
    p_to.latest_month, 
    p_to.month propagate_to, 
    base.month propagate_from, 
    base.so_qty, 
    base.inv_qty, 
    datediff(
        month, 
        to_date(to_char(base.month), 'YYYYMM'), 
        to_date(to_char(latest_month), 'YYYYMM')
        ) as diff_month, 
    p_to.reason 
    from 
    (
        select 
        *, 
        case when propagate_flag = 'Y' 
        and (
            nvl(so_value, 0)= 0 
            and nvl(inv_value, 0)= 0
        ) then 'Sellout and Inventory Missing' when propagate_flag = 'Y' 
        and nvl(so_value, 0)= 0 then 'Sellout Missing' when propagate_flag = 'Y' 
        and nvl(inv_value, 0)= 0 then 'Inventory Missing' else 'Not Propagate' end as reason 
        from 
        china_propagate_to 
        where 
        propagate_flag = 'Y'
    ) p_to, 
    (
        select 
        d.country_name, 
        d.sold_to, 
        d.sap_prnt_cust_key, 
        d.sap_bnr_key, 
        d.bu, 
        d.month as month, 
        sum(so_qty) so_qty, 
        sum(so_value) so_value, 
        sum(inv_qty) inv_qty, 
        sum(inv_value) inv_value 
        from 
        (
            select 
            country_name, 
            sold_to, 
            sap_prnt_cust_key, 
            sap_bnr_key, 
            bu, 
            max(month) as month 
            from 
            (
                Select 
                country_name, 
                sold_to, 
                sap_prnt_cust_key, 
                sap_bnr_key, 
                bu, 
                month 
                from 
                wks_china_base_detail_inv 
                where 
                (
                    country_name, sold_to, sap_prnt_cust_key, 
                    sap_bnr_key, bu, month
                ) not in (
                    select 
                    country_name, 
                    sold_to, 
                    sap_prnt_cust_key, 
                    sap_bnr_key, 
                    bu, 
                    month 
                    from 
                    china_propagate_to 
                    where 
                    propagate_flag = 'Y'
                ) 
                group by 
                country_name, 
                sold_to, 
                sap_prnt_cust_key, 
                sap_bnr_key, 
                bu, 
                month 
                having 
                (
                    sum(so_qty) > 0 
                    and sum(inv_qty) > 0
                )
            ) all_months 
            group by 
            country_name, 
            sold_to, 
            sap_prnt_cust_key, 
            sap_bnr_key, 
            bu
        ) max_month, 
        wks_china_base_detail_inv d 
        where 
        max_month.country_name = d.country_name 
        and max_month.sold_to = d.sold_to 
        and max_month.sap_prnt_cust_key = d.sap_prnt_cust_key 
        and max_month.sap_bnr_key = d.sap_bnr_key 
        and max_month.bu = d.bu 
        and max_month.month = d.month 
        group by 
        d.country_name, 
        d.sold_to, 
        d.sap_prnt_cust_key, 
        d.sap_bnr_key, 
        d.bu, 
        d.month
    ) base 
    where 
    p_to.country_name = base.country_name 
    and p_to.sold_to = base.sold_to 
    and p_to.sap_prnt_cust_key = base.sap_prnt_cust_key 
    and p_to.sap_bnr_key = base.sap_bnr_key 
    and p_to.bu = base.bu 
    and base.month < p_to.month 
    and datediff(
        month, 
        to_date(to_char(base.month), 'YYYYMM'), 
        to_date(to_char(latest_month), 'YYYYMM')
        ) <= 2
)
select * from final