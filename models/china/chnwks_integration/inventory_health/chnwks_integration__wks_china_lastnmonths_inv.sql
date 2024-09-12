with wks_china_allmonths_base_inv as
(
    select * from {{ ref('chnwks_integration__wks_china_allmonths_base_inv') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(
    select 
    country_name, 
    bu, 
    sap_prnt_cust_key, 
    sap_bnr_key, 
    sold_to, 
    coalesce(
        nullif(matl_num, ''), 
        'NA'
    ) as matl_num, 
    month, 
    sum(so_qty) so_qty, 
    sum(so_value) so_value, 
    sum(inv_qty) inv_qty, 
    sum(inv_value) inv_value, 
    sum(sell_in_qty) sell_in_qty, 
    sum(sell_in_value) sell_in_value, 
    sum(last_3months_so) as last_3months_so, 
    sum(last_3months_so_value) as last_3months_so_value, 
    sum(last_6months_so) as last_6months_so, 
    sum(last_6months_so_value) as last_6months_so_value, 
    sum(last_12months_so) as last_12months_so, 
    sum(last_12months_so_value) as last_12months_so_value, 
    sum(last_36months_so) as last_36months_so, 
    sum(last_36months_so_value) as last_36months_so_value 
    from 
    (
        select 
        base.month, 
        base.country_name, 
        base.bu, 
        base.matl_num, 
        base.sold_to, 
        base.sap_prnt_cust_key, 
        base.sap_bnr_key, 
        so_qty, 
        so_value, 
        inv_qty, 
        inv_value, 
        sell_in_qty, 
        sell_in_value, 
        last_3_months.last_3months_so_matl as last_3months_so, 
        last_3_months.last_3months_so_value_matl as last_3months_so_value, 
        last_6_months.last_6months_so_matl as last_6months_so, 
        last_6_months.last_6months_so_value_matl as last_6months_so_value, 
        last_12_months.last_12months_so_matl as last_12months_so, 
        last_12_months.last_12months_so_value_matl as last_12months_so_value, 
        last_36_months.last_36months_so_matl as last_36months_so, 
        last_36_months.last_36months_so_value_matl as last_36months_so_value 
        from 
        WKS_china_allmonths_base_inv base, 
        (
            select 
            mnth_id, 
            base3.country_name, 
            base3.bu, 
            base3.matl_num, 
            base3.sold_to, 
            base3.sap_prnt_cust_key, 
            base3.sap_bnr_key, 
            sum(so_qty) as last_3months_so_matl, 
            sum(so_value) as last_3months_so_value_matl 
            from 
            (
                select 
                * 
                from 
                WKS_china_allmonths_base_inv 
                where 
                left(month, 4) >= (
                    DATE_PART(YEAR, current_timestamp()) -6
                )
            ) base3, 
            (
                select 
                mnth_id, 
                third_month 
                from 
                (
                    select 
                    year, 
                    mnth_id, 
                    lag(mnth_id, 2) over (
                        order by 
                        mnth_id
                    ) third_month 
                    from 
                    (
                        select 
                        distinct cal_year as year, 
                        cal_mnth_id as mnth_id 
                        from 
                        edw_vw_os_time_dim -- limit 100
                        where 
                        year >= (
                            DATE_PART(YEAR, current_timestamp()) -6
                        )
                    )
                ) month_base
            ) to_month 
            where 
            month <= mnth_id 
            and month >= third_month 
            group by 
            mnth_id, 
            base3.country_name, 
            base3.bu, 
            base3.sold_to, 
            base3.matl_num, 
            base3.sap_prnt_cust_key, 
            base3.sap_bnr_key
        ) last_3_months, 
        (
            select 
            mnth_id, 
            base6.country_name, 
            base6.bu, 
            base6.sold_to, 
            base6.matl_num, 
            base6.sap_prnt_cust_key, 
            base6.sap_bnr_key, 
            sum(so_qty) as last_6months_so_matl, 
            sum(so_value) as last_6months_so_value_matl 
            from 
            (
                select 
                * 
                from 
                WKS_china_allmonths_base_inv 
                where 
                left(month, 4) >= (
                    DATE_PART(YEAR, current_timestamp()) -6
                )
            ) base6, 
            (
                select 
                mnth_id, 
                sixth_month 
                from 
                (
                    select 
                    year, 
                    mnth_id, 
                    lag(mnth_id, 5) over (
                        order by 
                        mnth_id
                    ) sixth_month 
                    from 
                    (
                        select 
                        distinct cal_year as year, 
                        cal_mnth_id as mnth_id 
                        from 
                        edw_vw_os_time_dim -- limit 100
                        where 
                        year >= (
                            DATE_PART(YEAR, current_timestamp()) -6
                        )
                    )
                ) month_base
            ) to_month 
            where 
            month <= mnth_id 
            and month >= sixth_month 
            group by 
            mnth_id, 
            base6.country_name, 
            base6.bu, 
            base6.sold_to, 
            base6.matl_num, 
            base6.sap_prnt_cust_key, 
            base6.sap_bnr_key
        ) last_6_months, 
        (
            select 
            mnth_id, 
            base12.country_name, 
            base12.bu, 
            base12.sold_to, 
            base12.matl_num, 
            base12.sap_prnt_cust_key, 
            base12.sap_bnr_key, 
            sum(so_qty) as last_12months_so_matl, 
            sum(so_value) as last_12months_so_value_matl 
            from 
            (
                select 
                * 
                from 
                WKS_china_allmonths_base_inv 
                where 
                left(month, 4) >= (
                    DATE_PART(YEAR, current_timestamp()) -6
                )
            ) base12, 
            (
                select 
                mnth_id, 
                twelfth_month 
                from 
                (
                    select 
                    year, 
                    mnth_id, 
                    lag(mnth_id, 11) over (
                        order by 
                        mnth_id
                    ) twelfth_month 
                    from 
                    (
                        select 
                        distinct cal_year as year, 
                        cal_mnth_id as mnth_id 
                        from 
                        edw_vw_os_time_dim -- limit 100
                        where 
                        year >= (
                            DATE_PART(YEAR, current_timestamp()) -6
                        )
                    )
                ) month_base
            ) to_month 
            where 
            month <= mnth_id 
            and month >= twelfth_month 
            group by 
            mnth_id, 
            base12.country_name, 
            base12.bu, 
            base12.sold_to, 
            base12.matl_num, 
            base12.sap_prnt_cust_key, 
            base12.sap_bnr_key
        ) last_12_months, 
        (
            select 
            mnth_id, 
            base36.country_name, 
            base36.bu, 
            base36.sold_to, 
            base36.matl_num, 
            base36.sap_prnt_cust_key, 
            base36.sap_bnr_key, 
            sum(so_qty) as last_36months_so_matl, 
            sum(so_value) as last_36months_so_value_matl 
            from 
            (
                select 
                * 
                from 
                WKS_china_allmonths_base_inv 
                where 
                left(month, 4) >= (
                    DATE_PART(YEAR, current_timestamp()) -6
                )
            ) base36, 
            (
                select 
                mnth_id, 
                thirtysixth_month 
                from 
                (
                    select 
                    year, 
                    mnth_id, 
                    lag(mnth_id, 35) over (
                        order by 
                        mnth_id
                    ) thirtysixth_month 
                    from 
                    (
                        select 
                        distinct cal_year as year, 
                        cal_mnth_id as mnth_id 
                        from 
                        edw_vw_os_time_dim -- limit 100
                        where 
                        year >= (
                            DATE_PART(YEAR, current_timestamp()) -6
                        )
                    )
                ) month_base
            ) to_month 
            where 
            month <= mnth_id 
            and month >= thirtysixth_month 
            group by 
            mnth_id, 
            base36.country_name, 
            base36.bu, 
            base36.sold_to, 
            base36.matl_num, 
            base36.sap_prnt_cust_key, 
            base36.sap_bnr_key
        ) last_36_months 
        where 
        base.country_name = last_3_months.country_name (+) 
        and base.sap_prnt_cust_key = last_3_months.sap_prnt_cust_key (+) 
        and base.sap_bnr_key = last_3_months.sap_bnr_key (+) 
        and base.bu = last_3_months.bu (+) 
        and base.sold_to = last_3_months.sold_to (+) 
        and base.matl_num = last_3_months.matl_num (+) 
        and base.month = last_3_months.mnth_id(+) 
        and base.country_name = last_6_months.country_name (+) 
        and base.sap_prnt_cust_key = last_6_months.sap_prnt_cust_key (+) 
        and base.sap_bnr_key = last_6_months.sap_bnr_key (+) 
        and base.bu = last_6_months.bu (+) 
        and base.sold_to = last_6_months.sold_to (+) 
        and base.matl_num = last_6_months.matl_num (+) 
        and base.month = last_6_months.mnth_id(+) 
        and base.country_name = last_12_months.country_name (+) 
        and base.sap_prnt_cust_key = last_12_months.sap_prnt_cust_key (+) 
        and base.sap_bnr_key = last_12_months.sap_bnr_key (+) 
        and base.bu = last_12_months.bu (+) 
        and base.sold_to = last_12_months.sold_to (+) 
        and base.matl_num = last_12_months.matl_num (+) 
        and base.month = last_12_months.mnth_id(+) 
        and base.country_name = last_36_months.country_name (+) 
        and base.sap_prnt_cust_key = last_36_months.sap_prnt_cust_key (+) 
        and base.sap_bnr_key = last_36_months.sap_bnr_key (+) 
        and base.bu = last_36_months.bu (+) 
        and base.sold_to = last_36_months.sold_to (+) 
        and base.matl_num = last_36_months.matl_num (+) 
        and base.month = last_36_months.mnth_id(+)
    ) 
    group by 
    country_name, 
    bu, 
    sold_to, 
    matl_num, 
    sap_prnt_cust_key, 
    sap_bnr_key, 
    month
)
select * from final