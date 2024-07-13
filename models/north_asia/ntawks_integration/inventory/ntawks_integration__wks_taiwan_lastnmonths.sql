with wks_taiwan_allmonths_base as (
    select * from {{ ref('ntawks_integration__wks_taiwan_allmonths_base') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
last_3_months as 
(
    select 
        base3.sap_parent_customer_key,
        base3.sap_parent_customer_desc,
        base3.bnr_key,
        base3.bnr_desc,
        base3.ean_num,
        mnth_id,
        sum(so_qty) as last_3months_so_matl,
        sum(so_value) as last_3months_so_value_matl
    from 
        (
            select *from WKS_taiwan_allmonths_base
            where left(month, 4) >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -6)
        ) base3,
        (
            select mnth_id,
                third_month
            from 
                (
                    select 
                        year,
                        mnth_id,
                        lag(mnth_id, 2) over (order by mnth_id) third_month
                    from (
                            select distinct cal_year as year,
                                cal_mnth_id as mnth_id
                            from edw_vw_os_time_dim -- limit 100
                            where cal_year >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -6)
                        )
                ) month_base
        ) to_month
    where month <= mnth_id
        and month >= third_month
    group by base3.sap_parent_customer_key,
        base3.sap_parent_customer_desc,
        base3.bnr_key,
        base3.bnr_desc,
        base3.ean_num,
        mnth_id
),
last_6_months as 
(
    select 
        base6.sap_parent_customer_key,
        base6.sap_parent_customer_desc,
        base6.bnr_key,
        base6.bnr_desc,
        base6.ean_num,
        mnth_id,
        sum(so_qty) as last_6months_so_matl,
        sum(so_value) as last_6months_so_value_matl
    from (
            select *
            from WKS_taiwan_allmonths_base
            where left(month, 4) >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -6)
        ) base6,
        (
            select mnth_id,
                sixth_month
            from (
                    select year,
                        mnth_id,
                        lag(mnth_id, 5) over (
                            order by mnth_id
                        ) sixth_month
                    from (
                            select distinct cal_year as year,
                                cal_mnth_id as mnth_id
                            from edw_vw_os_time_dim -- limit 100
                            where cal_year >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -6)
                        )
                ) month_base
        ) to_month
    where month <= mnth_id
        and month >= sixth_month
    group by base6.sap_parent_customer_key,
        base6.sap_parent_customer_desc,
        base6.bnr_key,
        base6.bnr_desc,
        base6.ean_num,
        mnth_id
),
last_12_months as 
(
    select 
        base12.sap_parent_customer_key,
        base12.sap_parent_customer_desc,
        base12.bnr_key,
        base12.bnr_desc,
        base12.ean_num,
        mnth_id,
        sum(so_qty) as last_12months_so_matl,
        sum(so_value) as last_12months_so_value_matl
    from (
            select *
            from wks_taiwan_allmonths_base
            where left(month, 4) >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -6)
        ) base12,
        (
            select mnth_id,
                twelfth_month
            from (
                    select year,
                        mnth_id,
                        lag(mnth_id, 11) over (
                            order by mnth_id
                        ) twelfth_month
                    from (
                            select distinct cal_year as year,
                                cal_mnth_id as mnth_id
                            from edw_vw_os_time_dim -- limit 100
                            where cal_year >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -6)
                        )
                ) month_base
        ) to_month
    where month <= mnth_id
        and month >= twelfth_month
    group by base12.sap_parent_customer_key,
        base12.sap_parent_customer_desc,
        base12.bnr_key,
        base12.bnr_desc,
        base12.ean_num,
        mnth_id
),
last_36_months as 
(
    select base36.sap_parent_customer_key,
        base36.sap_parent_customer_desc,
        base36.bnr_key,
        base36.bnr_desc,
        base36.ean_num,
        mnth_id,
        sum(so_qty) as last_36months_so_matl,
        sum(so_value) as last_36months_so_value_matl
    from (
            select *
            from wks_taiwan_allmonths_base
            where left(month, 4) >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -6)
        ) base36,
        (
            select mnth_id,
                thirtysixth_month
            from (
                    select year,
                        mnth_id,
                        lag(mnth_id, 35) over (
                            order by mnth_id
                        ) thirtysixth_month
                    from (
                            select distinct cal_year as year,
                                cal_mnth_id as mnth_id
                            from edw_vw_os_time_dim -- limit 100
                            where cal_year >= (DATE_PART(YEAR, convert_timezone('UTC', current_timestamp())) -6)
                        )
                ) month_base
        ) to_month
    where month <= mnth_id
        and month >= thirtysixth_month
    group by base36.sap_parent_customer_key,
        base36.sap_parent_customer_desc,
        base36.bnr_key,
        base36.bnr_desc,
        base36.ean_num,
        mnth_id
),
final as
(   
    select 
        sap_parent_customer_key,
        sap_parent_customer_desc,
        bnr_key,
        bnr_desc,
        coalesce(nullif(ean_num, ''), 'NA') as ean_num,
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
                base.sap_parent_customer_key,
                base.sap_parent_customer_desc,
                base.bnr_key,
                base.bnr_desc,
                base.ean_num,
                base.month,
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
            from wks_taiwan_allmonths_base base,
            last_3_months,
            last_6_months,
            last_12_months,
            last_36_months
            where 
                base.sap_parent_customer_key = last_3_months.sap_parent_customer_key (+)
                and base.sap_parent_customer_desc = last_3_months.sap_parent_customer_desc (+)
                and base.bnr_key = last_3_months.bnr_key(+)
                and base.bnr_desc = last_3_months.bnr_desc(+)
                and base.ean_num = last_3_months.ean_num (+)
                and base.month = last_3_months.mnth_id(+)
                and base.sap_parent_customer_key = last_6_months.sap_parent_customer_key (+)
                and base.sap_parent_customer_desc = last_6_months.sap_parent_customer_desc (+)
                and base.bnr_key = last_6_months.bnr_key(+)
                and base.bnr_desc = last_6_months.bnr_desc(+)
                and base.ean_num = last_6_months.ean_num (+)
                and base.month = last_6_months.mnth_id(+)
                and base.sap_parent_customer_key = last_12_months.sap_parent_customer_key (+)
                and base.sap_parent_customer_desc = last_12_months.sap_parent_customer_desc (+)
                and base.bnr_key = last_12_months.bnr_key(+)
                and base.bnr_desc = last_12_months.bnr_desc(+)
                and base.ean_num = last_12_months.ean_num (+)
                and base.month = last_12_months.mnth_id(+)
                and base.sap_parent_customer_key = last_36_months.sap_parent_customer_key (+)
                and base.sap_parent_customer_desc = last_36_months.sap_parent_customer_desc (+)
                and base.bnr_key = last_36_months.bnr_key(+)
                and base.bnr_desc = last_36_months.bnr_desc(+)
                and base.ean_num = last_36_months.ean_num (+)
                and base.month = last_36_months.mnth_id(+)
        )
    group by sap_parent_customer_key,
        sap_parent_customer_desc,
        bnr_key,
        bnr_desc,
        ean_num,
        month
)
select * from final