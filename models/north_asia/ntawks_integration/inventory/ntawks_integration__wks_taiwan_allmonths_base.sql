with wks_taiwan_base as (
    select * from snapntawks_integration.wks_taiwan_base
),
edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(   
    select 
        all_months.sap_parent_customer_key,
        all_months.sap_parent_customer_desc,
        all_months.bnr_key,
        all_months.bnr_desc,
        all_months.ean_num,
        all_months.mnth_id as month,
        sum(b.so_qty) as so_qty,
        sum(b.so_value) as so_value,
        sum(b.inv_qty) as inv_qty,
        sum(b.inv_value) as inv_value,
        sum(b.SI_QTY) as sell_in_qty,
        sum(b.SI_VALUE) as sell_in_value
    from 
        (
            select 
                distinct sap_parent_customer_key,
                sap_parent_customer_desc,
                bnr_key,
                bnr_desc,
                ean_num,
                mnth_id
            from 
                (
                    select distinct sap_parent_customer_key,
                        sap_parent_customer_desc,
                        bnr_key,
                        bnr_desc,
                        EAN_NUM
                    from wks_taiwan_base
                ) a,
                (
                    select distinct cal_year as year,
                        cal_mnth_id as mnth_id
                    from edw_vw_os_time_dim -- limit 100
                    where cal_year >= (date_part(year, convert_timezone('UTC', current_timestamp())) -6)
                ) b
        ) all_months,
        wks_taiwan_base b
    where all_months.sap_parent_customer_key = b.sap_parent_customer_key (+)
        and all_months.sap_parent_customer_desc = b.sap_parent_customer_desc (+)
        and all_months.bnr_key = b.bnr_key(+)
        and all_months.bnr_desc = b.bnr_desc(+)
        and all_months.EAN_num = b.EAN_num (+)
        and mnth_id = cal_month(+) --and month='202001' and all_months.sap_parent_customer_desc='SIGMA'
    group by all_months.sap_parent_customer_key,
        all_months.sap_parent_customer_desc,
        all_months.bnr_key,
        all_months.bnr_desc,
        all_months.ean_num,
        all_months.mnth_id
)
select * from final