with wks_china_base_inv as
(
    select * from {{ ref('chnwks_integration__wks_china_base_inv') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(
    select 
    all_months.mnth_id as month, 
    all_months.country_name, 
    all_months.bu, 
    all_months.matl_num, 
    all_months.sold_to, 
    all_months.sap_prnt_cust_key, 
    all_months.SAP_BNR_KEY, 
    sum(b.so_sls_qty) as so_qty, 
    sum(b.so_trd_sls) as so_value, 
    sum(b.inventory_quantity) as inv_qty, 
    sum(b.inventory_val) as inv_value, 
    sum(b.si_sls_qty) as sell_in_qty, 
    sum(b.si_gts_val) as sell_in_value 
    from 
    (
        select 
        distinct country_name, 
        bu, 
        matl_num, 
        sold_to, 
        sap_prnt_cust_key, 
        sap_bnr_key, 
        mnth_id 
        from 
        (
            select 
            distinct country_name, 
            sold_to, 
            bu, 
            matl_num, 
            sap_prnt_cust_key, 
            sap_bnr_key 
            from 
            wks_china_base_inv
        ) a, 
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
        ) b
    ) all_months, 
    wks_china_base_inv b 
    where 
    all_months.country_name = b.country_name (+) 
    and all_months.sold_to = b.sold_to(+) 
    and all_months.sap_prnt_cust_key = b.sap_prnt_cust_key (+) 
    and all_months.sap_bnr_key = b.sap_bnr_key (+) 
    and all_months.matl_num = b.matl_num (+) 
    and all_months.bu = b.bu (+) 
    and all_months.mnth_id = b.year_month(+) 
    group by 
    all_months.mnth_id, 
    all_months.country_name, 
    all_months.bu, 
    all_months.matl_num, 
    all_months.sold_to, 
    all_months.sap_prnt_cust_key, 
    all_months.sap_bnr_key

)
select * from final