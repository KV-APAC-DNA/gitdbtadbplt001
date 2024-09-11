with wks_china_lastnmonths_inv as
(
    select * from {{ ref('chnwks_integration__wks_china_lastnmonths_inv') }}
),
wks_china_base_inv as
(
    select * from {{ ref('chnwks_integration__wks_china_base_inv') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(
    Select 
    country_name, 
    sap_prnt_cust_key, 
    sap_bnr_key, 
    bu, 
    sold_to, 
    matl_num, 
    month, 
    base_matl_num, 
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
        agg.country_name, 
        agg.sap_prnt_cust_key, 
        agg.sap_bnr_key, 
        agg.bu, 
        agg.sold_to, 
        agg.matl_num, 
        agg.month, 
        base.matl_num as base_matl_num, 
        base.so_sls_qty as so_qty, 
        base.so_trd_sls as so_value, 
        base.inventory_quantity as inv_qty, 
        base.inventory_val as inv_value, 
        base.si_sls_qty as sell_in_qty, 
        base.si_gts_val as sell_in_value, 
        agg.last_3months_so, 
        agg.last_6months_so, 
        agg.last_12months_so, 
        agg.last_3months_so_value, 
        agg.last_6months_so_value, 
        agg.last_12months_so_value, 
        agg.last_36months_so_value, 
        case when (base.matl_num is NULL) then 'Y' else 'N' end as replicated_flag 
        from 
        wks_china_lastnmonths_inv agg, 
        wks_china_base_inv base 
        where 
        LEFT (agg.month, 4) >= (
            DATE_PART(year, current_timestamp()) -2
        ) 
        and agg.country_name = base.country_name (+) 
        and agg.sap_prnt_cust_key = base.sap_prnt_cust_key (+) 
        and agg.sap_bnr_key = base.sap_bnr_key (+) 
        and agg.bu = base.bu (+) 
        and agg.sold_to = base.sold_to (+) 
        and agg.matl_num = base.matl_num (+) 
        and agg.month = base.year_month (+) 
        and agg.month <= (
            select 
            distinct cal_MNTH_ID as mnth_id 
            from 
            edw_vw_os_time_dim 
            where 
            cal_date = to_date(current_timestamp())
        )
    ) 
    group by 
    country_name, 
    sap_prnt_cust_key, 
    sap_bnr_key, 
    bu, 
    sold_to, 
    matl_num, 
    month, 
    base_matl_num, 
    replicated_flag
)
select * from final