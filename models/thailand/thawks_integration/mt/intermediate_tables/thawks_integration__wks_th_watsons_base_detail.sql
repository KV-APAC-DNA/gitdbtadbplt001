with wks_th_watsons_lastnmonths as(
    select * from {{ source('snaposewks_integration','wks_th_watsons_lastnmonths') }}
),
wks_th_watson_base as(
    select * from {{ source('snaposewks_integration','wks_th_watson_base') }}
),
edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
trans as 
(
    
    select
        agg.sap_parent_customer_key,
        agg.sap_parent_customer_desc,
        agg.matl_num,
        agg.month, 
        base.matl_num as base_matl_num,
        base.sale_avg_qty_13weeks as so_qty,
        base.sale_avg_val_13weeks as so_value,
        base.total_stock_qty as inv_qty,
        base.total_stock_val as inv_value,
        base.sellin_qty as sell_in_qty,
        base.sellin_val as sell_in_value,
        agg.last_3months_so,
        agg.last_6months_so,
        agg.last_12months_so,
        agg.last_3months_so_value,
        agg.last_6months_so_value,
        agg.last_12months_so_value,
        agg.last_15months_so_value,
        agg.last_18months_so_value,
        agg.last_21months_so_value,
        agg.last_24months_so_value,
        agg.last_27months_so_value,
        agg.last_30months_so_value,
        agg.last_33months_so_value,
        agg.last_36months_so_value,
        case when (
        base.matl_num is null
        ) then 'Y' else 'N' end as replicated_flag
    from wks_th_watsons_lastnmonths as agg, wks_th_watson_base as base
    where
        left(agg.month, 4) >= (
        date_part(year, current_timestamp()) - 2
        )
        and agg.sap_parent_customer_key = base.sap_prnt_cust_key(+)
        and agg.matl_num = base.matl_num(+)
        and agg.month = base.month(+)
        and agg.month <= (
        select distinct
            cal_mnth_id as mnth_id
        from edw_vw_os_time_dim
        where
            cal_date = (current_timestamp()::date)
        )
    
),

temp as 
(
    select
        sap_parent_customer_key,
        sap_parent_customer_desc,
        matl_num,
        month, 
        matl_num as base_matl_num,
        replicated_flag,
        sum(so_qty) as so_qty,
        sum(so_value) as so_value,
        sum(inv_qty) as inv_qty,
        sum(inv_value) as inv_value,
        sum(sell_in_qty) as sell_in_qty,
        sum(sell_in_value) as sell_in_value,
        sum(last_3months_so) as last_3months_so,
        sum(last_6months_so) as last_6months_so,
        sum(last_12months_so) as last_12months_so,
        sum(last_3months_so_value) as last_3months_so_value,
        sum(last_6months_so_value) as last_6months_so_value,
        sum(last_12months_so_value) as last_12months_so_value,
        sum(last_15months_so_value) as last_15months_so_value,
        sum(last_18months_so_value) as last_18months_so_value,
        sum(last_21months_so_value) as last_21months_so_value,
        sum(last_24months_so_value) as last_24months_so_value,
        sum(last_27months_so_value) as last_27months_so_value,
        sum(last_30months_so_value) as last_30months_so_value,
        sum(last_33months_so_value) as last_33months_so_value,
        sum(last_36months_so_value) as last_36months_so_value
    from trans
        group by
        sap_parent_customer_key,
        sap_parent_customer_desc,
        matl_num,
        month,
        replicated_flag
),
final as 
(
    select
        sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
        sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
        matl_num::varchar(1500) as matl_num,
        month::numeric(18,0) as month,
        base_matl_num::varchar(1500) as base_matl_num,
        replicated_flag::varchar(1) as replicated_flag,
        so_qty::numeric(38,4) as so_qty,
        so_value::numeric(38,8) as so_value,
        inv_qty::numeric(38,4) as inv_qty,
        inv_value::numeric(38,8) as inv_value,
        sell_in_qty::numeric(38,4) as sell_in_qty,
        sell_in_value::numeric(38,4) as sell_in_value,
        last_3months_so::numeric(38,4) as last_3months_so,
        last_6months_so::numeric(38,4) as last_6months_so,
        last_12months_so::numeric(38,4) as last_12months_so,
        last_3months_so_value::numeric(38,8) as last_3months_so_value,
        last_6months_so_value::numeric(38,8) as last_6months_so_value,
        last_12months_so_value::numeric(38,8) as last_12months_so_value,
        last_15months_so_value::numeric(38,8) as last_15months_so_value,
        last_18months_so_value::numeric(38,8) as last_18months_so_value,
        last_21months_so_value::numeric(38,8) as last_21months_so_value,
        last_24months_so_value::numeric(38,8) as last_24months_so_value,
        last_27months_so_value::numeric(38,8) as last_27months_so_value,
        last_30months_so_value::numeric(38,8) as last_30months_so_value,
        last_33months_so_value::numeric(38,8) as last_33months_so_value,
        last_36months_so_value::numeric(38,8) as last_36months_so_value
    from temp
)

select * from final