with source as(
    select * from {{ ref('thawks_integration__wks_th_watson_base') }}
),
edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
all_months as(
    select  
        distinct sap_prnt_cust_key,sap_prnt_cust_desc,
        matl_num, 
        mnth_id
    from 
        (select  distinct sap_prnt_cust_key,sap_prnt_cust_desc,
        matl_num from source )  a ,
        (select distinct cal_year as year,cal_mnth_id as mnth_id
        from edw_vw_os_time_dim 
        where cal_year >= (DATE_PART(YEAR,CURRENT_TIMESTAMP()) -8) 
        ) b
),
transformed as(
    select 
        all_months.sap_prnt_cust_key as sap_parent_customer_key, 
        all_months.sap_prnt_cust_desc as sap_parent_customer_desc,
        all_months.matl_num, all_months.mnth_id as month
        , sum(b.sale_avg_qty_13weeks)  as so_qty, 
        sum(b.sale_avg_val_13weeks) as so_value
        , sum(b.total_stock_qty)  as inv_qty
        , sum(b.total_stock_val) as inv_value
        , sum(b.sellin_qty)  as sell_in_qty 
        , sum(b.sellin_val)  as sell_in_value 
    from all_months, source  b
    where all_months.sap_prnt_cust_key = b.sap_prnt_cust_key  (+)    
    and  all_months.matl_num = b.matl_num (+)
    and  mnth_id = month(+)

    group by all_months.sap_prnt_cust_key,all_months.sap_prnt_cust_desc,
    all_months.matl_num, 
    all_months.mnth_id 
),
final as(
    select 
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
	sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
	matl_num::varchar(1500) as matl_num,
	month::number(18,0) as month,
	so_qty::number(38,4) as so_qty,
	so_value::number(38,8) as so_value,
	inv_qty::number(38,4) as inv_qty,
	inv_value::number(38,8) as inv_value,
	sell_in_qty::number(38,4) as sell_in_qty,
	sell_in_value::number(38,4) as sell_in_value
    from transformed
)
select * from final