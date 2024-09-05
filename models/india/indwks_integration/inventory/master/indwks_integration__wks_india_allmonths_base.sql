with WKS_INDIA_base as
(
    select * from ({{ ref('indwks_integration__wks_india_base') }})
),
edw_vw_os_time_dim as
(
    select * from DEV_DNA_CORE.OSEEDW_INTEGRATION.EDW_VW_OS_TIME_DIM
),
trans as
(
select 
        all_months.sap_prnt_cust_key , 
        all_months.matl_num, 
        all_months.month as month,
        sum(b.so_qty )  as so_qty, 
         sum(b.so_val) as so_value,
        sum(b.closing_stock)  as inv_qty,
       sum(b.closing_stock_val ) as inv_value,
        sum(b.si_sls_qty)  as sell_in_qty ,
        sum(b.si_gts_val)  as sell_in_value 
  from
    
        (select  distinct sap_prnt_cust_key, matl_num,month
         from 
             (select  distinct sold_to as sap_prnt_cust_key, matl_num 
                from WKS_INDIA_base)  a
                  
            ,(select distinct cal_year as year,cal_mnth_id as month
                          from edw_vw_os_time_dim -- limit 100
                                  where cal_year >= (date_part(year, convert_timezone('UTC', current_timestamp())) -6) 
             ) b
        ) all_months
        ,WKS_INDIA_base  b
  where all_months.sap_prnt_cust_key = b.sold_to(+)
   and  all_months.matl_num  = b.matl_num  (+)
   and  month = mnth_id(+)

group by all_months.sap_prnt_cust_key ,all_months.matl_num, all_months.month),
final as
(
    select
    sap_prnt_cust_key::varchar(50) as sap_prnt_cust_key,
    matl_num::varchar(50) as matl_num,
    month::number(38,0) as month,
    so_qty::number(38,4) as so_qty,
    so_value::number(38,3) as so_value,
    inv_qty::number(38,3) as inv_qty,
    inv_value::number(38,6) as inv_value,
    sell_in_qty::number(38,4) as sell_in_qty,
    sell_in_value::number(38,4) as sell_in_value
    from trans
)
select * from final