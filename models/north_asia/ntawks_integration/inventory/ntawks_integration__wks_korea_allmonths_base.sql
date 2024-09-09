with 
wks_korea_base as 
(
    select * from {{ ref('ntawks_integration__wks_korea_base') }}
),
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as (
select all_months.dstr_cd as sap_parent_customer_key, all_months.matl_num, all_months.mnth_id as month
       , sum(b.so_qty )  as so_qty, 
         sum(b.so_value ) as so_value
       , sum(b.inv_qty)  as inv_qty
       , sum(b.inv_Value ) as inv_value
       , sum(b.si_qty)  as sell_in_qty 
       , sum(b.si_val)  as sell_in_value 
  from
    
        (select  distinct dstr_cd,matl_num,  mnth_id
         from 
             (select  distinct dstr_cd,prod_cd as matl_num 
                from wks_korea_base)  a  
            ,(select distinct "year" as year,mnth_id
                          from edw_vw_os_time_dim -- limit 100
                                  where year >= (date_part(year, convert_timezone('UTC', current_timestamp())) -6) 
             ) b
        )all_months
        ,wks_korea_base  b
  where all_months.dstr_cd = b.dstr_cd  (+)
   and  all_months.matl_num = b.prod_cd (+)
   and  mnth_id = fisc_per(+)

group by all_months.dstr_cd , all_months.matl_num, all_months.mnth_id)
select * from final