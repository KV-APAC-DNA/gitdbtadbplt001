with
korea_propagate_to as 
(
    select * from {{ ref('ntawks_integration__korea_propagate_to') }}
) ,
wks_korea_base_detail as 
(
    select * from {{ ref('ntawks_integration__wks_korea_base_detail') }}
),

final as 
(
select p_to.sap_parent_customer_key,p_to.latest_month, p_to.month propagate_to, base.month propagate_from, base.so_qty, base.inv_qty 
       ,datediff( month, to_date(base.month,'YYYYMM') ,to_date(latest_month,'YYYYMM')   ) diff_month
       ,p_to.reason
 from
    (select *,
                      case when propagate_flag='Y' and (nvl(so_value,0)=0 and nvl(inv_value,0)=0) then 'Sellout and Inventory Missing'
                 when propagate_flag='Y' and nvl(so_value,0)=0 then 'Sellout Missing'
                 when propagate_flag='Y' and nvl(inv_value,0)=0 then 'Inventory Missing'
                 else 'Not Propagate' 
           end as reason 
        from  korea_propagate_to where  propagate_flag ='Y') p_to
    ,
    (select  d.sap_parent_customer_key, 
            d.month as month,
            sum(so_qty)so_qty,
            sum(so_value)so_value,
            sum(inv_qty)inv_qty,
            sum(inv_value)inv_value 
       from 
        ( select sap_parent_customer_key,
        max(month) as month
            from 
               (Select
                          sap_parent_customer_key,
                          month
                  from    wks_korea_base_detail
                 where   (sap_parent_customer_key, month) not in
                                 (select sap_parent_customer_key, month
                                    from  korea_propagate_to
                                   where  propagate_flag ='Y' )
                group by sap_parent_customer_key, month
                 having ( sum(so_qty) > 0 and sum(inv_qty) > 0 )
                ) all_months
         group by sap_parent_customer_key
        ) max_month
        ,wks_korea_base_detail d
       where max_month.sap_parent_customer_key = d.sap_parent_customer_key
         and max_month.month  =d.month  
       group by d.sap_parent_customer_key, 
            d.month 
     ) base
     where p_to.sap_parent_customer_key = base.sap_parent_customer_key
       and base.month < p_to.month
       and datediff( month, to_date(base.month,'YYYYMM') ,to_date(latest_month,'YYYYMM')   ) <= 2)

select * from final