with indonesia_propagate_to as
(
    select * from {{ ref('idnwks_integration__indonesia_propagate_to') }}
),
wks_indonesia_base_detail as
(
    select * from {{ ref('idnwks_integration__wks_indonesia_base_detail') }}
),
final as 
(
    select p_to.jj_sap_dstrbtr_nm,
    p_to.sap_parent_customer_key,
    p_to.sap_parent_customer_desc,
    p_to.latest_month, 
    p_to.month as propagate_to,
    base.month propagate_from,
    base.so_qty,
    base.inv_qty,
    datediff( month, to_date(base.month,'YYYYMM') ,to_date(latest_month,'YYYYMM')) as diff_month,
    p_to.reason
    from
    (select *,
        case when propagate_flag='Y' and (nvl(so_value,0)=0 and nvl(inv_value,0)=0) then 'Sellout and Inventory Missing'
             when propagate_flag='Y' and nvl(so_value,0)=0 then 'Sellout Missing'
             when propagate_flag='Y' and nvl(inv_value,0)=0 then 'Inventory Missing'
             else 'Not Propagate' 
        end as reason 
     from  indonesia_propagate_to where  propagate_flag ='Y') p_to,
    (select d.jj_sap_dstrbtr_nm,
            d.sap_parent_customer_key, 
            d.sap_parent_customer_desc,
            d.month as month,
            sum(so_qty)so_qty,
            sum(so_value)so_value,
            sum(inv_qty)inv_qty,
            sum(inv_value)inv_value 
       from ( select jj_sap_dstrbtr_nm,sap_parent_customer_key,
                     sap_parent_customer_desc, 
                     max(month) as month
                from (select jj_sap_dstrbtr_nm,
                             sap_parent_customer_key,
                             sap_parent_customer_desc,
                             month
                        from wks_indonesia_base_detail where (jj_sap_dstrbtr_nm,sap_parent_customer_key, month) not in
                            (select jj_sap_dstrbtr_nm,
                                    sap_parent_customer_key,
                                    month
                                    from  indonesia_propagate_to
                                   where  propagate_flag ='Y' )
                        group by jj_sap_dstrbtr_nm,sap_parent_customer_key,sap_parent_customer_desc,month
                        having ( sum(so_qty) > 0 and sum(inv_qty) > 0 )
                    ) all_months
         group by jj_sap_dstrbtr_nm,sap_parent_customer_key,sap_parent_customer_desc
        ) max_month, wks_indonesia_base_detail d
          where max_month.jj_sap_dstrbtr_nm = d.jj_sap_dstrbtr_nm
            and max_month.sap_parent_customer_key = d.sap_parent_customer_key
            and max_month.month  =d.month  
        group by d.jj_sap_dstrbtr_nm,d.sap_parent_customer_key, d.sap_parent_customer_desc,d.month 
        ) base
        where p_to.jj_sap_dstrbtr_nm = base.jj_sap_dstrbtr_nm
         and  p_to.sap_parent_customer_key = base.sap_parent_customer_key
         and  p_to.sap_parent_customer_desc =base.sap_parent_customer_desc  
         and  base.month < p_to.month
         and  datediff( month, to_date(base.month,'YYYYMM') ,to_date(latest_month,'YYYYMM')   ) <= 2
)
select * from final
