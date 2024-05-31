with edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_indonesia_base_detail as
(
    select * from {{ ref('idnwks_integration__wks_indonesia_base_detail') }}
),
final as 
(
    select 
    jj_sap_dstrbtr_nm,
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month,
    so_qty,
    so_value,
    inv_qty,
    inv_value,
    case when month > (SELECT third_month FROM 
                            (SELECT mnth_id,LAG(mnth_id,2) OVER (ORDER BY mnth_id) third_month 
                                FROM (SELECT DISTINCT "year",mnth_id
                                        FROM edw_vw_os_time_dim
                                        WHERE mnth_id <= 
                                        (SELECT DISTINCT mnth_id
                                            FROM edw_vw_os_time_dim
                                            WHERE cal_date = TO_DATE(current_timestamp()::timestamp_ntz))))
                            WHERE mnth_id = (SELECT DISTINCT mnth_id
                        FROM edw_vw_os_time_dim
                    WHERE cal_date = TO_DATE(current_timestamp()::timestamp_ntz))) 
                    and ( nvl(so_value,0)=0 or nvl(inv_value,0)=0) 
    then 'Y' 
    else 'N' 
    end as propagate_flag,
    max(month) over( partition by sap_parent_customer_key) latest_month 
    from (
        Select jj_sap_dstrbtr_nm,
                sap_parent_customer_key,
                sap_parent_customer_desc,
                month,
                sum(so_qty)so_qty,
                sum(so_value)so_value,
                sum(inv_qty)inv_qty,
                sum(inv_value)inv_value 
            from  wks_indonesia_base_detail where 
            month <= ( SELECT DISTINCT mnth_id  FROM edw_vw_os_time_dim where cal_date = TO_DATE(current_timestamp()::timestamp_ntz ) )
            group by jj_sap_dstrbtr_nm,sap_parent_customer_key,sap_parent_customer_desc,month)
)
select * from final
