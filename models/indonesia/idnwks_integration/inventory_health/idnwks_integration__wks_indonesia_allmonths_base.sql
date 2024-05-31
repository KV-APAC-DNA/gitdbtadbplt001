with 
wks_indonesia_base as 
(   
    select * from {{ ref('idnwks_interation__wks_indonesia_base') }}
),
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
trans as 
(
    select 
    all_months.jj_sap_dstrbtr_nm,
    all_months.sap_prnt_cust_key as sap_parent_customer_key,
    all_months.sap_prnt_cust_desc as sap_parent_customer_desc,
    all_months.matl_num,
    all_months.mnth_id as month,
    sum(b.so_sls_qty) as so_qty,
    sum(b.so_trd_sls) as so_value,
    sum(b.inventory_quantity) as inv_qty,
    sum(b.inventory_val) as inv_value,
    sum(b.si_sls_qty) as sell_in_qty,
    sum(b.si_gts_val) as sell_in_value
from (
        select distinct
            jj_sap_dstrbtr_nm,
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            matl_num,
            mnth_id
        from (
                select distinct 
                    jj_sap_dstrbtr_nm,
                    dstrbtr_grp_cd as sap_prnt_cust_key,
                    sap_prnt_cust_desc,
                    sku_cd as matl_num
                from wks_indonesia_base
            ) a,
(
                select distinct "year",
                    mnth_id
                from edw_vw_os_time_dim
                where "year" >= (DATE_PART(YEAR, current_timestamp) -6)
            ) b
    ) all_months,
    wks_indonesia_base b
where  
    upper(all_months.jj_sap_dstrbtr_nm) = Upper(b.jj_sap_dstrbtr_nm (+))
    and all_months.sap_prnt_cust_key = b.dstrbtr_grp_cd (+)
    and all_months.sap_prnt_cust_desc = b.sap_prnt_cust_desc (+)
    and all_months.matl_num = b.sku_cd (+)
    and mnth_id = month(+)
group by 
    all_months.jj_sap_dstrbtr_nm,
    all_months.sap_prnt_cust_key,
    all_months.sap_prnt_cust_desc,
    all_months.matl_num,
    all_months.mnth_id
),
final as 
(
    select
    jj_sap_dstrbtr_nm::varchar(75) as jj_sap_dstrbtr_nm,
	sap_parent_customer_key::varchar(25) as sap_parent_customer_key,
	sap_parent_customer_desc::varchar(155) as sap_parent_customer_desc,
	matl_num::varchar(50) as matl_num,
	month::varchar(23) as month,
	so_qty::number(38,4) as so_qty,
	so_value::number(38,4) as so_value,
	inv_qty::number(38,4) as inv_qty,
	inv_value::number(38,4) as inv_value,
	sell_in_qty::number(38,4) as sell_in_qty,
	sell_in_value::number(38,4) as sell_in_value
    from trans
)
select * from final