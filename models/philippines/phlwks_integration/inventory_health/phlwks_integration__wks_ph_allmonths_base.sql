with wks_ph_base as (
    select * from {{ ref('phlwks_integration__wks_ph_base') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
all_months as
(
    select distinct sap_prnt_cust_key,
        dstrbtr_grp_cd,
        dstr_cd_nm,
        parent_customer_cd,
        sls_grp_desc,
        matl_num,
        mnth_id
    from 
        (
            select distinct sap_prnt_cust_key,
                dstrbtr_grp_cd,
                dstr_cd_nm,
                parent_customer_cd,
                sls_grp_desc,
                matl_num
            from wks_ph_base
        ) a,
        (
            select distinct "year",
                mnth_id
            from edw_vw_os_time_dim
            where "year" >= (date_part(year, current_timestamp) -6)
        ) b
),
final as
(    
    select 
        all_months.sap_prnt_cust_key::varchar(50) as sap_parent_customer_key,
        all_months.dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
        all_months.dstr_cd_nm::varchar(308) as dstr_cd_nm,
        all_months.parent_customer_cd::varchar(50) as parent_customer_cd,
        all_months.sls_grp_desc::varchar(255) as sls_grp_desc,
        all_months.matl_num::varchar(255) as matl_num,
        all_months.mnth_id::varchar(23) as month,
        sum(b.so_sls_qty)::number(38,6) as so_qty,
        sum(b.so_grs_trd_sls)::number(38,12) as so_value,
        sum(b.inventory_quantity)::number(38,4) as inv_qty,
        sum(b.inventory_val)::number(38,8) as inv_value,
        sum(b.si_sls_qty)::number(38,5) as sell_in_qty,
        sum(b.si_gts_val)::number(38,5) as sell_in_value
    from 
        all_months,
        wks_ph_base b
    where all_months.sap_prnt_cust_key = b.sap_prnt_cust_key (+)
        and all_months.dstrbtr_grp_cd = b.dstrbtr_grp_cd (+)
        and all_months.dstr_cd_nm = b.dstr_cd_nm (+)
        and all_months.parent_customer_cd = b.parent_customer_cd (+)
        and all_months.matl_num = b.matl_num (+)
        and mnth_id = jj_mnth_id(+)
    group by all_months.sap_prnt_cust_key,
        all_months.dstrbtr_grp_cd,
        all_months.dstr_cd_nm,
        all_months.parent_customer_cd,
        all_months.sls_grp_desc,
        all_months.matl_num,
        all_months.mnth_id
)
select * from final