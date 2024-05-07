with 
wks_ph_base_detail as 
(
    select * from {{ ref('phlwks_integration__wks_ph_base_detail') }}
),
ph_propagate_to as
(
    select * from {{ ref('phlwks_integration__ph_propagate_to') }}
),
trans as
(
    select 
    p_to.sap_parent_customer_key,
    p_to.dstrbtr_grp_cd,
    p_to.dstr_cd_nm,
    p_to.parent_customer_cd,
    p_to.sls_grp_desc,
    p_to.latest_month,
    p_to.month propagate_to,
    base.month propagate_from,
    base.so_qty,
    base.inv_qty,
    datediff(
        month,
        to_date(base.month, 'YYYYMM'),
        to_date(latest_month, 'YYYYMM')
    ) diff_month,
    p_to.reason
from (
        select *,
            case
                when propagate_flag = 'Y'
                and (
                    nvl(so_value, 0) = 0
                    and nvl(inv_value, 0) = 0
                ) then 'Sellout and Inventory Missing'
                when propagate_flag = 'Y'
                and nvl(so_value, 0) = 0 then 'Sellout Missing'
                when propagate_flag = 'Y'
                and nvl(inv_value, 0) = 0 then 'Inventory Missing'
                else 'Not Propagate'
            end as reason
        from ph_propagate_to
        where propagate_flag = 'Y'
    ) p_to,
    (
        select 
            d.sap_parent_customer_key,
            d.dstrbtr_grp_cd,
            d.dstr_cd_nm,
            d.parent_customer_cd,
            d.sls_grp_desc,
            d.month as month,
            sum(so_qty) as so_qty,
            sum(so_value) as so_value,
            sum(inv_qty) as inv_qty,
            sum(inv_value) as inv_value
        from (
                select 
                    sap_parent_customer_key,
                    dstrbtr_grp_cd,
                    dstr_cd_nm,
                    parent_customer_cd,
                    sls_grp_desc,
                    max(month) as month
                from (
                        Select sap_parent_customer_key,
                            dstrbtr_grp_cd,
                            dstr_cd_nm,
                            parent_customer_cd,
                            sls_grp_desc,
                            month
                        from wks_ph_base_detail
                        where (sap_parent_customer_key, month) not in (
                                select sap_parent_customer_key,
                                    month
                                from ph_propagate_to
                                where propagate_flag = 'Y'
                            )
                        group by sap_parent_customer_key,
                            dstrbtr_grp_cd,
                            dstr_cd_nm,
                            parent_customer_cd,
                            sls_grp_desc,
                            month
                        having (
                                sum(so_qty) > 0
                                and sum(inv_qty) > 0
                            )
                    ) all_months
                group by sap_parent_customer_key,
                    dstrbtr_grp_cd,
                    dstr_cd_nm,
                    parent_customer_cd,
                    sls_grp_desc
            ) max_month,
            wks_ph_base_detail d
        where max_month.sap_parent_customer_key = d.sap_parent_customer_key
            and max_month.month = d.month
        group by d.sap_parent_customer_key,
            d.dstrbtr_grp_cd,
            d.dstr_cd_nm,
            d.parent_customer_cd,
            d.sls_grp_desc,
            d.month
    ) base
where p_to.sap_parent_customer_key = base.sap_parent_customer_key
    and p_to.dstrbtr_grp_cd = base.dstrbtr_grp_cd
    and p_to.dstr_cd_nm = base.dstr_cd_nm
    and p_to.parent_customer_cd = base.parent_customer_cd            
    and base.month < p_to.month
    and datediff(
        month,
        to_date(base.month, 'YYYYMM'),
        to_date(latest_month, 'YYYYMM')
    ) <= 2
),
final as
(
    select 
    sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
	dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
	dstr_cd_nm::varchar(308) as dstr_cd_nm,
	parent_customer_cd::varchar(50) as parent_customer_cd,
	sls_grp_desc::varchar(255) as sls_grp_desc,
	latest_month::varchar(23) as latest_month,
	propagate_to::varchar(23) as propagate_to,
	propagate_from::varchar(23) as propagate_from,
	so_qty::number(38,6) as so_qty,
	inv_qty::number(38,4) as inv_qty,
	diff_month::number(38,0) as diff_month,
	reason::varchar(29) as reason
    from trans
)
select * from final