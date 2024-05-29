with
wks_indonesia_lastnmonths as 
(
    select * from idnwks_integration.wks_indonesia_lastnmonths
),
wks_indonesia_base as 
(
    select * from {{ ref('idnwks_interation__wks_indonesia_base') }}
),
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_rg_tih_distributor_closure as 
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_tih_distributor_closure') }}
),
trans as 
(
    select 
    jj_sap_dstrbtr_nm,
    sap_parent_customer_key,
    sap_parent_customer_desc,
    matl_num,
    month,
    base_matl_num,
    replicated_flag,
    so_qty,
    so_value,
    inv_qty,
    inv_value,
    sell_in_qty,
    sell_in_value,
    last_3months_so,
    last_6months_so,
    last_12months_so,
    last_3months_so_value,
    last_6months_so_value,
    last_12months_so_value,
    last_36months_so_value
from (
        select base.*,
            case
                when dist_closure.dstrbtr_grp_cd is not null
                and month > dist_closure.closemonth then 0
                else 1
            end as is_valid
        from (
                Select 
                    jj_sap_dstrbtr_nm,
                    sap_parent_customer_key,
                    sap_parent_customer_desc,
                    matl_num,
                    month,
                    matl_num as base_matl_num,
                    replicated_flag,
                    sum(so_qty) as  so_qty,
                    sum(so_value) as  so_value,
                    sum(inv_qty) as  inv_qty,
                    sum(inv_value) as  inv_value,
                    sum(sell_in_qty) as  sell_in_qty,
                    sum(sell_in_value) as  sell_in_value,
                    sum(last_3months_so) as  last_3months_so,
                    sum(last_6months_so) as  last_6months_so,
                    sum(last_12months_so) as  last_12months_so,
                    sum(last_3months_so_value) as  last_3months_so_value,
                    sum(last_6months_so_value) as  last_6months_so_value,
                    sum(last_12months_so_value) as  last_12months_so_value,
                    sum(last_36months_so_value) as  last_36months_so_value
                from (
                        select
                            agg.jj_sap_dstrbtr_nm,
                            agg.sap_parent_customer_key,
                            agg.sap_parent_customer_desc,
                            agg.matl_num,
                            agg.month,
                            base.sku_cd as base_matl_num,
                            base.so_sls_qty as so_qty,
                            base.so_trd_sls as so_value,
                            base.inventory_quantity as inv_qty,
                            base.inventory_val as inv_value,
                            base.si_sls_qty as sell_in_qty,
                            base.si_gts_val as sell_in_value,
                            agg.last_3months_so,
                            agg.last_6months_so,
                            agg.last_12months_so,
                            agg.last_3months_so_value,
                            agg.last_6months_so_value,
                            agg.last_12months_so_value,
                            agg.last_36months_so_value,
                            case
                                when (base.sku_cd is NULL) then 'Y'
                                else 'N'
                            end as replicated_flag
                        from wks_indonesia_lastnmonths agg,
                            wks_indonesia_base base
                        where left(agg.month, 4) >= (date_part(year, current_timestamp()) - 2)
                            and agg.jj_sap_dstrbtr_nm = base.jj_sap_dstrbtr_nm (+)
                            and agg.sap_parent_customer_key = base.dstrbtr_grp_cd (+)
                            and agg.sap_parent_customer_desc = base.sap_prnt_cust_desc (+)
                            and agg.matl_num = base.sku_Cd (+)
                            and agg.month = base.month (+)
                            and agg.month <= (
                                select distinct mnth_id as mnth_id
                                from edw_vw_os_time_dim
                                where cal_date = current_timestamp()::date
                            )
                    )
                group by jj_sap_dstrbtr_nm,
                    sap_parent_customer_key,
                    sap_parent_customer_desc,
                    matl_num,
                    month,
                    replicated_flag
            ) base
            LEFT JOIN (
                select distinct dstrbtr_grp_cd,
                    closemonth
                from itg_mds_rg_tih_distributor_closure
                where upper(market) = 'INDONESIA'
            ) dist_closure ON base.sap_parent_customer_key = dist_closure.dstrbtr_grp_cd
    ) base2
where is_valid = 1
),
final as 
(
    select 
    jj_sap_dstrbtr_nm::varchar(75) as jj_sap_dstrbtr_nm,
	sap_parent_customer_key::varchar(25) as sap_parent_customer_key,
	sap_parent_customer_desc::varchar(155) as sap_parent_customer_desc,
	matl_num::varchar(50) as matl_num,
	month::varchar(23) as month,
	base_matl_num::varchar(50) as base_matl_num,
	replicated_flag::varchar(1) as replicated_flag,
	so_qty::number(38,4) as so_qty,
	so_value::number(38,4) as so_value,
	inv_qty::number(38,4) as inv_qty,
	inv_value::number(38,4) as inv_value,
	sell_in_qty::number(38,4) as sell_in_qty,
	sell_in_value::number(38,4) as sell_in_value,
	last_3months_so::number(38,4) as last_3months_so,
	last_6months_so::number(38,4) as last_6months_so,
	last_12months_so::number(38,4) as last_12months_so,
	last_3months_so_value::number(38,4) as last_3months_so_value,
	last_6months_so_value::number(38,4) as last_6months_so_value,
	last_12months_so_value::number(38,4) as last_12months_so_value,
	last_36months_so_value::number(38,4) as last_36months_so_value
    from trans
)
select * from final