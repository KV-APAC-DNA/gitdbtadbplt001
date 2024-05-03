with
itg_mds_rg_tih_distributor_closure as
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_tih_distributor_closure') }}
),
wks_ph_lastnmonths as 
(
    select * from {{ ref('phlwks_integration__wks_ph_lastnmonths') }}
),
wks_ph_base as 
(
    select * from from {{ ref('phlwks_integration__wks_ph_base') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
trans as (
Select sap_parent_customer_key,
    dstrbtr_grp_cd,
    dstr_cd_nm,
    parent_customer_cd,
    sls_grp_desc,
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
                Select sap_parent_customer_key,
                    dstrbtr_grp_cd,
                    dstr_cd_nm,
                    parent_customer_cd,
                    sls_grp_desc,
                    matl_num,
                    month,
                    matl_num as base_matl_num,
                    replicated_flag,
                    sum(so_qty) as so_qty,
                    sum(so_value) as so_value,
                    sum(inv_qty) as inv_qty,
                    sum(inv_value) as inv_value,
                    sum(sell_in_qty) as sell_in_qty,
                    sum(sell_in_value) as sell_in_value,
                    sum(last_3months_so) as last_3months_so,
                    sum(last_6months_so) as last_6months_so,
                    sum(last_12months_so) as last_12months_so,
                    sum(last_3months_so_value) as last_3months_so_value,
                    sum(last_6months_so_value) as last_6months_so_value,
                    sum(last_12months_so_value) as last_12months_so_value,
                    sum(last_36months_so_value) as last_36months_so_value
                from (
                        select agg.sap_parent_customer_key,
                            agg.dstrbtr_grp_cd,
                            agg.dstr_cd_nm,
                            agg.parent_customer_cd,
                            agg.sls_grp_desc,
                            agg.matl_num,
                            agg.month,
                            base.matl_num as base_matl_num,
                            base.so_sls_qty as so_qty,
                            base.so_grs_trd_sls as so_value,
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
                                when (base.matl_num is NULL) then 'Y'
                                else 'N'
                            end as replicated_flag
                        from wks_ph_lastnmonths agg,
                            wks_ph_base base
                        where left(agg.month, 4) >= (date_part(year, current_timestamp()) - 2)
                            and agg.sap_parent_customer_key = base.sap_prnt_cust_key (+)
                            and agg.dstrbtr_grp_cd = base.dstrbtr_grp_cd (+)
                            and agg.dstr_cd_nm = base.dstr_cd_nm (+)
                            and agg.parent_customer_cd = base.parent_customer_cd
                            and agg.matl_num = base.matl_num (+)
                            and agg.month = base.jj_mnth_id (+)
                            and agg.month <= (
                                select distinct mnth_id as mnth_id
                                from edw_vw_os_time_dim
                                where cal_date = current_timestamp()::date
                            )
                    )
                group by sap_parent_customer_key,
                    dstrbtr_grp_cd,
                    dstr_cd_nm,
                    parent_customer_cd,
                    sls_grp_desc,
                    matl_num,
                    month,
                    replicated_flag
            ) base
            LEFT JOIN (
                select distinct dstrbtr_grp_cd,
                    closemonth
                from itg_mds_rg_tih_distributor_closure
                where upper(market) = 'PHILIPPINES'
            ) dist_closure ON base.dstrbtr_grp_cd = dist_closure.dstrbtr_grp_cd
    ) base2
where is_valid = 1
),
final as
( 
    select
    sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
	dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
	dstr_cd_nm::varchar(308) as dstr_cd_nm,
	parent_customer_cd::varchar(50) as parent_customer_cd,
	sls_grp_desc::varchar(255) as sls_grp_desc,
	matl_num::varchar(255) as matl_num,
	month::varchar(23) as month,
	base_matl_num::varchar(255) as base_matl_num,
	replicated_flag::varchar(1) as replicated_flag,
	so_qty::number(38,6) as so_qty,
	so_value::number(38,12) as so_value,
	inv_qty::number(38,4) as inv_qty,
	inv_value::number(38,8) as inv_value,
	sell_in_qty::number(38,5) as sell_in_qty,
	sell_in_value::number(38,5) as sell_in_value,
	last_3months_so::number(38,6) as last_3months_so,
	last_6months_so::number(38,6) as last_6months_so,
	last_12months_so::number(38,6) as last_12months_so,
	last_3months_so_value::number(38,12) as last_3months_so_value,
	last_6months_so_value::number(38,12) as last_6months_so_value,
	last_12months_so_value::number(38,12) as last_12months_so_value,
	last_36months_so_value::number(38,12) as last_36months_so_value
    from trans
)
select * from final