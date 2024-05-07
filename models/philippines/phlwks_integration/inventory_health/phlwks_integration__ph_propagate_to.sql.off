with
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_ph_base_detail as
(
    select * from {{ ref('phlwks_integration__wks_ph_base_detail') }}
),
trans as
(
    Select sap_parent_customer_key,
    dstrbtr_grp_cd,
    dstr_cd_nm,
    parent_customer_cd,
    sls_grp_desc,
    month,
    so_qty,
    so_value,
    inv_qty,
    inv_value,
    case
        when month > (
            SELECT
                third_month
            FROM (
                    SELECT mnth_id,
                        LAG(mnth_id, 2) OVER (
                            ORDER BY mnth_id
                        ) third_month
                    FROM (
                            SELECT DISTINCT "year",
                                mnth_id
                            FROM edw_vw_os_time_dim
                            WHERE mnth_id <= (
                                    SELECT DISTINCT MNTH_ID
                                    FROM edw_vw_os_time_dim
                                    WHERE cal_date = current_timestamp()::date
                                )
                        )
                )
            WHERE mnth_id = (
                    SELECT DISTINCT MNTH_ID
                    FROM edw_vw_os_time_dim
                    WHERE cal_date = current_timestamp()::date
                )
        )
        and (
            nvl(so_value, 0) = 0
            or nvl(inv_value, 0) = 0
        ) then 'Y'
        else 'N'
    end as propagate_flag,
    max(month) over(
        partition by sap_parent_customer_key,
        dstrbtr_grp_cd,
        dstr_cd_nm,
        parent_customer_cd
    ) latest_month
from (
        Select sap_parent_customer_key,
            dstrbtr_grp_cd,
            dstr_cd_nm,
            parent_customer_cd,
            sls_grp_desc,
            month,
            sum(so_qty) as so_qty,
            sum(so_value) as so_value,
            sum(inv_qty) as inv_qty,
            sum(inv_value) as inv_value
        from wks_ph_base_detail
        where
            month <= (
                SELECT DISTINCT MNTH_ID
                FROM edw_vw_os_time_dim
                where cal_date = current_timestamp()::date
            )
        group by sap_parent_customer_key,
            dstrbtr_grp_cd,
            dstr_cd_nm,
            parent_customer_cd,
            sls_grp_desc,
            month
    )
),
final as
(
    select
    sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
	dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
	dstr_cd_nm::varchar(308) as dstr_cd_nm,
	parent_customer_cd::varchar(50) as parent_customer_cd,
	sls_grp_desc::varchar(255) as sls_grp_desc,
	month::varchar(23) as month,
	so_qty::number(38,6) as so_qty,
	so_value::number(38,12) as so_value,
	inv_qty::number(38,4) as inv_qty,
	inv_value::number(38,8) as inv_value,
	propagate_flag::varchar(1) as propagate_flag,
	latest_month::varchar(23) as latest_month 
    from trans
)
select * from final