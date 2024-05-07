with wks_ph_allmonths_base as (
    select * from {{ ref('phlwks_integration__wks_ph_allmonths_base') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
last_3_months as
(
    select base3.sap_parent_customer_key,
        base3.dstrbtr_grp_cd,
        base3.dstr_cd_nm,
        base3.parent_customer_cd,
        base3.sls_grp_desc,
        base3.matl_num,
        mnth_id,
        sum(so_qty) as last_3months_so_matl,
        sum(so_value) as last_3months_so_value_matl
    from
        (
            select * from wks_ph_allmonths_base
            where left(month, 4) >= (DATE_PART(YEAR, current_timestamp) -6)
        ) base3,
        (
            select mnth_id,
                third_month
            from
                (
                    select year,
                        mnth_id,
                        lag(mnth_id, 2) over (
                            order by mnth_id
                        ) third_month
                    from (
                            select distinct "year" as year,
                                mnth_id
                            from edw_vw_os_time_dim -- limit 100
                            where year >= (DATE_PART(YEAR, current_timestamp) -6)
                        )
                ) month_base
        ) to_month
    where month <= mnth_id
        and month >= third_month
    group by base3.sap_parent_customer_key,
        base3.dstrbtr_grp_cd,
        base3.dstr_cd_nm,
        base3.parent_customer_cd,
        base3.sls_grp_desc,
        base3.matl_num,
        mnth_id
),
last_6_months as
(
    select base6.sap_parent_customer_key,
        base6.dstrbtr_grp_cd,
        base6.dstr_cd_nm,
        base6.parent_customer_cd,
        base6.sls_grp_desc,
        base6.matl_num,
        mnth_id,
        sum(so_qty) as last_6months_so_matl,
        sum(so_value) as last_6months_so_value_matl
    from
        (
            select *
            from WKS_PH_allmonths_base
            where left(month, 4) >= (DATE_PART(YEAR, current_timestamp) -6)
        ) base6,
        (
            select mnth_id,
                sixth_month
            from
                (
                    select year,
                        mnth_id,
                        lag(mnth_id, 5) over (
                            order by mnth_id
                        ) sixth_month
                    from (
                            select distinct "year" as year,
                                mnth_id as mnth_id
                            from edw_vw_os_time_dim -- limit 100
                            where year >= (DATE_PART(YEAR, current_timestamp) -6)
                        )
                ) month_base
        ) to_month
    where month <= mnth_id
        and month >= sixth_month
    group by base6.sap_parent_customer_key,
        base6.dstrbtr_grp_cd,
        base6.dstr_cd_nm,
        base6.parent_customer_cd,
        base6.sls_grp_desc,
        base6.matl_num,
        mnth_id
),
last_12_months as
(
    select base12.sap_parent_customer_key,
        base12.dstrbtr_grp_cd,
        base12.dstr_cd_nm,
        base12.parent_customer_cd,
        base12.sls_grp_desc,
        base12.matl_num,
        mnth_id,
        sum(so_qty) as last_12months_so_matl,
        sum(so_value) as last_12months_so_value_matl
    from
        (
            select *
            from wks_ph_allmonths_base
            where left(month, 4) >= (DATE_PART(YEAR, current_timestamp) -6)
        ) base12,
        (
            select mnth_id,
                twelfth_month
            from
                (
                    select year,
                        mnth_id,
                        lag(mnth_id, 11) over (order by mnth_id) twelfth_month
                    from
                        (
                            select distinct "year" as year,
                                mnth_id as mnth_id
                            from edw_vw_os_time_dim -- limit 100
                            where year >= (DATE_PART(YEAR, current_timestamp) -6)
                        )
                ) month_base
        ) to_month
    where month <= mnth_id
        and month >= twelfth_month
    group by base12.sap_parent_customer_key,
        base12.dstrbtr_grp_cd,
        base12.dstr_cd_nm,
        base12.parent_customer_cd,
        base12.sls_grp_desc,
        base12.matl_num,
        mnth_id
),
last_36_months as
(
    select base36.sap_parent_customer_key,
        base36.dstrbtr_grp_cd,
        base36.dstr_cd_nm,
        base36.parent_customer_cd,
        base36.sls_grp_desc,
        base36.matl_num,
        mnth_id,
        sum(so_qty) as last_36months_so_matl,
        sum(so_value) as last_36months_so_value_matl
    from
        (
            select *
            from WKS_PH_allmonths_base
            where left(month, 4) >= (DATE_PART(YEAR, current_timestamp) -6)
        ) base36,
        (
            select mnth_id,
                thirtysixth_month
            from (
                    select year,
                        mnth_id,
                        lag(mnth_id, 35) over (
                            order by mnth_id
                        ) thirtysixth_month
                    from (
                            select distinct "year" as year,
                                mnth_id as mnth_id
                            from edw_vw_os_time_dim -- limit 100
                            where year >= (DATE_PART(YEAR, current_timestamp) -6)
                        )
                ) month_base
        ) to_month
    where month <= mnth_id
        and month >= thirtysixth_month
    group by base36.sap_parent_customer_key,
        base36.dstrbtr_grp_cd,
        base36.dstr_cd_nm,
        base36.parent_customer_cd,
        base36.sls_grp_desc,
        base36.matl_num,
        mnth_id
),
final as
(
    select
        sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
        dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
        dstr_cd_nm::varchar(308) as dstr_cd_nm,
        parent_customer_cd::varchar(50) as parent_customer_cd,
        sls_grp_desc::varchar(255) as sls_grp_desc,
        nvl(nullif(matl_num, ''), 'NA')::varchar(255) as matl_num,
        month::varchar(23) as month,
        sum(so_qty)::number(38,6) as  so_qty,
        sum(so_value)::number(38,12) as  so_value,
        sum(inv_qty)::number(38,4) as  inv_qty,
        sum(inv_value)::number(38,8) as  inv_value,
        sum(sell_in_qty)::number(38,5) as  sell_in_qty,
        sum(sell_in_value)::number(38,5) as  sell_in_value,
        sum(last_3months_so)::number(38,6)  as last_3months_so,
        sum(last_3months_so_value)::number(38,12) as last_3months_so_value,
        sum(last_6months_so)::number(38,6) as last_6months_so,
        sum(last_6months_so_value)::number(38,12) as last_6months_so_value,
        sum(last_12months_so)::number(38,6) as last_12months_so,
        sum(last_12months_so_value)::number(38,12) as last_12months_so_value,
        sum(last_36months_so)::number(38,6) as last_36months_so,
        sum(last_36months_so_value)::number(38,12) as last_36months_so_value
    from
        (
            select base.sap_parent_customer_key,
                base.dstrbtr_grp_cd,
                base.dstr_cd_nm,
                base.parent_customer_cd,
                base.sls_grp_desc,
                base.matl_num,
                base.month,
                so_qty,
                so_value,
                inv_qty,
                inv_value,
                sell_in_qty,
                sell_in_value,
                last_3_months.last_3months_so_matl as last_3months_so,
                last_3_months.last_3months_so_value_matl as last_3months_so_value,
                last_6_months.last_6months_so_matl as last_6months_so,
                last_6_months.last_6months_so_value_matl as last_6months_so_value,
                last_12_months.last_12months_so_matl as last_12months_so,
                last_12_months.last_12months_so_value_matl as last_12months_so_value,
                last_36_months.last_36months_so_matl as last_36months_so,
                last_36_months.last_36months_so_value_matl as last_36months_so_value
            from WKS_PH_allmonths_base base,
                last_3_months,
                last_6_months,
                last_12_months,
                last_36_months
            where base.sap_parent_customer_key = last_3_months.sap_parent_customer_key (+)
                and base.dstrbtr_grp_cd = last_3_months.dstrbtr_grp_cd (+)
                and base.dstr_cd_nm = last_3_months.dstr_cd_nm (+)
                and base.parent_customer_cd = last_3_months.parent_customer_cd (+) --  and  base.sls_grp_desc   = last_3_months.sls_grp_desc       (+)
                and base.matl_num = last_3_months.matl_num (+)
                and base.month = last_3_months.mnth_id(+)
                and base.sap_parent_customer_key = last_6_months.sap_parent_customer_key (+)
                and base.dstrbtr_grp_cd = last_6_months.dstrbtr_grp_cd (+)
                and base.dstr_cd_nm = last_6_months.dstr_cd_nm (+)
                and base.parent_customer_cd = last_6_months.parent_customer_cd (+) --and  base.sls_grp_desc   = last_6_months.sls_grp_desc       (+)
                and base.matl_num = last_6_months.matl_num (+)
                and base.month = last_6_months.mnth_id(+)
                and base.sap_parent_customer_key = last_12_months.sap_parent_customer_key (+)
                and base.dstrbtr_grp_cd = last_12_months.dstrbtr_grp_cd (+)
                and base.dstr_cd_nm = last_12_months.dstr_cd_nm (+)
                and base.parent_customer_cd = last_12_months.parent_customer_cd (+) --and  base.sls_grp_desc   = last_12_months.sls_grp_desc       (+)
                and base.matl_num = last_12_months.matl_num (+)
                and base.month = last_12_months.mnth_id(+)
                and base.sap_parent_customer_key = last_36_months.sap_parent_customer_key (+)
                and base.dstrbtr_grp_cd = last_36_months.dstrbtr_grp_cd (+)
                and base.dstr_cd_nm = last_36_months.dstr_cd_nm (+)
                and base.parent_customer_cd = last_36_months.parent_customer_cd (+) --and  base.sls_grp_desc   = last_36_months.sls_grp_desc       (+)
                and base.matl_num = last_36_months.matl_num (+)
                and base.month = last_36_months.mnth_id(+)
        )
    group by sap_parent_customer_key,
        dstrbtr_grp_cd,
        dstr_cd_nm,
        parent_customer_cd,
        sls_grp_desc,
        matl_num,
        month
)
select * from final