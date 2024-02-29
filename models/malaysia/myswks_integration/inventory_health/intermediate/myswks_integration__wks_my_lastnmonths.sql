with wks_my_allmonths_base as
(
    select * from {{ ref('myswks_integration__wks_my_allmonths_base') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
base as
(
    select
    *
    from wks_my_allmonths_base
    where
    left(month, 4) >= (  date_part(year, current_timestamp()::timestampntz) - 6)
),
month_base3 as
(
    select
        year,
        mnth_id,
        lag(mnth_id, 2) over (order by mnth_id) as third_month
    from 
    (
        select distinct
        cal_year as year,
        cal_mnth_id as mnth_id
        from edw_vw_os_time_dim
        where
        cal_year >= ( date_part(year, current_timestamp()::timestampntz) - 6)
    )
),
to_month3 as
(
      select
        mnth_id,
        third_month
      from month_base3

),
last_3_months as
(
    select
        base.distributor,
        base.dstrbtr_grp_cd,
        base.sap_prnt_cust_key,
        base.sap_prnt_cust_desc,
        base.matl_num,
        mnth_id,
        sum(so_qty) as last_3months_so_matl,
        sum(so_value) as last_3months_so_value_matl
    from base,  to_month3
    where
    month <= mnth_id and month >= third_month
    group by
        base.distributor,
        base.dstrbtr_grp_cd,
        base.sap_prnt_cust_key,
        base.sap_prnt_cust_desc,
        base.matl_num,
        mnth_id

),
month_base6 as
(
        select
          year,
          mnth_id,
          lag(mnth_id, 5) over (order by mnth_id) as sixth_month
        from (
          select distinct
            cal_year as year,
            cal_mnth_id as mnth_id
          from edw_vw_os_time_dim
          where
            cal_year >= (
              date_part(year, current_timestamp()::timestampntz) - 6
            )
        )
),
to_month6 as(
      select mnth_id,sixth_month
      from month_base6
),
last_6_months as
(
    select
        base.distributor,
        base.dstrbtr_grp_cd,
        base.sap_prnt_cust_key,
        base.sap_prnt_cust_desc,
        base.matl_num,
        mnth_id,
        sum(so_qty) as last_6months_so_matl,
        sum(so_value) as last_6months_so_value_matl
    from base, to_month6
    where
      month <= mnth_id and month >= sixth_month
    group by
        base.distributor,
        base.dstrbtr_grp_cd,
        base.sap_prnt_cust_key,
        base.sap_prnt_cust_desc,
        base.matl_num,
        mnth_id
),
month_base12 as
(
    select
        year,
        mnth_id,
        lag(mnth_id, 11) over (order by mnth_id) as twelfth_month
    from (
        select distinct
        cal_year as year,
        cal_mnth_id as mnth_id
        from edw_vw_os_time_dim
        where
        cal_year >= (date_part(year, current_timestamp()::timestampntz) - 6)
    )
),
to_month12 as(
      select mnth_id,twelfth_month
      from month_base12
),
last_12_months as
(
    select
        base.distributor,
        base.dstrbtr_grp_cd,
        base.sap_prnt_cust_key,
        base.sap_prnt_cust_desc,
        base.matl_num,
        mnth_id,
        sum(so_qty) as last_12months_so_matl,
        sum(so_value) as last_12months_so_value_matl
    from  base, to_month12
    where
        month <= mnth_id and month >= twelfth_month
    group by
        base.distributor,
        base.dstrbtr_grp_cd,
        base.sap_prnt_cust_key,
        base.sap_prnt_cust_desc,
        base.matl_num,
        mnth_id 
),
month_base36 as(
    select
        year,
        mnth_id,
        lag(mnth_id, 35) over (order by mnth_id) as thirtysixth_month
    from (
        select distinct
        cal_year as year,
        cal_mnth_id as mnth_id
        from edw_vw_os_time_dim
        where
        cal_year >= (
            date_part(year, current_timestamp()::timestampntz) - 6
        )
    )
),
to_month36 as 
(
    select
    mnth_id,
    thirtysixth_month
    from  month_base36
),
last_36_months as
(
    select
      base.distributor,
      base.dstrbtr_grp_cd,
      base.sap_prnt_cust_key,
      base.sap_prnt_cust_desc,
      base.matl_num,
      mnth_id,
      sum(so_qty) as last_36months_so_matl,
      sum(so_value) as last_36months_so_value_matl
    from base, to_month36
    where
      month <= mnth_id and month >= thirtysixth_month
    group by
      base.distributor,
      base.dstrbtr_grp_cd,
      base.sap_prnt_cust_key,
      base.sap_prnt_cust_desc,
      base.matl_num,
      mnth_id
)
,
final as 
(
    select
        distributor::varchar(40) as distributor,
        dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
        sap_prnt_cust_key::varchar(12) as sap_parent_customer_key,
        sap_prnt_cust_desc::varchar(50) as sap_parent_customer_desc,
        coalesce(nullif(matl_num, ''), 'NA')::varchar(100) as matl_num,
        month::number(18,0) as month,
        sum(so_qty)::number(38,6) as so_qty,
        sum(so_value)::number(38,17) as so_value,
        sum(inv_qty)::number(38,4) as inv_qty,
        sum(inv_value)::number(38,13) as inv_value,
        sum(sell_in_qty)::number(38,4) as sell_in_qty,
        sum(sell_in_value)::number(38,13) as sell_in_value,
        sum(last_3months_so)::number(38,6) as last_3months_so,
        sum(last_3months_so_value)::number(38,17) as last_3months_so_value,
        sum(last_6months_so)::number(38,6) as last_6months_so,
        sum(last_6months_so_value)::number(38,17) as last_6months_so_value,
        sum(last_12months_so)::number(38,6) as last_12months_so,
        sum(last_12months_so_value)::number(38,17) as last_12months_so_value,
        sum(last_36months_so)::number(38,6) as last_36months_so,
        sum(last_36months_so_value)::number(38,17) as last_36months_so_value
    from 
        (select 
        base.distributor,
        base.dstrbtr_grp_cd,
        base.sap_prnt_cust_key,
        base.sap_prnt_cust_desc,
        base.matl_num,
        base.month,
        so_qty,
        so_value,
        inv_qty,
        inv_value,
        sell_in_qty,
        sell_in_value,
        last_3_months.last_3months_so_matl AS last_3months_so,
        last_3_months.last_3months_so_value_matl AS last_3months_so_value,
        last_6_months.last_6months_so_matl AS last_6months_so,
        last_6_months.last_6months_so_value_matl AS last_6months_so_value,
        last_12_months.last_12months_so_matl AS last_12months_so,
        last_12_months.last_12months_so_value_matl AS last_12months_so_value,
        last_36_months.last_36months_so_matl AS last_36months_so,
        last_36_months.last_36months_so_value_matl AS last_36months_so_value
        from wks_my_allmonths_base as base,
        last_3_months, last_6_months, last_12_months, 
        last_36_months
    where
        base.distributor = last_3_months.distributor(+)
        and base.dstrbtr_grp_cd = last_3_months.dstrbtr_grp_cd(+)
        and base.sap_prnt_cust_key = last_3_months.sap_prnt_cust_key(+)
        and base.sap_prnt_cust_desc = last_3_months.sap_prnt_cust_desc(+)
        and base.matl_num = last_3_months.matl_num(+)
        and base.month = last_3_months.mnth_id(+)
        and base.distributor = last_6_months.distributor(+)
        and base.dstrbtr_grp_cd = last_6_months.dstrbtr_grp_cd(+)
        and base.sap_prnt_cust_key = last_6_months.sap_prnt_cust_key(+)
        and base.sap_prnt_cust_desc = last_6_months.sap_prnt_cust_desc(+)
        and base.matl_num = last_6_months.matl_num(+)
        and base.month = last_6_months.mnth_id(+)
        and base.distributor = last_12_months.distributor(+)
        and base.dstrbtr_grp_cd = last_12_months.dstrbtr_grp_cd(+)
        and base.sap_prnt_cust_key = last_12_months.sap_prnt_cust_key(+)
        and base.sap_prnt_cust_desc = last_12_months.sap_prnt_cust_desc(+)
        and base.matl_num = last_12_months.matl_num(+)
        and base.month = last_12_months.mnth_id(+)
        and base.distributor = last_36_months.distributor(+)
        and base.dstrbtr_grp_cd = last_36_months.dstrbtr_grp_cd(+)
        and base.sap_prnt_cust_key = last_36_months.sap_prnt_cust_key(+)
        and base.sap_prnt_cust_desc = last_36_months.sap_prnt_cust_desc(+)
        and base.matl_num = last_36_months.matl_num(+)
        and base.month = last_36_months.mnth_id(+)
    )
    group by
        distributor,
        dstrbtr_grp_cd,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        matl_num,
        month
)
select * from final
