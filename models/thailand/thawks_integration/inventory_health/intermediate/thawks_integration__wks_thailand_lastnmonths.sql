with edw_vw_os_time_dim as(
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_thailand_allmonths_base as (
select * from {{ ref('thawks_integration__wks_thailand_allmonths_base') }}
),
last_3_months as (
     select
      base3.dstr_nm,
      base3.sap_parent_customer_key,
      base3.sap_parent_customer_desc,
      base3.matl_num,
      mnth_id,
      sum(so_qty) as last_3months_so_matl,
      sum(so_value) as last_3months_so_value_matl
    from (
      select
        *
      from wks_thailand_allmonths_base
      where
        left(month, 4) >= (
          date_part(year, current_timestamp()) - 6
        )
    ) as base3, (
      select
        mnth_id,
        third_month
      from (
        select
          year,
          mnth_id,
          lag(mnth_id, 2) over (order by mnth_id) as third_month
        from (
          select distinct
            cal_year as year,
            cal_mnth_id as mnth_id
          from edw_vw_os_time_dim /* limit 100 */
          where
            cal_year >= (
              date_part(year, current_timestamp()) - 6
            )
        )
      ) as month_base
    ) as to_month
    where
      month <= mnth_id and month >= third_month
    group by
      base3.dstr_nm,
      base3.sap_parent_customer_key,
      base3.sap_parent_customer_desc,
      base3.matl_num,
      mnth_id
 ),
 
 last_6_months as (
      select
      base6.dstr_nm,
      base6.sap_parent_customer_key,
      base6.sap_parent_customer_desc,
      base6.matl_num,
      mnth_id,
      sum(so_qty) as last_6months_so_matl,
      sum(so_value) as last_6months_so_value_matl
    from (
      select
        *
      from wks_thailand_allmonths_base
      where
        left(month, 4) >= (
          date_part(year, current_timestamp()) - 6
        )
    ) as base6, (
      select
        mnth_id,
        sixth_month
      from (
        select
          year,
          mnth_id,
          lag(mnth_id, 5) over (order by mnth_id) as sixth_month
        from (
          select distinct
            cal_year as year,
            cal_mnth_id as mnth_id
          from edw_vw_os_time_dim /* limit 100 */
          where
            cal_year >= (
              date_part(year, current_timestamp()) - 6
            )
        )
      ) as month_base
    ) as to_month
    where
      month <= mnth_id and month >= sixth_month
    group by
      base6.dstr_nm,
      base6.sap_parent_customer_key,
      base6.sap_parent_customer_desc,
      base6.matl_num,
      mnth_id
  
  ),
  
 last_12_months as (
      select
      base12.dstr_nm,
      base12.sap_parent_customer_key,
      base12.sap_parent_customer_desc,
      base12.matl_num,
      mnth_id,
      sum(so_qty) as last_12months_so_matl,
      sum(so_value) as last_12months_so_value_matl
    from (
      select
        *
      from wks_thailand_allmonths_base
      where
        left(month, 4) >= (
          date_part(year, current_timestamp()) - 6
        )
    ) as base12, (
      select
        mnth_id,
        twelfth_month
      from (
        select
          year,
          mnth_id,
          lag(mnth_id, 11) over (order by mnth_id) as twelfth_month
        from (
          select distinct
            cal_year as year,
            cal_mnth_id as mnth_id
          from edw_vw_os_time_dim /* limit 100 */
          where
            cal_year >= (
              date_part(year, current_timestamp()) - 6
            )
        )
      ) as month_base
    ) as to_month
    where
      month <= mnth_id and month >= twelfth_month
    group by
      base12.dstr_nm,
      base12.sap_parent_customer_key,
      base12.sap_parent_customer_desc,
      base12.matl_num,
      mnth_id
  
  ),
  
 last_36_months as (
     select
      base36.dstr_nm,
      base36.sap_parent_customer_key,
      base36.sap_parent_customer_desc,
      base36.matl_num,
      mnth_id,
      sum(so_qty) as last_36months_so_matl,
      sum(so_value) as last_36months_so_value_matl
    from (
      select
        *
      from wks_thailand_allmonths_base
      where
        left(month, 4) >= (
          date_part(year, current_timestamp()) - 6
        )
    ) as base36, (
      select
        mnth_id,
        thirtysixth_month
      from (
        select
          year,
          mnth_id,
          lag(mnth_id, 35) over (order by mnth_id) as thirtysixth_month
        from (
          select distinct
            cal_year as year,
            cal_mnth_id as mnth_id
          from edw_vw_os_time_dim /* limit 100 */
          where
            cal_year >= (
              date_part(year, current_timestamp()) - 6
            )
        )
      ) as month_base
    ) as to_month
    where
      month <= mnth_id and month >= thirtysixth_month
    group by
      base36.dstr_nm,
      base36.sap_parent_customer_key,
      base36.sap_parent_customer_desc,
      base36.matl_num,
      mnth_id
  
  ),
transformed as (
select
  dstr_nm,
  sap_parent_customer_key,
  sap_parent_customer_desc,
  coalesce(nullif(matl_num, ''), 'NA') as matl_num,
  month,
  sum(so_qty) as so_qty,
  sum(so_value) as so_value,
  sum(inv_qty) as inv_qty,
  sum(inv_value) as inv_value,
  sum(sell_in_qty) as sell_in_qty,
  sum(sell_in_value) as sell_in_value,
  sum(last_3months_so) as last_3months_so,
  sum(last_3months_so_value) as last_3months_so_value,
  sum(last_6months_so) as last_6months_so,
  sum(last_6months_so_value) as last_6months_so_value,
  sum(last_12months_so) as last_12months_so,
  sum(last_12months_so_value) as last_12months_so_value,
  sum(last_36months_so) as last_36months_so,
  sum(last_36months_so_value) as last_36months_so_value
from (
  select
    base.dstr_nm,
    base.sap_parent_customer_key,
    base.sap_parent_customer_desc,
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
  from wks_thailand_allmonths_base as base, 
  last_3_months, 
  last_6_months, 
  last_12_months, 
  last_36_months
  where
    base.sap_parent_customer_key = last_3_months.sap_parent_customer_key(+)
    and base.sap_parent_customer_desc = last_3_months.sap_parent_customer_desc(+)
    and base.dstr_nm = last_3_months.dstr_nm(+)
    and base.matl_num = last_3_months.matl_num(+)
    and base.month = last_3_months.mnth_id(+)
    and base.sap_parent_customer_key = last_6_months.sap_parent_customer_key(+)
    and base.sap_parent_customer_desc = last_6_months.sap_parent_customer_desc(+)
    and base.dstr_nm = last_6_months.dstr_nm(+)
    and base.matl_num = last_6_months.matl_num(+)
    and base.month = last_6_months.mnth_id(+)
    and base.sap_parent_customer_key = last_12_months.sap_parent_customer_key(+)
    and base.sap_parent_customer_desc = last_12_months.sap_parent_customer_desc(+)
    and base.dstr_nm = last_12_months.dstr_nm(+)
    and base.matl_num = last_12_months.matl_num(+)
    and base.month = last_12_months.mnth_id(+)
    and base.sap_parent_customer_key = last_36_months.sap_parent_customer_key(+)
    and base.sap_parent_customer_desc = last_36_months.sap_parent_customer_desc(+)
    and base.dstr_nm = last_36_months.dstr_nm(+)
    and base.matl_num = last_36_months.matl_num(+)
    and base.month = last_36_months.mnth_id(+)
)
group by
  dstr_nm,
  sap_parent_customer_key,
  sap_parent_customer_desc,
  matl_num,
  month
  
)
select * from transformed