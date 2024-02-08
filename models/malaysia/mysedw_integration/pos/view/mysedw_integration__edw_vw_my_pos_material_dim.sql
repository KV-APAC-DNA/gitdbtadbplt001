with edw_vw_os_time_dim as (
              select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_my_pos_sales_fact as (
              select * from {{ ref('mysitg_integration__itg_my_pos_sales_fact') }}
),
c as  (
    select distinct
    edw_vw_os_time_dim."year" || right('00' || edw_vw_os_time_dim.wk::text, 2 ) as yr_wk,
      edw_vw_os_time_dim.mnth_id
    from edw_vw_os_time_dim
    where
      (
        edw_vw_os_time_dim.cal_date = current_timestamp()::date
      )

),
f as 
  (
            select
              min(edw_vw_os_time_dim.cal_date) as eff_str_date,
              max(edw_vw_os_time_dim.cal_date) as eff_end_date,
            
    edw_vw_os_time_dim."year" || right('00' || edw_vw_os_time_dim.wk::text, 2 ) as yr_wk
            from edw_vw_os_time_dim
            group by
              edw_vw_os_time_dim."year" || right('00' || edw_vw_os_time_dim.wk::text, 2 )
          
),
a as 
  (
          select distinct
            p.cust_id,
            p.mt_item_cd,
            p.sap_matl_num,
            max(f.eff_end_date) over (partition by p.cust_id, p.mt_item_cd, p.sap_matl_num  order by null rows between unbounded preceding and unbounded following) as eff_end_date,
            min(f.eff_str_date) over (partition by p.cust_id, p.mt_item_cd, p.sap_matl_num  order by null rows between unbounded preceding and unbounded following) as eff_str_date
          from itg_my_pos_sales_fact as p,  f
          where
            (
              (
                (
                  not p.jj_yr_week_no is null
                )
                or (
                  cast((
                    p.jj_yr_week_no
                  ) as text) <> cast((
                    cast('' as varchar)
                  ) as text)
                )
              )
              and (
                cast((
                  p.jj_yr_week_no
                ) as text) = f.yr_wk
              )
            )
        
),
e as
  (
        select
          min(edw_vw_os_time_dim.cal_date) as eff_str_date,
          max(edw_vw_os_time_dim.cal_date) as eff_end_date,
          edw_vw_os_time_dim.mnth_id
        from edw_vw_os_time_dim
        group by
          edw_vw_os_time_dim.mnth_id
      
),
d as (
      select distinct
        s.cust_id,
        s.mt_item_cd,
        s.sap_matl_num,
        max(e.eff_end_date) over (partition by s.cust_id, s.mt_item_cd, s.sap_matl_num  order by null rows between unbounded preceding and unbounded following) as eff_end_date,
        min(e.eff_str_date) over (partition by s.cust_id, s.mt_item_cd, s.sap_matl_num  order by null rows between unbounded preceding and unbounded following) as eff_str_date
      from itg_my_pos_sales_fact as s,e
      where
        (
          (
            (
              s.jj_yr_week_no is null
            )
            or (
              cast((
                s.jj_yr_week_no
              ) as text) = cast((
                cast('' as varchar)
              ) as text)
            )
          )
          and (
            cast((
              s.jj_mnth_id
            ) as text) = e.mnth_id
          )
        )
    
),
derived_table1 as 
  (
  select distinct
    cast('MY' as varchar) as cntry_cd,
    cast('Malaysia' as varchar) as cntry_nm,
    cast(null as varchar) as jj_mnth_id,
    b.cust_id,
    b.mt_item_cd as item_cd,
    cast((
      max(cast((
        b.mt_item_desc
      ) as text))
    ) as varchar) as item_nm,
    b.sap_matl_num as sap_item_cd,
    cast(null as varchar) as bar_cd,
    cast(null as varchar) as cust_sku_grp,
    1 as cust_conv_factor,
    cast((
      cast(null as decimal)
    ) as decimal(18, 0)) as cust_item_prc,
    cast(null as varchar) as lst_period,
    cast(null as varchar) as early_bk_period,
    case
      when (
        (
          not b.jj_yr_week_no is null
        )
        or (
          cast((
            b.jj_yr_week_no
          ) as text) <> cast((
            cast('' as varchar)
          ) as text)
        )
      )
      then a.eff_str_date
      when (
        (
          b.jj_yr_week_no is null
        )
        or (
          cast((
            b.jj_yr_week_no
          ) as text) = cast((
            cast('' as varchar)
          ) as text)
        )
      )
      then d.eff_str_date
      else cast(null as date)
    end as eff_str_date,
    case
      when (
        (
          not b.jj_yr_week_no is null
        )
        or (
          cast((
            b.jj_yr_week_no
          ) as text) <> cast((
            cast('' as varchar)
          ) as text)
        )
      )
      then case
        when (
          a.eff_end_date < current_timestamp()::date
        )
        then a.eff_end_date
        else cast(null as date)
      end
      when (
        (
          b.jj_yr_week_no is null
        )
        or (
          cast((
            b.jj_yr_week_no
          ) as text) = cast((
            cast('' as varchar)
          ) as text)
        )
      )
      then case
        when (
          d.eff_end_date < current_timestamp()::date
        )
        then d.eff_end_date
        else cast(null as date)
      end
      else cast(null as date)
    end as eff_end_date
  from  c, (
    (
      itg_my_pos_sales_fact as b
        left join  a
          on (
            (
              (
                (
                  ltrim(
                    cast((
                      coalesce(a.sap_matl_num, cast('' as varchar))
                    ) as text),
                    cast((
                      cast('0' as varchar)
                    ) as text)
                  ) = ltrim(
                    cast((
                      coalesce(b.sap_matl_num, cast('' as varchar))
                    ) as text),
                    cast((
                      cast('0' as varchar)
                    ) as text)
                  )
                )
                and (
                  ltrim(cast((
                    a.mt_item_cd
                  ) as text), cast((
                    cast('0' as varchar)
                  ) as text)) = ltrim(cast((
                    b.mt_item_cd
                  ) as text), cast((
                    cast('0' as varchar)
                  ) as text))
                )
              )
              and (
                ltrim(
                  cast((
                    coalesce(a.cust_id, cast('' as varchar))
                  ) as text),
                  cast((
                    cast('0' as varchar)
                  ) as text)
                ) = ltrim(
                  cast((
                    coalesce(b.cust_id, cast('' as varchar))
                  ) as text),
                  cast((
                    cast('0' as varchar)
                  ) as text)
                )
              )
            )
          )
    )
    left join  d
      on (
        (
          (
            (
              ltrim(
                cast((
                  coalesce(d.sap_matl_num, cast('' as varchar))
                ) as text),
                cast((
                  cast('0' as varchar)
                ) as text)
              ) = ltrim(
                cast((
                  coalesce(b.sap_matl_num, cast('' as varchar))
                ) as text),
                cast((
                  cast('0' as varchar)
                ) as text)
              )
            )
            and (
              ltrim(cast((
                d.mt_item_cd
              ) as text), cast((
                cast('0' as varchar)
              ) as text)) = ltrim(cast((
                b.mt_item_cd
              ) as text), cast((
                cast('0' as varchar)
              ) as text))
            )
          )
          and (
            ltrim(
              cast((
                coalesce(d.cust_id, cast('' as varchar))
              ) as text),
              cast((
                cast('0' as varchar)
              ) as text)
            ) = ltrim(
              cast((
                coalesce(b.cust_id, cast('' as varchar))
              ) as text),
              cast((
                cast('0' as varchar)
              ) as text)
            )
          )
        )
      )
  )
  group by
    b.cust_id,
    b.mt_item_cd,
    b.sap_matl_num,
    case
      when (
        (
          not b.jj_yr_week_no is null
        )
        or (
          cast((
            b.jj_yr_week_no
          ) as text) <> cast((
            cast('' as varchar)
          ) as text)
        )
      )
      then case
        when (
          a.eff_end_date < current_timestamp()::date
        )
        then a.eff_end_date
        else cast(null as date)
      end
      when (
        (
          b.jj_yr_week_no is null
        )
        or (
          cast((
            b.jj_yr_week_no
          ) as text) = cast((
            cast('' as varchar)
          ) as text)
        )
      )
      then case
        when (
          d.eff_end_date < current_timestamp()::date
        )
        then d.eff_end_date
        else cast(null as date)
      end
      else cast(null as date)
    end,
    case
      when (
        (
          not b.jj_yr_week_no is null
        )
        or (
          cast((
            b.jj_yr_week_no
          ) as text) <> cast((
            cast('' as varchar)
          ) as text)
        )
      )
      then a.eff_str_date
      when (
        (
          b.jj_yr_week_no is null
        )
        or (
          cast((
            b.jj_yr_week_no
          ) as text) = cast((
            cast('' as varchar)
          ) as text)
        )
      )
      then d.eff_str_date
      else cast(null as date)
    end 
),
final as (
select
  derived_table1.cntry_cd,
  derived_table1.cntry_nm,
  derived_table1.jj_mnth_id,
  derived_table1.cust_id as cust_cd,
  derived_table1.item_cd,
  derived_table1.item_nm,
  derived_table1.sap_item_cd,
  derived_table1.bar_cd,
  derived_table1.cust_sku_grp,
  derived_table1.cust_conv_factor,
  derived_table1.cust_item_prc,
  derived_table1.lst_period,
  derived_table1.early_bk_period,
  derived_table1.eff_str_date,
  derived_table1.eff_end_date
from  derived_table1
)

select * from final