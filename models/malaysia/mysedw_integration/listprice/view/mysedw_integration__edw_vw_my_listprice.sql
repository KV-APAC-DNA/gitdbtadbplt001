with itg_my_listprice as 
(
    select * from {{ ref('mysitg_integration__itg_my_listprice') }}
),

itg_my_listprice_daily as
(
    select * from {{ ref('mysitg_integration__itg_my_listprice_daily') }}
),


a as ((
select
        max(cast((itg_my_listprice.yearmo) as text)) as max_hist
        from itg_my_listprice
      )
),
b as (
select
        'MY' as cntry_key,
        'MALAYSIA' as cntry_nm,
        plant,
        cnty,
        item_cd,
        item_desc,
        valid_from,
        valid_to,
        rate,
        currency,
        price_unit,
        uom,
        yearmo,
        mnth_type,
        snapshot_dt
      from itg_my_listprice_daily 
),

temp_c as (
    select b.* from b,a
    where
        ((
            cast((b.yearmo) as text) > coalesce(a.max_hist, cast('' as text))
          )
    and (
            cast((b.mnth_type) as text) = cast('CAL' as text)
        )
        )
),

temp_d as (
select b.* from b,a 
where
        (
          (
            cast((
              b.yearmo
            ) as text) > coalesce(a.max_hist, cast('' as text))
          )
          and (
            cast((
              b.mnth_type
            ) as text) = cast('JJ' as text)
          )
        )
),

temp_cal as (
select
      'MY' as cntry_key,
      'MALAYSIA' as cntry_nm,
      itg_my_listprice.plant,
      itg_my_listprice.cnty,
      itg_my_listprice.item_cd,
      itg_my_listprice.item_desc,
      itg_my_listprice.valid_from,
      itg_my_listprice.valid_to,
      itg_my_listprice.rate,
      itg_my_listprice.currency,
      itg_my_listprice.field9 as price_unit,
      itg_my_listprice.uom,
      itg_my_listprice.yearmo,
      'CAL' as mnth_type,
      null as snapshot_dt
    from itg_my_listprice
),

temp_jj as (
select
    'MY' as cntry_key,
    'MALAYSIA' as cntry_nm,
    itg_my_listprice.plant,
    itg_my_listprice.cnty,
    itg_my_listprice.item_cd,
    itg_my_listprice.item_desc,
    itg_my_listprice.valid_from,
    itg_my_listprice.valid_to,
    itg_my_listprice.rate,
    itg_my_listprice.currency,
    itg_my_listprice.field9 as price_unit,
    itg_my_listprice.uom,
    itg_my_listprice.yearmo,
    'JJ' as mnth_type,
    null as snapshot_dt
  from itg_my_listprice
),

final as 
(
    select * from temp_c
    union all
    select * from temp_d
    union all
    select * from temp_cal
    union all
    select * from temp_jj
)

select * from final