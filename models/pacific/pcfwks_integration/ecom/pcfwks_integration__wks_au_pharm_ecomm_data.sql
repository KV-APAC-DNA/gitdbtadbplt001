with itg_national_ecomm_data as (
    select * from {{ ref('pcfitg_integration__itg_national_ecomm_data') }}
),
itg_chw_ecomm_data as (
    select * from {{ ref('pcfitg_integration__itg_chw_ecomm_data') }}
),
union1 as(
    select
        'National' as cust_group,
        product_probe_id,
        product_name,
        nec1_desc,
        nec2_desc,
        nec3_desc,
        brand,
        category,
        owner,
        manufacturer,
        mat_year,
        time_period,
        week_end_dt,
        sales_value,
        sales_qty,
        crncy
    from itg_national_ecomm_data
),
union2 as(
    select
        'CHW' as cust_group,
        product_probe_id,
        product_name,
        nec1_desc,
        nec2_desc,
        nec3_desc,
        brand,
        category,
        owner,
        manufacturer,
        mat_year,
        time_period,
        week_end_dt,
        sales_value,
        sales_qty,
        crncy
    from itg_chw_ecomm_data
),
union3 as(
  select
  'Other AURX' as cust_group,
  a.product_probe_id,
  a.product_name,
  a.nec1_desc,
  a.nec2_desc,
  a.nec3_desc,
  a.brand,
  a.category,
  a.owner,
  a.manufacturer,
  a.mat_year,
  a.time_period,
  a.week_end_dt,
  (
    coalesce(a.sales_value, 0) - coalesce(b.sales_value, 0)
  ) as sales_value,
  (
    coalesce(a.sales_qty, 0) - coalesce(b.sales_qty, 0)
  ) as sales_qty,
  a.crncy
from itg_national_ecomm_data as a
left join (
  select
    *
  from itg_chw_ecomm_data
  where
    not upper(category) in ('TOILET SOAP', 'DEODORANTS', 'FEMININE HYGIENE', 'COSMETICS', 'THROAT PREPARATIONS', 'EYE CARE', 'URINARY', 'PAPER PRODUCTS', 'FOOT CARE')
) as b
  on a.product_probe_id = b.product_probe_id
  and upper(a.owner) = upper(b.owner)
  and a.week_end_dt = b.week_end_dt
),
final as(
  select * from union1
  union all
  select * from union2
  union all
  select * from union3
)
select * from final

