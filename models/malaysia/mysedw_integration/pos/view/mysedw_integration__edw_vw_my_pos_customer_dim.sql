with source as(
    select * from {{ ref('mysitg_integration__itg_my_pos_cust_mstr') }}
),

final as (
select
    'MY' as cntry_cd,
    'MALAYSIA' as cntry_nm,
    cust_id as cust_cd,
    cust_nm,
    cust_id as sold_to,
    store_cd as brnch_cd,
    store_nm as brnch_nm,
    store_frmt as brnch_frmt,
    store_type as brnch_typ,
    dept_cd,
    dept_nm,
    null as address1,
    null as address2,
    null as region_cd,
    region as region_nm,
    null as prov_cd,
    null as prov_nm,
    null as city_cd,
    null as city_nm,
    null as mncplty_cd,
    null as mncplty_nm
  from source
  )

  select * from final