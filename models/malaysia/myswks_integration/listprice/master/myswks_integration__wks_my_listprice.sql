{{ config(
    materialized="incremental",
    incremental_strategy="append",
) }}

with edw_list_price as (
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
edw_vw_my_material_dim as (
    select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
    ),
veomd as (
  select
    *
  from edw_vw_my_material_dim
  where
    cntry_key = 'MY'
),
relp as (
  select
    *
  from edw_list_price
  where
    sls_org = '2100'
),
transformed as (
select
  sls_org as plant,
  knart as cnty,
  ltrim(material, '0') as item_cd,
  veomd.sap_mat_desc as item_desc,
  to_date(dt_from, 'YYYYMMDD') as valid_from,
  to_date(valid_to, 'YYYYMMDD') as valid_to,
  cast(amount as decimal(16, 4)) as rate,
  currency as currency,
  cast(price_unit as decimal(16, 4)) as price_unit,
  unit as uom,
  cdl_dttm,
  dateadd(day, -1, cast(current_timestamp() as timestampntz)) as snapshot_dt,
  current_timestamp() as crtd_dttm,
  cast(null as date) as updt_dttm
from relp , veomd where
  trim(veomd.sap_matl_num(+)) = ltrim(relp.material, '0')
),
final as (
    select
    plant::varchar(4) as plant,
    cnty::varchar(4) as cnty,
    item_cd::varchar(20) as item_cd,
    item_desc::varchar(100) as item_desc,
    valid_from::varchar(10) as valid_from,
    valid_to::varchar(10) as valid_to,
    rate::number(20,4) as rate,
    currency::varchar(4) as currency,
    price_unit::number(16,4) as price_unit,
    uom::varchar(6) as uom,
    cdl_dttm::varchar(255) as cdl_dttm,
    snapshot_dt::date as snapshot_dt,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final 