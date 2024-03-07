{{
    config(
            materialized="incremental",
            incremental_strategy="delete+insert",
            unique_key=["matl_num"]
    )
}}


with itg_th_tims_materialmaster as (
    select * from {{ ref('thaitg_integration__itg_th_tims_materialmaster') }}
),
transformed as(
    select
  'TESCO'::varchar(10) as cust_cd,
  mm.barcd::varchar(20) as barcd,
  LTRIM(mm.matl_num, 0)::varchar(50) as matl_num,
  mm.matl_desc::varchar(500) as matl_desc,
  mm.mega_brnd::varchar(10) as item_grp_cd,
  mm.mega_brnd::varchar(10) as mega_brnd,
  mm.brnd::varchar(10) as brnd,
  mm.base_prod::varchar(20) as base_prod,
  mm.vrnt::varchar(10) as vrnt,
  mm.put_up::varchar(10) as put_up,
  mm.gross_prc::number(10,2) as gross_prc,
  mm.unit_cd::varchar(10) as unit_cd,
  mm.crt_date::timestamp_ntz(9) as crt_date,
  NULL::varchar(255) as CDL_DTTM,
  current_timestamp()::timestamp_ntz(9) as curr_date
from itg_th_tims_materialmaster as mm
)
select * from transformed