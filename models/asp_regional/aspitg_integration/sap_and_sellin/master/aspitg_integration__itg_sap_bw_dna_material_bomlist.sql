{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="table",
        transient=false
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_sap_bw_dna_material_bomlist') }}
),

final as (
    select zbomno,
    plant,
    material,
    component,
    to_date(createdon, 'YYYYMMDD') as createdon,
    to_date(validfrom, 'YYYYMMDD') as validfrom,
    to_date(validto, 'YYYYMMDD') as validto,
    to_date(zvlfromc, 'YYYYMMDD') as zvlfromc,
    to_date(zvltoi, 'YYYYMMDD') as zvltoi,
    to_date(id_credat, 'YYYYMMDD') as id_credat,
    try_cast(zcoqty as decimal(38, 8)) as zcoqty,
    try_cast(case when comp_scrap = '' then '0.00' else comp_scrap end as decimal(20, 8)) as comp_scrap,
    base_uom,
    recordmode,
    try_cast(zhdqty as decimal(38, 8)) as zhdqty,
    unit,
    matl_type,
    zmat_type,
    salesorg,
    zstlan,
    zstlal,
    try_cast(zstlst as int) as zstlst,
    zz_tp,
    zz_cp,
    zfmnge,
    zausch,
    zuposz,
    zmaktx,
    zupmng,
    zlosbs,
    zlosvn,
    znlfzt,
    zverti,
    znlfzv,
    znlfmv,
    zzausch,
    zavoau,
    znlfzv1,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final