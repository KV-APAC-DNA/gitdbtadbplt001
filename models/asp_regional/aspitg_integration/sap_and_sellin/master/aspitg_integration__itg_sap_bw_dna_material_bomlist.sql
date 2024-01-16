with 

source as (

    select * from {{ ref('aspwks_integration__wks_sap_bw_dna_material_bomlist') }}
),

final as (
    select zbomno :: varchar(10) as zbomno,
    plant :: varchar(6) as plant,
    material :: varchar(30) as material,
    component :: varchar(30) as component,
    to_date(createdon, 'YYYYMMDD') as createdon,
    to_date(validfrom, 'YYYYMMDD') as validfrom,
    to_date(validto, 'YYYYMMDD') as validto,
    to_date(zvlfromc, 'YYYYMMDD') as zvlfromc,
    to_date(zvltoi, 'YYYYMMDD') as zvltoi,
    to_date(id_credat, 'YYYYMMDD') as id_credat,
    try_cast(zcoqty as decimal(38, 8)) as zcoqty,
    try_cast(case when comp_scrap = '' then '0.00' else comp_scrap end as decimal(20, 8)) as comp_scrap,
    base_uom :: varchar(50) as base_uom,
    recordmode :: varchar(50) as recordmode,
    try_cast(zhdqty as decimal(38, 8)) as zhdqty,
    unit :: varchar(50) as unit,
    matl_type :: varchar(50) as matl_type,
    zmat_type :: varchar(50) as zmat_type,
    salesorg :: varchar(50) as salesorg,
    zstlan :: varchar(50) as zstlan,
    zstlal :: varchar(50) as zstlal,
    try_cast(zstlst as int) as zstlst,
    zz_tp :: varchar(50) as zz_tp,
    zz_cp :: varchar(50) as zz_cp,
    zfmnge :: varchar(50) as zfmnge,
    zausch :: varchar(50) as zausch,
    zuposz :: varchar(50) as zuposz,
    zmaktx :: varchar(50) as zmaktx,
    zupmng :: varchar(50) as zupmng,
    zlosbs :: varchar(50) as zlosbs,
    zlosvn :: varchar(50) as zlosvn,
    znlfzt :: varchar(50) as znlfzt,
    zverti :: varchar(50) as zverti,
    znlfzv :: varchar(50) as znlfzv,
    znlfmv :: varchar(50) as znlfmv,
    zzausch :: varchar(50) as zzausch,
    zavoau :: varchar(50) as zavoau,
    znlfzv1 :: varchar(50) as znlfzv1,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final