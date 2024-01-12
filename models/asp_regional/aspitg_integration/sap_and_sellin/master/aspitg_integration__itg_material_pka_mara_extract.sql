{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table"
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_material_pka_mara_extract') }}
),

--Logical CTE

--Final CTE
final as (
    select
  material,
  franchise,
  franchise_description,
  brand,
  brand_description,
  subbrand,
  subbranddesc,
  variant,
  variantdesc,
  subvariant,
  subvariantdesc,
  flavor,
  flavordesc,
  ingredient,
  ingredientdesc,
  application,
  applicationdesc,
  strength,
  strengthdesc,
  shape,
  shapedesc,
  spf,
  spfdesc,
  cover,
  coverdesc,
  form,
  formdesc,
  size,
  sizedesc,
  character,
  charaterdesc,
  package,
  packagedesc,
  attribute13,
  attribute13desc,
  attribute14,
  attribute14desc,
  skuidentification,
  skuiddesc,
  onetimerel,
  onetimereldesc,
  productkey,
  productdesc,
  rootcode,
  rootcodedes,
current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)

--Final select
select * from final 