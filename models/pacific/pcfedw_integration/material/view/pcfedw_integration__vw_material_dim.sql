with
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_material_plant_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
final as
(
select distinct emd.matl_num as matl_id,
    emd.matl_desc,
    emd.mega_brnd_cd,
    emd.mega_brnd_desc,
    emd.brnd_cd,
    emd.brnd_desc,
    emd.prod_base as base_prod_cd,
    emd.base_prod_desc,
    emd.vrnt as variant_cd,
    emd.varnt_desc as variant_desc,
    emd.prodh3 as fran_cd,
    emd.prodh3_txtmd as fran_desc,
    emd.prodh2 as grp_fran_cd,
    emd.prodh2_txtmd as grp_fran_desc,
    emd.matl_type_cd,
    emd.matl_type_desc,
    emd.prodh4 as prod_fran_cd,
    emd.prodh4_txtmd as prod_fran_desc,
    emd.prod_hier_cd,
    emd.prodh6_txtmd as prod_hier_desc,
    emd.prodh5 as prod_mjr_cd,
    emd.prodh5_txtmd as prod_mjr_desc,
    emd.prodh5 as prod_mnr_cd,
    emd.prodh5_txtmd as prod_mnr_desc,
    emd.mercia_plan,
    emd.put_up as putup_cd,
    emd.put_up_desc as putup_desc,
    emd.prmry_upc_cd as bar_cd,
    null::text as prft_ctr,
    current_timestamp::timestamp_ntz(9) as updt_dt
from edw_material_dim emd,
    edw_material_plant_dim empd
where (((((emd.matl_num)::text = (empd.matl_plnt_view)::text)
    AND (((((empd.plnt)::text = '3300'::text)
    OR ((empd.plnt)::text = '3410'::text))
    OR ((empd.plnt)::text = '330A'::text))
    OR ((empd.plnt)::text = '341A'::text)))
    AND ((emd.prod_hier_cd)::text <> ''::text))
    AND ((((((emd.matl_type_cd)::text = 'FERT'::text)
    OR ((emd.matl_type_cd)::text = 'HALB'::text))
    OR ((emd.matl_type_cd)::text = 'PROM'::text))
    OR ((emd.matl_type_cd)::text = 'SAPR'::text))
    OR ((emd.matl_type_cd)::text = 'ROH'::text)))
)
select * from final