with source as (
    select * from {{ ref('pcfedw_integration__vw_material_dim') }}
),
final as (
    select 
        matl_id,
        matl_desc,
        mega_brnd_cd,
        mega_brnd_desc,
        brnd_cd,
        brnd_desc,
        base_prod_cd,
        base_prod_desc,
        variant_cd,
        variant_desc,
        fran_cd,
        fran_desc,
        grp_fran_cd,
        grp_fran_desc,
        matl_type_cd,
        matl_type_desc,
        prod_fran_cd,
        prod_fran_desc,
        prod_hier_cd,
        prod_hier_desc,
        prod_mjr_cd,
        prod_mjr_desc,
        prod_mnr_cd,
        prod_mnr_desc,
        mercia_plan,
        putup_cd,
        putup_desc,
        bar_cd,
        updt_dt,
        prft_ctr
    from source
)
select * from final