with source as (
    select * from {{ ref('pcfedw_integration__edw_material_dim') }}
),
final as (
    select 
        matl_id as "matl_id",
        matl_desc as "matl_desc",
        mega_brnd_cd as "mega_brnd_cd",
        mega_brnd_desc as "mega_brnd_desc",
        brnd_cd as "brnd_cd",
        brnd_desc as "brnd_desc",
        base_prod_cd as "base_prod_cd",
        base_prod_desc as "base_prod_desc",
        variant_cd as "variant_cd",
        variant_desc as "variant_desc",
        fran_cd as "fran_cd",
        fran_desc as "fran_desc",
        grp_fran_cd as "grp_fran_cd",
        grp_fran_desc as "grp_fran_desc",
        matl_type_cd as "matl_type_cd",
        matl_type_desc as "matl_type_desc",
        prod_fran_cd as "prod_fran_cd",
        prod_fran_desc as "prod_fran_desc",
        prod_hier_cd as "prod_hier_cd",
        prod_hier_desc as "prod_hier_desc",
        prod_mjr_cd as "prod_mjr_cd",
        prod_mjr_desc as "prod_mjr_desc",
        prod_mnr_cd as "prod_mnr_cd",
        prod_mnr_desc as "prod_mnr_desc",
        mercia_plan as "mercia_plan",
        putup_cd as "putup_cd",
        putup_desc as "putup_desc",
        bar_cd as "bar_cd",
        updt_dt as "updt_dt",
        prft_ctr as "prft_ctr"
    from source
)
select * from final