with 

source as (

    select * from {{ source('bwa_access', 'bwa_material_sales') }}

),

final as (

    select
        salesorg,
        distr_chan,
        mat_sales,
        base_uom,
        matl_grp_1,
        prod_hier,
        prov_group,
        rebate_grp,
        ph_refnr,
        del_flag,
        matl_grp_2,
        matl_grp_3,
        matl_grp_4,
        matl_grp_5,
        mat_stgrp,
        rt_assgrad,
        af_vasmg,
        af_prind,
        bic_zpredecsr as zpredecsr,
        bic_zskuid as zskuid,
        bic_zprallocd as zprallocd,
        bic_zpackpcs as zpackpcs,
        bic_zean as zean,
        bic_zoldmatl as zoldmatl,
        bic_zd_plant as zd_plant,
        bic_zchdcind as zchdcind,
        bic_zprc_grp as zprc_grp,
        accnt_asgn,
        bic_zitm_cgrp as zitm_cgrp,
        bic_zmin_qty as zmin_qty,
        bic_zmin_dqty as zmin_dqty,
        bic_zdel_unit as zdel_unit,
        bic_zschme as zschme,
        bic_zvrkme as zvrkme,
        bic_zlaunchd as zlaunchd,
        bic_znpi_ind as znpi_ind,
        bic_zlmat_gr1 as zlmat_gr1,
        bic_zlmat_gr2 as zlmat_gr2,
        bic_zlmat_gr3 as zlmat_gr3,
        bic_zlmat_gr4 as zlmat_gr4,
        bic_zlmat_gr5 as zlmat_gr5,
        bic_zlmat_gr6 as zlmat_gr6,
        bic_znpi_indr as znpi_indr,
        bic_zcpy_hist as zcpy_hist,
        bic_zabc as zabc,
        bic_zfc_indc as zfc_indc,
        bic_zprdtype as zprdtype,
        bic_zparent as zparent,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source

)

select * from final
