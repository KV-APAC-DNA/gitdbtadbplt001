{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(plnt,'_',matl_plnt_view))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspedw_integration',
            identifier='edw_material_plant_dim'
        ),
        b_relation=ref('aspedw_integration__edw_material_plant_dim'),
        primary_key=c_pk
    )
}}