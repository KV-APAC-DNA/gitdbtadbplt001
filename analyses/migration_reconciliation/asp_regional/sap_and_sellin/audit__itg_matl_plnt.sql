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
            schema='snapaspitg_integration',
            identifier='itg_matl_plnt'
        ),
        b_relation=ref('aspitg_integration__itg_matl_plnt'),
        primary_key=c_pk
    )
}}