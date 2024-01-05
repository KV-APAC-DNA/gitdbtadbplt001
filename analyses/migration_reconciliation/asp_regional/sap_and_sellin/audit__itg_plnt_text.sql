{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(plnt))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_plnt_text'
        ),
        b_relation=ref('aspitg_integration__itg_plnt_text'),
        primary_key=c_pk
    )
}}