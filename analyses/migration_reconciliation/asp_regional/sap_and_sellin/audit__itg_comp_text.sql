{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(co_cd))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='asing012_workspace',
            identifier='itg_comp_text'
        ),
        b_relation=ref('aspitg_integration__itg_comp_text'),
        primary_key=c_pk
    )
}}