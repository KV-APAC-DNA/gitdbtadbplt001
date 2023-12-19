{% set c_pk= "md5(concat(co_cd))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='asing012_workspace',
            identifier='edw_company_dim'
        ),
        b_relation=ref('aspedw_integration__edw_company_dim'),
        primary_key=c_pk
    )
}}