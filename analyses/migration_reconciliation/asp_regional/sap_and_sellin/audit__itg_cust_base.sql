{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(cust_num_1))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='asing012_workspace',
            identifier='itg_cust_base'
        ),
        b_relation=ref('aspitg_integration__itg_cust_base'),
        primary_key=c_pk
    )
}}