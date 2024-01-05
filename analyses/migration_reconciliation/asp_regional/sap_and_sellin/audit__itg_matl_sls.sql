{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(sls_org,'_',dstr_chnl,'_',matl))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_matl_sls'
        ),
        b_relation=ref('aspitg_integration__itg_matl_sls'),
        primary_key=c_pk
    )
}}