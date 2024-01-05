{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(sls_org,'_',dstn_chnl,'_',mat_sls,'_',lang_key))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_matl_sls_text'
        ),
        b_relation=ref('aspitg_integration__itg_matl_sls_text'),
        primary_key=c_pk
    )
}}