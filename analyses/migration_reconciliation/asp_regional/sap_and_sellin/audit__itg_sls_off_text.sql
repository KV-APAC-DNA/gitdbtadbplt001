{% set c_pk= "md5(concat(CLNT,'_',LANG_KEY,'_',SLS_OFF))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='DEV_DNA_CORE',
            schema='snapaspitg_integration',
            identifier='itg_sls_off_text'
        ),
        b_relation=ref('aspitg_integration__itg_sls_off_text'),
        primary_key=c_pk
    )
}}