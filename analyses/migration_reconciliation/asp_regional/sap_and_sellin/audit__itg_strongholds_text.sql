{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(STRONGHOLDS,'_',LANGUAGE_KEY))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='DEV_DNA_CORE',
            schema='snapaspitg_integration',
            identifier='itg_strongholds_text'
        ),
        b_relation=ref('aspitg_integration__itg_strongholds_text'),
        primary_key=c_pk
    )
}}