{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(chrt_acct,'_',acct_num,'_',lang_key))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_acct_text'
        ),
        b_relation=ref('aspitg_integration__itg_acct_text'),
        primary_key=c_pk
    )
}}