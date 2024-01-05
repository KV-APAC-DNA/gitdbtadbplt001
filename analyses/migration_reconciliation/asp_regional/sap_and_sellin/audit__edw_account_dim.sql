{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(chrt_acct,'_',acct_num,'_',obj_ver))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspedw_integration',
            identifier='edw_account_dim'
        ),
        b_relation=ref('aspedw_integration__edw_account_dim'),
        primary_key=c_pk
    )
}}