{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(matl_no,'_',alt_unt))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspedw_integration',
            identifier='edw_ecc_marm'
        ),
        b_relation=ref('aspedw_integration__edw_ecc_marm'),
        primary_key=c_pk
    )
}}