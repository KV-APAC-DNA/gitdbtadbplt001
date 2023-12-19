{% set c_pk= "md5(concat(mandt,'_',matnr,'_',bwkey,'_',bwtar))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspedw_integration',
            identifier='edw_ecc_standard_cost'
        ),
        b_relation=ref('aspedw_integration__edw_ecc_standard_cost'),
        primary_key=c_pk
    )
}}