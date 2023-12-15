{% set c_pk= "md5(concat(mandt,'_',matnr,'_',bwkey,'_',bwtar))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_ecc_standard_cost'
        ),
        b_relation=ref('aspitg_integration__itg_ecc_standard_cost'),
        exclude_columns=['updt_dttm','crt_dttm'], 
        primary_key=c_pk
    )
}}