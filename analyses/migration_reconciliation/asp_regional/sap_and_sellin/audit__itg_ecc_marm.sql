{% set c_pk= "md5(concat(matl_no,'_',alt_unt))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_ecc_marm'
        ),
        b_relation=ref('aspitg_integration__itg_ecc_marm'),
        exclude_columns=['updt_dttm','crt_dttm'], 
        primary_key=c_pk
    )
}}