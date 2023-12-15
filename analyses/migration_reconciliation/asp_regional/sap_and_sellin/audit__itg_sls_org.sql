{% set c_pk= "md5(concat(sls_org))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_sls_org'
        ),
        b_relation=ref('aspitg_integration__itg_sls_org'),
        exclude_columns=['updt_dttm','crt_dttm'], 
        primary_key=c_pk
    )
}}