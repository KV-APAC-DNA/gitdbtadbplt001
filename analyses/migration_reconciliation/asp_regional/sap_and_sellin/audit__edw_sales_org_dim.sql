{% set c_pk= "md5(concat(sls_org))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspedw_integration',
            identifier='edw_sales_org_dim'
        ),
        b_relation=ref('aspedw_integration__edw_sales_org_dim'),
        exclude_columns=['updt_dttm','crt_dttm'], 
        primary_key=c_pk
    )
}}