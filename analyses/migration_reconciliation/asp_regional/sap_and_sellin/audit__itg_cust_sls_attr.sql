{% set c_pk= "md5(concat(DIVISION,'_',DISTR_CHAN,'_',SALESORG,'_',CUST_SALES))"%} 
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='DEV_DNA_CORE',
            schema='snapaspitg_integration',
            identifier='itg_cust_sls_attr'
        ),
        b_relation=ref('aspitg_integration__itg_cust_sls_attr'),
        primary_key=c_pk
    )
}}