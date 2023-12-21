{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(CUST_NO,'_',SLS_ORG,'_',DSTR_CHNL,'_',DIV))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='DEV_DNA_CORE',
            schema='snapaspitg_integration',
            identifier='itg_cust_sls'
        ),
        b_relation=ref('aspitg_integration__itg_cust_sls'),
        primary_key=c_pk
    )
}}