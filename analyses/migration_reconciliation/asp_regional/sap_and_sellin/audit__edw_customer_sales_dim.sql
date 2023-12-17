{% set c_pk= "md5(concat(CUST_NUM,'_',SLS_ORG,'_',DSTR_CHNL,'_',DIV))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='DEV_DNA_CORE',
            schema='snapaspedw_integration',
            identifier='edw_customer_sales_dim'
        ),
        b_relation=ref('aspedw_integration__edw_customer_sales_dim'),
        primary_key=c_pk
    )
}}