{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(sls_org,'_',dstr_chnl,'_',matl_num))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspedw_integration',
            identifier='edw_material_sales_dim'
        ),
        b_relation=ref('aspedw_integration__edw_material_sales_dim'),
        primary_key=c_pk
    )
}}