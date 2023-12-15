{% set c_pk= "md5(concat(cntl_area,'_',prft_ctr,'_',vld_to_dt,'_',vld_from_dt))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_prft_ctr'
        ),
        b_relation=ref('aspitg_integration__itg_prft_ctr'),
        exclude_columns=['updt_dttm','crt_dttm'], 
        primary_key=c_pk
    )
}}