{% set c_pk= "md5(concat(clnt,'_', ex_rt_typ,'_', from_crncy,'_' ,to_crncy,'_', vld_from))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='asing012_workspace',
            identifier='itg_crncy_exch'
        ),
        b_relation=ref('aspitg_integration__itg_crncy_exch'),
        primary_key=c_pk
    )
}}