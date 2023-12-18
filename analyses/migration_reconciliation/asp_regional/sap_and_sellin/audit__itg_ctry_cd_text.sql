{% set c_pk= "md5(concat(ctry_key,'_',lang_key))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='asing012_workspace',
            identifier='itg_ctry_cd_text'
        ),
        b_relation=ref('aspitg_integration__itg_ctry_cd_text'),
        primary_key=c_pk
    )
}}