{{
    config(
        tags=["audits"]
    )
}}



{% set c_pk= "md5(concat(matl_num))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='DEV_DNA_CORE',
            schema='snapaspitg_integration',
            identifier='itg_material_base'
        ),
        b_relation=ref('aspitg_integration__itg_material_base'),
        primary_key=c_pk
    )
}}


