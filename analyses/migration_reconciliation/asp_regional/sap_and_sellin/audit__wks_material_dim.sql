

{% set c_pk= "md5(concat(matl_num))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='DEV_DNA_CORE',
            schema='snapaspwks_integration',
            identifier='wks_material_dim'
        ),
        b_relation=ref('aspwks_integration__wks_material_dim'),
        primary_key=c_pk
    )
}}


