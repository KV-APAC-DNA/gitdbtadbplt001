{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(zbomno,'_',plant,'_',material,'_',component,'_',createdon,'_',validfrom,'_',validto,'_',zvlfromc,'_',zvltoi))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspitg_integration',
            identifier='itg_sap_bw_dna_material_bomlist'
        ),
        b_relation=ref('aspitg_integration__itg_sap_bw_dna_material_bomlist'),
        primary_key=c_pk
    )
}}