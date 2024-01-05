{{
    config(
        tags=["audits"]
    )
}}

{% set c_pk= "md5(concat(bom,'_',plant,'_',matl_num,'_',component,'_',createdon,'_',validfrom,'_',validto,'_',validfrom_zvlfromc,'_',validto_zvltoi))"%}
{{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database='dev_dna_core',
            schema='snapaspedw_integration',
            identifier='edw_sap_bw_dna_material_bomlist'
        ),
        b_relation=ref('aspedw_integration__edw_sap_bw_dna_material_bomlist'),
        primary_key=c_pk
    )
}}