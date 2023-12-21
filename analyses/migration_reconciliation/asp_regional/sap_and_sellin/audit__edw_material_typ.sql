{{
    config(
        tags=["audits"]
    )
}}

{{compare_snapshot_static(
        primary_key=["matl_type","langu"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspedw_integration',
        src_table='edw_material_typ',
        tgt_model=ref('aspedw_integration__edw_material_typ')
    )}}