{{
    config(
        tags=["audits"]
    )
}}

{{compare_snapshot_static(
        primary_key=["matl_type","langu"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_material_typ',
        tgt_model=ref('aspitg_integration__itg_material_typ')
    )}}