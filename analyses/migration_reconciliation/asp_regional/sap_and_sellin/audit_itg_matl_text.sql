{{
    config(
        tags=["audits"]
    )
}}

{{compare_snapshot_static(
        primary_key=["matl_no"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_matl_text',
        tgt_model=ref('aspitg_integration__itg_matl_text')
    )}}