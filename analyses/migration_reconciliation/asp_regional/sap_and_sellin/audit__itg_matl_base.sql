{{compare_snapshot_static(
        primary_key=["matl_num"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_matl_base',
        tgt_model=ref('aspitg_integration__itg_matl_base')
    )}}