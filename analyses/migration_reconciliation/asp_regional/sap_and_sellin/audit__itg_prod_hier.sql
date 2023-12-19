{{compare_snapshot_static(
        primary_key=["prod_hier","langu"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_prod_hier',
        tgt_model=ref('aspitg_integration__itg_prod_hier')
    )}}