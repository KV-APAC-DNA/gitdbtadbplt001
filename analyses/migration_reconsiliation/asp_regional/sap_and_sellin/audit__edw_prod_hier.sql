{{compare_snapshot_static(
        primary_key=["prod_hier","langu"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspedw_integration',
        src_table='edw_prod_hier',
        tgt_model=ref('aspedw_integration__edw_prod_hier')
    )}}