


{{compare_snapshot_static(
        primary_key=['matl_num'],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspedw_integration',
        src_table='edw_material_dim',
        tgt_model=ref('aspedw_integration__edw_material_dim')
    )}}