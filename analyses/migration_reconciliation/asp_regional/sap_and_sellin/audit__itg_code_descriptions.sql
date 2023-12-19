{{compare_snapshot_static(
        primary_key=["source_type","code_type","code"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_code_descriptions',
        tgt_model=ref('aspitg_integration__itg_code_descriptions')
    )}}
