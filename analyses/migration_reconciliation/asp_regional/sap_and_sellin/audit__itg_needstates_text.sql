{{compare_snapshot_static(
        primary_key=["need_states"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_needstates_text',
        tgt_model=ref('aspitg_integration__itg_needstates_text')
    )}}