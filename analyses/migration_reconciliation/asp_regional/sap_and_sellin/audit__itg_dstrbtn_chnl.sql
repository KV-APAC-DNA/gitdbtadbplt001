{{compare_snapshot_static(
        primary_key=["distr_chan","langu"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_dstrbtn_chnl',
        tgt_model=ref('aspitg_integration__itg_dstrbtn_chnl')
    )}}