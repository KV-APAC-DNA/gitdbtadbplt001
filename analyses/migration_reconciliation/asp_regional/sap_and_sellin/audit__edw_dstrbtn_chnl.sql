{{
    config(
        tags=["audits"]
    )
}}


{{compare_snapshot_static(
        primary_key=["distr_chan","langu"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspedw_integration',
        src_table='edw_dstrbtn_chnl',
        tgt_model=ref('aspedw_integration__edw_dstrbtn_chnl')
    )}}