{{compare_snapshot_static(
        primary_key=["clnt","lang_key","mega_brnd"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_mega_brnd_text',
        tgt_model=ref('aspitg_integration__itg_mega_brnd_text')
    )}}