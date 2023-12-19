{{compare_snapshot_static(
        primary_key=["clnt","lang_key","base_prod"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_base_prod_text',
        tgt_model=ref('aspitg_integration__itg_base_prod_text')
    )}}