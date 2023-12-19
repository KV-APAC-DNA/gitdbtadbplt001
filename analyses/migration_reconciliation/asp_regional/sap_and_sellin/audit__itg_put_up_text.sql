
{{compare_snapshot_static(
        primary_key=["clnt","lang_key","put_up"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_put_up_text',
        tgt_model=ref('aspitg_integration__itg_put_up_text')
    )}}