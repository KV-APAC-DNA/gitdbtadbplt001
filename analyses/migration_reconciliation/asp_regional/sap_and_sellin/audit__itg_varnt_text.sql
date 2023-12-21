{{
    config(
        tags=["audits"]
    )
}}


{{compare_snapshot_static(
        primary_key=["clnt","lang_key","varnt"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspitg_integration',
        src_table='itg_varnt_text',
        tgt_model=ref('aspitg_integration__itg_varnt_text')
    )}}