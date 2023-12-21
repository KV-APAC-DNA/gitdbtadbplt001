{{
    config(
        tags=["audits"]
    )
}}

{{compare_snapshot_static(
        primary_key=["source_type","code_type","code"],
        src_database='DEV_DNA_CORE',
        src_schema='snapaspedw_integration',
        src_table='edw_code_descriptions',
        tgt_model=ref('aspedw_integration__edw_code_descriptions')
    )}}