{{
    config(
        materialized = "incremental",
        incremental_strategy = "append"
    )
}}


with sdl_extarcted_tabel
as
(
    select * from dev_dna_core.SNAPJPDCLSDL_RAW.extracted_table
)

,

transformed
as
(
    select 
    from_date,
    to_date,
    'Y' as processed_flag,
    itemid,
    source_file_date,
    inserted
)