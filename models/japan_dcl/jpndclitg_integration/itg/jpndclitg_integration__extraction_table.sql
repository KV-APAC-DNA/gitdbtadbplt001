
{{
    config(
        materialized = "incremental",
        incremental_strategy = "append"
    )
}}



with sdl_extracted_table
as
(
    select * from DEV_DNA_LOAD.SNAPJPDCLSDL_RAW.EXTRACTION_TABLE
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
    null as source_file_date,
    current_timestamp() as inserted_date,
    null as inserted_by,
    null as updated_date,
    null as updated_by
    from 

    sdl_extracted_table
    
)
,

final
as
(
 select 
    from_date,
    to_date,
    'Y' as processed_flag,
    itemid,
    source_file_date  ,
     inserted_date,
    inserted_by,
     updated_date,
     updated_by
 from transformed
 

)

select * from final 