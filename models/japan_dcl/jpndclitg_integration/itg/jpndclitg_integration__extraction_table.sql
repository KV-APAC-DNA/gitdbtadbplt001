{{ 
    config(
        materialized="incremental", 
        incremental_strategy="delete+insert",
        unique_key = ['itemid'],
        pre_hook = "{% if is_incremental() %}
                    UPDATE {{this}} SET source_file_date = NULL;
                    {% endif %}"
        ) 
    }}


with
    sdl_extracted_table as (
        select * from {{source('jpdclsdl_raw','extraction_table')}}
    ),

     final as (
        select
            from_date::varchar(60) as from_date,
            to_date::varchar(60) as to_date,
            processed_flag::varchar(1) as processed_flag,
            itemid::varchar(256) as itemid,
            'Src_File_Dt'::varchar(30) as source_file_date,
            current_timestamp()::timestamp_ntz(9) as inserted_date,
            null::varchar(100) as inserted_by,
            current_timestamp()::timestamp_ntz(9) as updated_date,
            null::varchar(9) as updated_by
        from sdl_extracted_table

    )

select *
from final



    