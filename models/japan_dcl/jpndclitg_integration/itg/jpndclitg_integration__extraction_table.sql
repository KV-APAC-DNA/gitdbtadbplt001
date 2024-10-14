{{ 
    config(
        materialized="incremental", 
        incremental_strategy="append",
        unique_key = ['itemid'],
        pre_hook = "{% if is_incremental() %}
                    UPDATE {{this}} SET source_file_date = NULL;
                    {% endif %}"
        ) 
    }}


with
    sdl_extracted_table as (
        select *, dense_rank() over(partition by itemid order by file_name desc) as rnk
        from {{source('jpdclsdl_raw','extraction_table')}}
        --qualify rnk =1
    ),

     final as (
        select
            from_date::varchar(60) as from_date,
            to_date::varchar(60) as to_date,
            processed_flag::varchar(1) as processed_flag,
            itemid::varchar(256) as itemid,
            source_file_date::varchar(30) as source_file_date,
            current_timestamp()::timestamp_ntz(9) as inserted_date,
            null::varchar(100) as inserted_by,
            current_timestamp()::timestamp_ntz(9) as updated_date,
            null::varchar(9) as updated_by,
            file_name::varchar(255) as file_name
        from sdl_extracted_table

    )

select *
from final



    