{{ config(materialized="incremental", incremental_strategy="append") }}


with
    ocl_in_userlist_v as (
        select * from {{ source('jpdcledw_integration', 'ocl_in_userlist_v') }} --using as source as cycle is created
    ),

    transformed as (
        select file_id, filename, current_timestamp()::timestamp_ntz(9) as insertdate,

        -- CONVERT_TIMEZONE('Asia/Tokyo', current_timestamp()) AS insertdate
        from ocl_in_userlist_v
       
    ),

    final as (select 
    file_id::number(18,0) as file_id,
	filename::varchar(100) as filename,
	insertdate::timestamp_ntz(9) as insertdate 
    from 
    transformed)

select *
from final


    
