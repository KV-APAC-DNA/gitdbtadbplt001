with source as
(
    select * from {{ source( 'pcfsdl_raw','sdl_perenso_work_item') }}
),
final as 
(
select 
    work_item_key::number(10,0) as work_item_key,
    work_item_desc::varchar(255) as work_item_desc,
    work_item_type::number(10,0) as work_item_type,
    diary_item_type_key::number(10,0) as diary_item_type_key,
    to_timestamp(to_date(start_date,'dd/mm/yyyy')) as start_date, 
    to_timestamp(to_date(start_date,'dd/mm/yyyy')) as end_date,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final