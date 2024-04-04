with sdl_perenso_todo as (
 select * from {{ source('pcfsdl_raw', 'sdl_perenso_todo') }}
),

final as (
select 
        todo_key::number(10,0) as todo_key,
        todo_type::number(10,0) as todo_type,
        todo_desc::varchar(255) as todo_desc,
        work_item_key::number(10,0) as work_item_key,
        to_timestamp(start_date, 'DD/MM/YYYY')::timestamp_ntz(9) as start_date,
        to_timestamp(end_date, 'DD/MM/YYYY')::timestamp_ntz(9) as end_date,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        dsp_order::number(10,0) as dsp_order,
        ans_type::number(10,0) as ans_type,
        cascadeon_answermode::number(10,0) as cascadeon_answermode,
        cascade_todo_key::number(10,0) as cascade_todo_key,
        cascade_next_todo_key::number(10,0) as cascade_next_todo_key
from sdl_perenso_todo
)
select * from final