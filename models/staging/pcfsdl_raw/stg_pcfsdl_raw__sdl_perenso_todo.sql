with 

source as (

    select * from {{ source('pcfsdl_raw', 'sdl_perenso_todo') }}

),

renamed as (

    select
        todo_key,
        todo_type,
        todo_desc,
        work_item_key,
        start_date,
        end_date,
        run_id,
        create_dt,
        dsp_order,
        ans_type,
        cascadeon_answermode,
        cascade_todo_key,
        cascade_next_todo_key

    from source

)

select * from renamed
