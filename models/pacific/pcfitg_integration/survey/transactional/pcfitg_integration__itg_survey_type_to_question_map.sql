with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_survey_type_to_question_map') }}
),
final as
(
select 
    account_type_description::varchar(256) as account_type_description,
    target_type::varchar(256) as target_type,
    perenso_questions::varchar(256) as perenso_questions,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final