with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_survey_product_grp_to_category_map') }}
),
final as
(
select 
    category::varchar(256) as category,
    question_product_group::varchar(256) as question_product_group,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final