with

    source as (select * from {{ source("arsadpprd001", "itg_cbd_gt_customer") }}),

    renamed as (

        select
            *
        from source

    )

select *
from renamed
