{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_product_attribute_audits') }}
    where file_name not in (
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_kr_product_attribute_audits__null_test') }}
            union all
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_kr_product_attribute_audits__duplicate_test') }}
    )
),

final as
(
    select * from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
{% endif %}
)

select * from final