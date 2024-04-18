with edw_code_descriptions as (
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),

final as (
    select
        source_type as "source_type",
        code_type as "code_type",
        code as "code",
        code_desc as "code_desc",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm"

    from edw_code_descriptions
)

select * from final
