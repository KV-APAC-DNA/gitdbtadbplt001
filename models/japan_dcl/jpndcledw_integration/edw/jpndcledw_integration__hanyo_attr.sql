
{{
    config
    (
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE
                    FROM {{this}} HANYO_ATTR using {{ ref('jpndclitg_integration__hanyo_attr_temp') }} HANYO_WORK_TEMP
                    WHERE HANYO_ATTR.KBNMEI = HANYO_WORK_TEMP.KBNMEI
                        AND HANYO_ATTR.ATTR1 = HANYO_WORK_TEMP.ATTR1;
                    {% endif %}
                    ",
        post_hook = "{{ macro_update_hanyo_attrandtm06dmout() }}"
    )
}}

with source as (
    select * from  {{ ref('jpndclitg_integration__hanyo_attr_temp') }}
),
final as (
    select 
        KBNMEI::VARCHAR(45)  as KBNMEI,
        ATTR1::VARCHAR(60) as ATTR1,
        ATTR2::VARCHAR(160) as ATTR2,
        ATTR3::VARCHAR(60) as ATTR3,
        ATTR4::VARCHAR(60) as ATTR4,
        ATTR5::VARCHAR(60) as ATTR5,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as INSERTDATE,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9)  as UPDATEDATE,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as INSERTED_DATE,
        'ETL_Batch'::VARCHAR(100) as inserted_by,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as UPDATED_DATE,
        NULL::VARCHAR(100) as UPDATED_BY,
        NULL::VARCHAR(10) as SOURCE_FILE_DATE
    from source
)
select * from final