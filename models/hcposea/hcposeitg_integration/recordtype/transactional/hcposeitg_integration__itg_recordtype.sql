{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
            {% if is_incremental() %}
                DELETE FROM {{ this }} WHERE record_type_source_id IN (
                SELECT record_type_source_id FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_recordtype') }}) 
            {% endif %}
            "
    )
}}

with sdl_hcp_osea_recordtype
as (
    select *
    from {{ source('hcposesdl_raw', 'sdl_hcp_osea_recordtype') }}
    ),
sdl_hcp_osea_recordtype_rg
as (
    select *
    from {{ source('hcposesdl_raw', 'sdl_hcp_osea_recordtype_rg') }}
    ),
cte1
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        IS_PERSON_TYPE,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'PH' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE
    ),
cte2
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        IS_PERSON_TYPE,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'SG' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE
    ),
cte3
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        IS_PERSON_TYPE,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'ID' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE
    ),
cte4
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        IS_PERSON_TYPE,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'MY' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE
    ),
cte5
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        IS_PERSON_TYPE,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'TH' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE
    ),
cte6
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        IS_PERSON_TYPE,
        CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'VN' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE
    ),
insert1
AS (
    SELECT *
    FROM cte1
    
    UNION ALL
    
    SELECT *
    FROM cte2
    
    UNION ALL
    
    SELECT *
    FROM cte3
    
    UNION ALL
    
    SELECT *
    FROM cte4
    
    UNION ALL
    
    SELECT *
    FROM cte5
    
    UNION ALL
    
    SELECT *
    FROM cte6
    ),
cte7
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        NULL,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'PH' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE_RG
    ),
cte8
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        NULL,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'SG' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE_RG
    ),
cte9
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        NULL,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'ID' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE_RG
    ),
cte10
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        NULL,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'MY' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE_RG
    ),
cte11
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        NULL,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'TH' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE_RG
    ),
cte12
AS (
    SELECT RECORD_TYPE_SOURCE_ID,
        CASE 
            WHEN UPPER(SUBSTRING(RECORD_TYPE_NAME, LENGTH(RECORD_TYPE_NAME) - 3, 4)) = '_VOD'
                THEN SUBSTRING(RECORD_TYPE_NAME, 1, (LENGTH(RECORD_TYPE_NAME) - 4))
            ELSE RECORD_TYPE_NAME
            END RECORD_TYPE_NAME,
        DEVELOPER_NAME,
        NAME_SPACE_PREFIX,
        DESCRIPTION,
        BUSINESS_PROCESS_ID,
        SOBJECTTYPE,
        (
            CASE 
                WHEN UPPER(IS_ACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_ACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_ACTIVE,
        NULL,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        CREATED_DATE,
        LAST_MODIFIED_BY_ID,
        LAST_MODIFIED_DATE,
        'VN' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_RECORDTYPE_RG
    ),
insert2
AS (
    SELECT *
    FROM cte7
    
    UNION ALL
    
    SELECT *
    FROM cte8
    
    UNION ALL
    
    SELECT *
    FROM cte9
    
    UNION ALL
    
    SELECT *
    FROM cte10
    
    UNION ALL
    
    SELECT *
    FROM cte11
    
    UNION ALL
    
    SELECT *
    FROM cte12
    ),
result
AS (
    SELECT *
    FROM insert1
    
    UNION ALL
    
    SELECT *
    FROM insert2
    ),

final as (
    select 
        record_type_source_id::varchar(18) as record_type_source_id,
        record_type_name::varchar(255) as record_type_name,
        developer_name::varchar(80) as developer_name,
        name_space_prefix::varchar(15) as name_space_prefix,
        description::varchar(255) as description,
        business_process_id::varchar(18) as business_process_id,
        substring(sobjecttype,1,40)::varchar(40) as sobjecttype,
        is_active::varchar(5) as is_active,
        lower(is_person_type)::varchar(5) as is_person_type,
        created_by_id::varchar(30) as created_by_id,
        created_date::timestamp_ntz(9) as created_date,
        last_modified_by_id::varchar(18) as last_modified_by_id,
        last_modified_date::timestamp_ntz(9) as last_modified_date,
        country_code::varchar(255) as country_code,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        current_timestamp()::timestamp_ntz(9) as updated_date
    from result
)

select * from final
