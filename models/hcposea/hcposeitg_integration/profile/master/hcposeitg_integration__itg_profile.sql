{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
            {% if is_incremental() %}
                DELETE FROM {{ this }} 
                WHERE profile_source_id  IN(SELECT profile_source_id FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_profile') }}); 
            {% endif %}
            "
    )
}}

with sdl_hcp_osea_profile
as (
    select *
    from {{ source('hcposesdl_raw', 'sdl_hcp_osea_profile') }}
    ),
sdl_hcp_osea_profile_rg
as (
    select *
    from {{ source('hcposesdl_raw', 'sdl_hcp_osea_profile_rg') }}
    ),
cte1
AS (
    SELECT MD5('PH' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        TYPE,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'PH' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE
    ),
cte2
AS (
    SELECT MD5('SG' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        TYPE,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'SG' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE
    ),
cte3
AS (
    SELECT MD5('ID' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        TYPE,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'ID' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE
    ),
cte4
AS (
    SELECT MD5('MY' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        TYPE,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'MY' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE
    ),
cte5
AS (
    SELECT MD5('TH' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        TYPE,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'TH' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE
    ),
cte6
AS (
    SELECT MD5('VN' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        TYPE,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'VN' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE
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
    SELECT MD5('PH' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        NULL,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'PH' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE_RG
    ),
cte8
AS (
    SELECT MD5('SG' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        NULL,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'SG' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE_RG
    ),
cte9
AS (
    SELECT MD5('ID' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        NULL,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'ID' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE_RG
    ),
cte10
AS (
    SELECT MD5('MY' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        NULL,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'MY' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE_RG
    ),
cte11
AS (
    SELECT MD5('TH' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        NULL,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'TH' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE_RG
    ),
cte12
AS (
    SELECT MD5('VN' || PROFILE_SOURCE_ID) AS PROFILE_KEY,
        PROFILE_SOURCE_ID,
        PROFILE_NAME,
        NULL,
        USERLICENSE_SOURCE_ID,
        USERTYPE,
        CREATED_DATE,
        CREATED_BY_ID || '_RG' AS CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        DESCRIPTION,
        'VN' as country_code,
        current_timestamp(),
        current_timestamp()
    FROM sdl_hcp_OSEA_PROFILE_RG
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
        profile_key::varchar(32) as profile_key,
        profile_source_id::varchar(18) as profile_source_id,
        profile_name::varchar(255) as profile_name,
        type::varchar(40) as type,
        userlicense_source_id::varchar(18) as userlicense_source_id,
        usertype::varchar(40) as usertype,
        created_date::timestamp_ntz(9) as created_date,
        created_by_id::varchar(30) as created_by_id,
        last_modified_date::timestamp_ntz(9) as last_modified_date,
        last_modified_by_id::varchar(18) as last_modified_by_id,
        description::varchar(255) as description,
        country_code::varchar(255) as country_code,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        current_timestamp()::timestamp_ntz(9) as updated_date
    from result
)

select * from final
