{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        pre_hook="
            {% if is_incremental() %}
                delete from {{ this }} where employee_source_id in (
                select employee_source_id from {{ source('hcposesdl_raw', 'sdl_hcp_osea_user') }});
            {% endif %}    
        ",
        post_hook="
            update {{ this }} set manager_name = stg_us.employee_name,manager_wwid = stg_us.wwid
            from (select employee_source_id,upper(country_code) country_code,employee_name,wwid from {{ source('hcposesdl_raw', 'sdl_hcp_osea_user') }}) stg_us
            where manager_source_id = stg_us.employee_source_id
            and {{ this }}.country_code = stg_us.country_code; 
        "
    )
}}

with sdl_hcp_osea_user as (
    select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_user') }}
),

sdl_hcp_osea_user_rg as (
    select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_user_rg') }}
),


insert1 as (
SELECT MD5(UPPER(COUNTRY_CODE) || EMPLOYEE_SOURCE_ID) AS EMPLOYEE_KEY,
  EMPLOYEE_SOURCE_ID,
  USER_NAME,
  EMPLOYEE_NAME,
  COMPANY_NAME,
  DIVISION,
  DEPARTMENT,
  TITLE,
  COUNTRY,
  ADDRESS,
  EMAIL,
  PHONE,
  MOBILE_PHONE,
  ALIAS,
  NICKNAME,
  (
    CASE 
      WHEN UPPER(IS_ACTIVE) = 'TRUE'
        THEN 1
      WHEN UPPER(IS_ACTIVE) = 'FALSE'
        THEN 0
      ELSE 0
      END
    ) AS IS_ACTIVE,
  TIMEZONESIDKEY,
  USER_ROLE_SOURCE_ID,
  (
    CASE 
      WHEN UPPER(RECEIVES_INFO_EMAILS) = 'TRUE'
        THEN 1
      WHEN UPPER(RECEIVES_INFO_EMAILS) = 'FALSE'
        THEN 0
      ELSE 0
      END
    ) AS RECEIVES_INFO_EMAILS,
  EMPLOYEE_PROFILE_ID,
  LOCAL_EMPLOYEE_NUMBER,
  MANAGER_SOURCE_ID,
  LAST_LOGIN_DATE,
  CREATED_DATE,
  CREATED_BY_ID,
  LAST_MODIFIED_DATE,
  LAST_MODIFIED_BY_ID,
  FEDERATION_IDENTIFIER,
  LAST_IPAD_SYNC,
  UPPER(COUNTRY_CODE) AS COUNTRY_CODE,
  WWID,
  REGION,
  profile_group_ap,
  USER_LICENSE,
  NULL as MANAGER_NAME,
  NULL as MANAGER_WWID,
  current_timestamp() AS INSERTED_DATE,
  NULL AS UPDATED_DATE,
  MSL_PRIMARY_RESPONSIBLE_TA,
  MSL_SECONDARY_RESPONSIBLE_TA,
  last_name,
  first_name,
  city,
  STATE,
  postal_code,
  user_type,
  language_local_key,
  last_mobile_sync,
  veeva_user_type,
  veeva_country_code,
  shc_user_franchise
FROM sdl_hcp_OSEA_USER
WHERE COUNTRY_CODE IS NOT NULL
  OR COUNTRY_CODE != ''
  OR COUNTRY_CODE != ' '
  ),

insert2 as (
  SELECT MD5(UPPER(COUNTRY) || EMPLOYEE_SOURCE_ID) AS EMPLOYEE_KEY,
  EMPLOYEE_SOURCE_ID,
  USER_NAME,
  EMPLOYEE_NAME,
  COMPANY_NAME,
  DIVISION,
  DEPARTMENT,
  TITLE,
  UPPER(COUNTRY) AS COUNTRY,
  ADDRESS,
  EMAIL,
  PHONE,
  MOBILE_PHONE,
  ALIAS,
  NICKNAME,
  (
    CASE 
      WHEN UPPER(IS_ACTIVE) = 'TRUE'
        THEN 1
      WHEN UPPER(IS_ACTIVE) = 'FALSE'
        THEN 0
      ELSE 0
      END
    ) AS IS_ACTIVE,
  TIMEZONESIDKEY,
  USER_ROLE_SOURCE_ID,
  (
    CASE 
      WHEN UPPER(RECEIVES_INFO_EMAILS) = 'TRUE'
        THEN 1
      WHEN UPPER(RECEIVES_INFO_EMAILS) = 'FALSE'
        THEN 0
      ELSE 0
      END
    ) AS RECEIVES_INFO_EMAILS,
  EMPLOYEE_PROFILE_ID,
  LOCAL_EMPLOYEE_NUMBER,
  MANAGER_SOURCE_ID,
  LAST_LOGIN_DATE,
  CREATED_DATE,
  CREATED_BY_ID,
  LAST_MODIFIED_DATE,
  LAST_MODIFIED_BY_ID,
  FEDERATION_IDENTIFIER,
  NULL AS LAST_IPAD_SYNC,
  UPPER(COUNTRY) AS COUNTRY_CODE,
  NULL AS WWID,
  NULL AS REGION,
  NULL AS profile_group_ap,
  'Not applicable for Regional Service Cloud' AS USER_LICENSE,
  NULL as MANAGER_NAME,
  NULL as MANAGER_WWID,
  current_timestamp() AS INSERTED_DATE,
  NULL AS UPDATED_DATE,
  NULL AS MSL_PRIMARY_RESPONSIBLE_TA,
  NULL AS MSL_SECONDARY_RESPONSIBLE_TA,
  null as last_name,
  null as first_name,
  null as city,
  null as STATE,
  null as postal_code,
  null as user_type,
  null as language_local_key,
  null as last_mobile_sync,
  null as veeva_user_type,
  null as veeva_country_code,
  null as shc_user_franchise
FROM sdl_hcp_OSEA_USER_RG
WHERE COUNTRY IS NOT NULL
  OR COUNTRY != ''
  OR COUNTRY != ' '
  ),

result as (
    select * from insert1
    union all
    select * from insert2
),

final as (
    select 
        employee_key::varchar(32) as employee_key,
        employee_source_id::varchar(18) as employee_source_id,
        user_name::varchar(80) as user_name,
        employee_name::varchar(121) as employee_name,
        company_name::varchar(80) as company_name,
        division::varchar(80) as division,
        department::varchar(80) as department,
        title::varchar(80) as title,
        country::varchar(80) as country,
        address::varchar(255) as address,
        email::varchar(128) as email,
        phone::varchar(40) as phone,
        mobile_phone::varchar(40) as mobile_phone,
        alias::varchar(8) as alias,
        nickname::varchar(40) as nickname,
        is_active::number(38,0) as is_active,
        timezonesidkey::varchar(40) as timezonesidkey,
        user_role_source_id::varchar(18) as user_role_source_id,
        receives_info_emails::number(38,0) as receives_info_emails,
        employee_profile_id::varchar(18) as employee_profile_id,
        local_employee_number::varchar(20) as local_employee_number,
        manager_source_id::varchar(18) as manager_source_id,
        last_login_date::timestamp_ntz(9) as last_login_date,
        created_date::timestamp_ntz(9) as created_date,
        created_by_id::varchar(18) as created_by_id,
        last_modified_date::timestamp_ntz(9) as last_modified_date,
        last_modified_by_id::varchar(18) as last_modified_by_id,
        federation_identifier::varchar(512) as federation_identifier,
        last_ipad_sync::timestamp_ntz(9) as last_ipad_sync,
        country_code::varchar(8) as country_code,
        wwid::varchar(9) as wwid,
        region::varchar(80) as region,
        profile_group_ap::varchar(1300) as profile_group_ap,
        user_license::varchar(1300) as user_license,
        manager_name::varchar(80) as manager_name,
        manager_wwid::varchar(9) as manager_wwid,
        inserted_date::timestamp_ntz(9) as inserted_date,
        updated_date::timestamp_ntz(9) as updated_date,
        msl_primary_responsible_ta::varchar(255) as msl_primary_responsible_ta,
        msl_secondary_responsible_ta::varchar(255) as msl_secondary_responsible_ta,
        last_name::varchar(80) as last_name,
        first_name::varchar(40) as first_name,
        city::varchar(40) as city,
        state::varchar(80) as state,
        postal_code::varchar(20) as postal_code,
        user_type::varchar(40) as user_type,
        language_local_key::varchar(40) as language_local_key,
        last_mobile_sync::timestamp_ntz(9) as last_mobile_sync,
        veeva_user_type::varchar(255) as veeva_user_type,
        veeva_country_code::varchar(255) as veeva_country_code,
        shc_user_franchise::varchar(255) as shc_user_franchise
    from result
)

select * from final

