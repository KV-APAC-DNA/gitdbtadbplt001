{{ config(
  sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
) }}
with itg_hcp360_veeva_account_territory_loader as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_account_territory_loader') }}
),
wks_hcp360_in_veeva_territory_loader as
(
    select * from {{ ref('hcpwks_integration__wks_hcp360_in_veeva_territory_loader') }}
),
itg_hcp360_veeva_territory as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_territory') }}
),
final as
(
    SELECT TL.TL_ID as TERRITORY_LOADER_ID,
    T.TERRITORY_SOURCE_ID,
    TL.TL_ACCOUNT_SOURCE_ID,
    TL.CREATED_BY_ID,
    TL.CREATED_DATE,
    TL.TL_EXTERNAL_ID,
    TL.IS_DELETED,
    TL.IS_LOCKED,
    TL.SFE_APPROVED,
    TL.LAST_MODIFIED_BY_ID,
    TL.LAST_MODIFIED_DATE,
    TL.LAST_REFERENCED_DATE,
    TL.LAST_VIEWED_DATE,
    TL.MAY_EDIT,
    TL.MOBILE_ID,
    TL.NAME,
    TL.OWNER_ID,
    TL.SYSTEM_MODSTAMP,
    TL.TERRITORY_TO_ADD,
    TL.TERRITORY_TO_DROP,
    WTL.LEVEL_1 AS TERRITORY,
    TL.COUNTRY_CODE,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm
    FROM itg_hcp360_veeva_account_territory_loader TL,
    wks_hcp360_in_veeva_territory_loader WTL,
    itg_hcp360_veeva_territory T
    WHERE TL.TL_ID = WTL.TL_ID 
    AND  trim(T.TERRITORY_NAME) = trim(WTL.LEVEL_1(+))


    UNION 


    SELECT TL.TL_ID as TERRITORY_LOADER_ID,
    T.TERRITORY_SOURCE_ID,
    TL.TL_ACCOUNT_SOURCE_ID,
    TL.CREATED_BY_ID,
    TL.CREATED_DATE,
    TL.TL_EXTERNAL_ID,
    TL.IS_DELETED,
    TL.IS_LOCKED,
    TL.SFE_APPROVED,
    TL.LAST_MODIFIED_BY_ID,
    TL.LAST_MODIFIED_DATE,
    TL.LAST_REFERENCED_DATE,
    TL.LAST_VIEWED_DATE,
    TL.MAY_EDIT,
    TL.MOBILE_ID,
    TL.NAME,
    TL.OWNER_ID,
    TL.SYSTEM_MODSTAMP,
    TL.TERRITORY_TO_ADD,
    TL.TERRITORY_TO_DROP,
    WTL.LEVEL_2 AS TERRITORY,
    TL.COUNTRY_CODE,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm
    FROM itg_hcp360_veeva_account_territory_loader TL,
    wks_hcp360_in_veeva_territory_loader WTL,
    itg_hcp360_veeva_territory T
    WHERE TL.TL_ID = WTL.TL_ID 
    AND   trim(T.TERRITORY_NAME) =  trim(LEVEL_2(+))
    AND  LEVEL_2 IS NOT NULL
)
select territory_loader_id::varchar(18) as territory_loader_id,
    territory_source_id::varchar(18) as territory_source_id,
    tl_account_source_id::varchar(18) as tl_account_source_id,
    created_by_id::varchar(18) as created_by_id,
    created_date::timestamp_ntz(9) as created_date,
    tl_external_id::varchar(50) as tl_external_id,
    is_deleted::varchar(5) as is_deleted,
    is_locked::varchar(5) as is_locked,
    sfe_approved::varchar(5) as sfe_approved,
    last_modified_by_id::varchar(18) as last_modified_by_id,
    last_modified_date::timestamp_ntz(9) as last_modified_date,
    last_referenced_date::timestamp_ntz(9) as last_referenced_date,
    last_viewed_date::timestamp_ntz(9) as last_viewed_date,
    may_edit::varchar(5) as may_edit,
    mobile_id::varchar(100) as mobile_id,
    name::varchar(80) as name,
    owner_id::varchar(18) as owner_id,
    system_modstamp::timestamp_ntz(9) as system_modstamp,
    territory_to_add::varchar(255) as territory_to_add,
    territory_to_drop::varchar(255) as territory_to_drop,
    territory::varchar(1500) as territory,
    country_code::varchar(18) as country_code,
    crt_dttm::timestamp_ntz(9) as crt_dttm
 from final