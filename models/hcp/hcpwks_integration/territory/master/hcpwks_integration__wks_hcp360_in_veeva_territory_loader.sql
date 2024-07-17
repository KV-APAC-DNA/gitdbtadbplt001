with itg_hcp360_veeva_account_territory_loader as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_account_territory_loader') }}
),
final as
(
    SELECT
    TL_ID ,
    TERRITORY,
    'IN' AS COUNTRY_CODE,
    SPLIT_PART(TERRITORY,';',2) AS LEVEL1,
    SPLIT_PART(TERRITORY,';',3) AS LEVEL2,
    NULL AS LEVEL3,
    NULL AS LEVEL4,
    NULL AS LEVEL5,
    NULL AS LEVEL6,
    NULL AS LEVEL7,
    NULL AS LEVEL8,
    NULL AS LEVEL9,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
    NULL AS UPDT_DTTM
    FROM itg_hcp360_veeva_account_territory_loader
)
select * from final