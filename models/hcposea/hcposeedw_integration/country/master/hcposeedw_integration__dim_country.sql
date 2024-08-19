with itg_country as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_COUNTRY
),
final as (
SELECT COUNTRY_KEY,
       COUNTRY_CODE,
       COUNTRY_NAME,
       COUNTRY_DISPLAY_ORDER,
       COUNTRY_GROUP_CODE,
       COUNTRY_GROUP_NAME,
       COUNTRY_GROUP_DISPLAY_ORDER,
       DESCRIPT,
       ATTR1,
       ATTR2,
       ATTR3,
       ATTR4,
       ATTR5,
       MANUAL_UPDATE_DATE,
       MANUAL_UPDATE_USER,
       SYSDATE(),
       NULL
FROM itg_country
)
select * from final