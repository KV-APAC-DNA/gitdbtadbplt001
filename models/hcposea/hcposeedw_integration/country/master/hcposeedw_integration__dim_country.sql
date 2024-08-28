with itg_country as (
select * from {{ source('hcposeitg_integration', 'itg_country') }}
),
final as (
SELECT 
country_key as country_key,
country_code as country_code,
country_name as country_name,
country_display_order as country_display_order,
country_group_code as country_group_code,
country_group_name as country_group_name,
country_group_display_order as country_group_display_order,
descript as description,
attr1 as attr1,
attr2 as attr2,
attr3 as attr3,
attr4 as attr4,
attr5 as attr5,
manual_update_date as manual_update_date,
manual_update_user as manual_update_user,
sysdate() as inserted_date,
null as updated_date
FROM itg_country
)
select * from final