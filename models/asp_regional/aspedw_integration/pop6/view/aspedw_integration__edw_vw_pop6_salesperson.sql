with source as
(
    select * from {{ ref('aspitg_integration__itg_pop6_users') }}
),

final as
(
    select 
        users.cntry_cd,
        users.src_file_date,
        users.STATUS,
        users.userdb_id,
        users.username,
        users.first_name,
        users.last_name,
        users.team,
        users.superior_name,
        users.authorisation_group,
        users.email_address,
        users.longitude,
        users.latitude,
        users.business_units_id,
        users.business_unit_name
    from source users
    where users.active::text = 'Y'::character varying::text
)

select * from final