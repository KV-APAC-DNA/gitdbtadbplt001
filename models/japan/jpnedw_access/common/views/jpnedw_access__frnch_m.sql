with source as(
    select * from {{ ref('jpnedw_integration__frnch_m') }}
),
transformed as(
    SELECT create_dt as "create_dt"
        ,create_user as "create_user"
        ,update_dt as "update_dt"
        ,update_user as "update_user"
        ,ph_cd as "ph_cd"
        ,ph_lvl as "ph_lvl"
        ,ph_nm as "ph_nm"
    FROM source
)
select * from transformed