{{
    config(
        pre_hook="{{build_itg_pop6_users()}}"
    )
}}
with sdl_pop6_hk_users as 
(
    select * from {{ ref('aspwks_integration__wks_pop6_hk_users') }}
),
sdl_pop6_kr_users as 
(
    select * from {{ ref('aspwks_integration__wks_pop6_kr_users') }}
),
sdl_pop6_tw_users as 
(
    select * from {{ ref('aspwks_integration__wks_pop6_tw_users') }}
),
sdl_pop6_jp_users as 
(
    select * from {{ ref('aspwks_integration__wks_pop6_jp_users') }}
),
sdl_pop6_sg_users as 
(
    select * from {{ ref('aspwks_integration__wks_pop6_sg_users') }}
),
sdl_pop6_th_users as 
(
    select * from {{ ref('aspwks_integration__wks_pop6_th_users') }}
),
sdl AS 
(
    SELECT 'HK' AS CNTRY_CD,SDL_HK.* FROM sdl_pop6_hk_users SDL_HK
    UNION ALL
    SELECT 'KR' AS CNTRY_CD,SDL_KR.* FROM sdl_pop6_kr_users SDL_KR
    UNION ALL
    SELECT 'TW' AS CNTRY_CD, SDL_TW.* FROM sdl_pop6_tw_users SDL_TW
    UNION ALL
    SELECT 
        'JP' AS CNTRY_CD,
        status,
        userdb_id,
        username,
        first_name,
        last_name,
        team,
        superior_name,
        authorisation_group,
        email_address,
        longitude,
        latitude,
        file_name,
        run_id,
        crtd_dttm,
        hashkey,
        business_units_id,
        business_unit_name
    FROM sdl_pop6_jp_users sdl_jp
    UNION ALL
    SELECT 'SG' AS CNTRY_CD,
        status,
        userdb_id,
        username,
        first_name,
        last_name,
        team,
        superior_name,
        authorisation_group,
        email_address,
        longitude,
        latitude,
        file_name,
        run_id,
        crtd_dttm,
        hashkey,
        business_units_id,
        business_unit_name
    FROM sdl_pop6_sg_users sdl_sg
    UNION ALL
    SELECT 'TH' AS CNTRY_CD,
        status,
        userdb_id,
        username,
        first_name,
        last_name,
        team,
        superior_name,
        authorisation_group,
        email_address,
        longitude,
        latitude,
        file_name,
        run_id,
        crtd_dttm,
        hashkey,
        business_units_id,
        business_unit_name
    FROM sdl_pop6_th_users sdl_th
),
wks AS 
(
    SELECT 
        usr.cntry_cd,
        usr.src_file_date,
        usr.status,
        usr.userdb_id,
        usr.username,
        usr.first_name,
        usr.last_name,
        usr.team,
        usr.superior_name,
        usr.authorisation_group,
        usr.email_address,
        usr.longitude,
        usr.latitude,
        usr.hashkey,
        usr.effective_from,
        CASE
            WHEN usr.effective_to IS NULL THEN dateadd(DAY, -1, current_timestamp())
            ELSE usr.effective_to
        END AS effective_to,
        'N' AS active,
        usr.file_name,
        usr.run_id,
        usr.crtd_dttm,
        usr.updt_dttm,
        business_units_id,
        business_unit_name
    FROM (
            SELECT itg.*
            FROM sdl,
                {{this}} itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.userdb_id = itg.userdb_id
        ) usr
    UNION ALL
    SELECT usr.cntry_cd,
        SUBSTRING(usr.file_name, 1, 8) AS src_file_date,
        usr.status,
        usr.userdb_id,
        usr.username,
        usr.first_name,
        usr.last_name,
        usr.team,
        usr.superior_name,
        usr.authorisation_group,
        usr.email_address,
        usr.longitude,
        usr.latitude,
        usr.hashkey,
        current_timestamp() AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        usr.file_name,
        usr.run_id,
        usr.crtd_dttm,
        current_timestamp() AS updt_dttm,
        business_units_id,
        business_unit_name
    FROM (
            SELECT sdl.*
            FROM sdl,
                {{this}} itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.userdb_id = itg.userdb_id
                AND itg.active = 'Y'
        ) usr
    UNION ALL
    SELECT 
        usr.cntry_cd,
        SUBSTRING(usr.file_name, 1, 8) AS src_file_date,
        usr.status,
        usr.userdb_id,
        usr.username,
        usr.first_name,
        usr.last_name,
        usr.team,
        usr.superior_name,
        usr.authorisation_group,
        usr.email_address,
        usr.longitude,
        usr.latitude,
        usr.hashkey,
        usr.effective_from,
        NULL AS effective_to,
        'Y' AS active,
        usr.file_name,
        usr.run_id,
        usr.crtd_dttm,
        current_timestamp() AS updt_dttm,
        business_units_id,
        business_unit_name
    FROM (
            SELECT sdl.*,
                itg.effective_from
            FROM sdl,
                {{this}} itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.userdb_id = itg.userdb_id
                AND itg.active = 'Y'
        ) usr
    UNION ALL
    SELECT 
        usr.cntry_cd,
        usr.src_file_date,
        usr.status,
        usr.userdb_id,
        usr.username,
        usr.first_name,
        usr.last_name,
        usr.team,
        usr.superior_name,
        usr.authorisation_group,
        usr.email_address,
        usr.longitude,
        usr.latitude,
        usr.hashkey,
        usr.effective_from,
        usr.effective_to,
        usr.active,
        usr.file_name,
        usr.run_id,
        usr.crtd_dttm,
        usr.updt_dttm,
        business_units_id,
        business_unit_name
    FROM (
            SELECT itg.*
            FROM sdl,
                {{this}} itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.userdb_id = itg.userdb_id
                AND itg.active = 'N'
        ) usr
    UNION ALL
    SELECT usr.cntry_cd,
        SUBSTRING(usr.file_name, 1, 8) AS src_file_date,
        usr.status,
        usr.userdb_id,
        usr.username,
        usr.first_name,
        usr.last_name,
        usr.team,
        usr.superior_name,
        usr.authorisation_group,
        usr.email_address,
        usr.longitude,
        usr.latitude,
        usr.hashkey,
        current_timestamp() AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        usr.file_name,
        usr.run_id,
        usr.crtd_dttm,
        current_timestamp() AS updt_dttm,
        business_units_id,
        business_unit_name
    FROM (
            SELECT *
            FROM sdl
            WHERE userdb_id NOT IN (
                    SELECT userdb_id
                    FROM {{this}}
                )
        ) usr
),
transformed as
(
    SELECT * FROM wks
    UNION ALL
    SELECT * FROM {{this}}
    WHERE userdb_id NOT IN 
    (
        SELECT userdb_id FROM wks
    )
),
final as 
(
    select 
    	cntry_cd::varchar(10) as cntry_cd,
        src_file_date::varchar(10) as src_file_date,
        status::number(18,0) as status,
        userdb_id::varchar(255) as userdb_id,
        username::varchar(50) as username,
        first_name::varchar(50) as first_name,
        last_name::varchar(50) as last_name,
        team::varchar(50) as team,
        superior_name::varchar(50) as superior_name,
        authorisation_group::varchar(50) as authorisation_group,
        email_address::varchar(50) as email_address,
        longitude::number(18,5) as longitude,
        latitude::number(18,5) as latitude,
        hashkey::varchar(200) as hashkey,
        effective_from::timestamp_ntz(9) as effective_from,
        effective_to::timestamp_ntz(9) as effective_to,
        active::varchar(2) as active,
        file_name::varchar(100) as file_name,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        business_units_id::varchar(200) as business_units_id,
        business_unit_name::varchar(200) as business_unit_name
    from transformed
)
select * from final