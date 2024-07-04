{{
    config(
        pre_hook='{{build_itg_pop6_pop_lists_temp()}}'
    )
}}
with sdl_pop6_hk_pop_lists as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_pop_lists') }}
),
sdl_pop6_kr_pop_lists as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_pop_lists') }}
),
sdl_pop6_tw_pop_lists as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_pop_lists') }}
),
sdl_pop6_jp_pop_lists as (
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_pop_lists') }}
),
sdl_pop6_sg_pop_lists as (
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_pop_lists') }}
),
sdl_pop6_th_pop_lists as (
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_pop_lists') }}
),
itg_pop6_pop_lists as (
    select * from {{ source('aspitg_integration','itg_pop6_pop_lists_temp') }}
),
sdl as (
    SELECT 'HK' AS CNTRY_CD,
        SDL_HK.*
    FROM sdl_pop6_hk_pop_lists SDL_HK
    UNION ALL
    SELECT 'KR' AS CNTRY_CD,
        SDL_KR.*
    FROM sdl_pop6_kr_pop_lists SDL_KR
    UNION ALL
    SELECT 'TW' AS CNTRY_CD,
        SDL_TW.*
    FROM sdl_pop6_tw_pop_lists SDL_TW
    UNION ALL
    SELECT 'JP' AS CNTRY_CD,
        SDL_JP.*
    FROM sdl_pop6_jp_pop_lists SDL_JP
    UNION ALL
    SELECT 'SG' AS CNTRY_CD,
        SDL_SG.*
    FROM sdl_pop6_sg_pop_lists SDL_SG
    UNION ALL
    SELECT 'TH' AS CNTRY_CD,
        SDL_TH.*
    FROM sdl_pop6_th_pop_lists SDL_TH
),
wks as (
    SELECT poplist.cntry_cd,
        poplist.src_file_date,
        poplist.status,
        poplist.pop_list,
        poplist.popdb_id,
        poplist.pop_code,
        poplist.pop_name,
        poplist.pop_list_date,
        poplist.hashkey,
        poplist.effective_from,
        CASE
            WHEN poplist.effective_to IS NULL THEN dateadd(DAY, -1, current_timestamp())
            ELSE poplist.effective_to
        END AS effective_to,
        'N' AS active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        poplist.updt_dttm
    FROM (
            SELECT itg.*
            FROM sdl,
                itg_pop6_pop_lists itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.pop_list = itg.pop_list
                AND sdl.popdb_id = itg.popdb_id
        ) poplist
    UNION ALL
    SELECT poplist.cntry_cd,
        SUBSTRING(poplist.file_name, 1, 8) AS src_file_date,
        poplist.status,
        poplist.pop_list,
        poplist.popdb_id,
        poplist.pop_code,
        poplist.pop_name,
        poplist.pop_list_date,
        poplist.hashkey,
        current_timestamp() AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        current_timestamp() AS updt_dttm
    FROM (
            SELECT sdl.*
            FROM sdl,
                itg_pop6_pop_lists itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.pop_list = itg.pop_list
                AND sdl.popdb_id = itg.popdb_id
                AND itg.active = 'Y'
        ) poplist
    UNION ALL
    SELECT poplist.cntry_cd,
        SUBSTRING(poplist.file_name, 1, 8) AS src_file_date,
        poplist.status,
        poplist.pop_list,
        poplist.popdb_id,
        poplist.pop_code,
        poplist.pop_name,
        poplist.pop_list_date,
        poplist.hashkey,
        poplist.effective_from,
        NULL AS effective_to,
        'Y' AS active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        current_timestamp() AS updt_dttm
    FROM (
            SELECT sdl.*,
                itg.effective_from
            FROM sdl,
                itg_pop6_pop_lists itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.pop_list = itg.pop_list
                AND sdl.popdb_id = itg.popdb_id
                AND itg.active = 'Y'
        ) poplist
    UNION ALL
    SELECT poplist.cntry_cd,
        poplist.src_file_date,
        poplist.status,
        poplist.pop_list,
        poplist.popdb_id,
        poplist.pop_code,
        poplist.pop_name,
        poplist.pop_list_date,
        poplist.hashkey,
        poplist.effective_from,
        poplist.effective_to,
        poplist.active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        poplist.updt_dttm
    FROM (
            SELECT itg.*
            FROM sdl,
                itg_pop6_pop_lists itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.pop_list = itg.pop_list
                AND sdl.popdb_id = itg.popdb_id
                AND itg.active = 'N'
        ) poplist
    UNION ALL
    SELECT poplist.cntry_cd,
        SUBSTRING(poplist.file_name, 1, 8) AS src_file_date,
        poplist.status,
        poplist.pop_list,
        poplist.popdb_id,
        poplist.pop_code,
        poplist.pop_name,
        poplist.pop_list_date,
        poplist.hashkey,
        current_timestamp() AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        current_timestamp() AS updt_dttm
    FROM (
            SELECT *
            FROM sdl
            WHERE pop_list || popdb_id NOT IN (
                    SELECT pop_list || popdb_id
                    FROM itg_pop6_pop_lists
                )
        ) poplist
),
itg_pop6_pop_lists_wrk as (
    select * from wks
    union all
    select * from itg_pop6_pop_lists
    where pop_list || popdb_id not in (
            select pop_list || popdb_id
            from wks
        )
),
transformed as (
    select cntry_cd,
        src_file_date,
        status,
        pop_list,
        popdb_id,
        pop_code,
        pop_name,
        pop_list_date,
        hashkey,
        effective_from,
        effective_to,
        active,
        file_name,
        run_id,
        crtd_dttm,
        updt_dttm,
        row_number() over(
            partition by cntry_cd,
            src_file_date,
            status,
            pop_list,
            popdb_id,
            pop_code,
            pop_name,
            pop_list_date,
            hashkey,
            effective_from,
            effective_to,
            active,
            file_name,
            run_id,
            crtd_dttm,
            updt_dttm
            order by effective_to desc
        ) as rnk
    from itg_pop6_pop_lists_wrk
),
final as (
    select cntry_cd::varchar(10) as cntry_cd,
        src_file_date::varchar(10) as src_file_date,
        status::number(18,0) as status,
        pop_list::varchar(25) as pop_list,
        popdb_id::varchar(255) as popdb_id,
        pop_code::varchar(50) as pop_code,
        pop_name::varchar(100) as pop_name,
        pop_list_date::date as pop_list_date,
        hashkey::varchar(200) as hashkey,
        effective_from::timestamp_ntz(9) as effective_from,
        effective_to::timestamp_ntz(9) as effective_to,
        active::varchar(2) as active,
        file_name::varchar(100) as file_name,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
    where rnk=1
)
select * from final