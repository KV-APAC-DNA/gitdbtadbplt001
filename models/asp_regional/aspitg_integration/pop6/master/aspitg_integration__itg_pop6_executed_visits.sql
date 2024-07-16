{{
    config
        (
            materialized="incremental",
            incremental_strategy= "append"
        )
}}
with sdl_pop6_kr_executed_visits as
(
    select * from {{ref('aspwks_integration__wks_pop6_kr_executed_visits')}}
),
sdl_pop6_tw_executed_visits as
(
    select * from {{ref('aspwks_integration__wks_pop6_tw_executed_visits')}}
),
sdl_pop6_hk_executed_visits as
(
    select * from {{ref('aspwks_integration__wks_pop6_hk_executed_visits')}}
),
sdl_pop6_jp_executed_visits as
(
    select * from {{ref('aspwks_integration__wks_pop6_jp_executed_visits')}}
),
sdl_pop6_sg_executed_visits as
(
    select * from {{ref('aspwks_integration__wks_pop6_sg_executed_visits')}}
),
sdl_pop6_th_executed_visits as
(
    select * from {{ref('aspwks_integration__wks_pop6_th_executed_visits')}}
),
transformed as
(
    SELECT
        'KR' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        visit_date as visit_date,
        check_in_datetime as check_in_datetime,
        check_out_datetime as check_out_datetime,
        popdb_id as popdb_id,
        pop_code as pop_code,
        pop_name as pop_name,
        address as address,
        check_in_longitude as check_in_longitude,
        check_in_latitude as check_in_latitude,
        check_out_longitude as check_out_longitude,
        check_out_latitude as check_out_latitude,
        check_in_photo as check_in_photo,
        check_out_photo as check_out_photo,
        username as username,
        user_full_name as user_full_name,
        superior_username as superior_username,
        superior_name as superior_name,
        planned_visit as planned_visit,
        cancelled_visit as cancelled_visit,
        cancellation_reason as cancellation_reason,
        cancellation_note as cancellation_note,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm,
    FROM sdl_pop6_kr_executed_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='KR')
    {% endif %}
    union
    SELECT
        'TW' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        visit_date as visit_date,
        check_in_datetime as check_in_datetime,
        check_out_datetime as check_out_datetime,
        popdb_id as popdb_id,
        pop_code as pop_code,
        pop_name as pop_name,
        address as address,
        check_in_longitude as check_in_longitude,
        check_in_latitude as check_in_latitude,
        check_out_longitude as check_out_longitude,
        check_out_latitude as check_out_latitude,
        check_in_photo as check_in_photo,
        check_out_photo as check_out_photo,
        username as username,
        user_full_name as user_full_name,
        superior_username as superior_username,
        superior_name as superior_name,
        planned_visit as planned_visit,
        cancelled_visit as cancelled_visit,
        cancellation_reason as cancellation_reason,
        cancellation_note as cancellation_note,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_tw_executed_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TW')
    {% endif %}
    union
    SELECT
        'HK' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        visit_date as visit_date,
        check_in_datetime as check_in_datetime,
        check_out_datetime as check_out_datetime,
        popdb_id as popdb_id,
        pop_code as pop_code,
        pop_name as pop_name,
        address as address,
        check_in_longitude as check_in_longitude,
        check_in_latitude as check_in_latitude,
        check_out_longitude as check_out_longitude,
        check_out_latitude as check_out_latitude,
        check_in_photo as check_in_photo,
        check_out_photo as check_out_photo,
        username as username,
        user_full_name as user_full_name,
        superior_username as superior_username,
        superior_name as superior_name,
        planned_visit as planned_visit,
        cancelled_visit as cancelled_visit,
        cancellation_reason as cancellation_reason,
        cancellation_note as cancellation_note,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_hk_executed_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='HK')
    {% endif %}
    union
    SELECT
        'JP' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        visit_date as visit_date,
        check_in_datetime as check_in_datetime,
        check_out_datetime as check_out_datetime,
        popdb_id as popdb_id,
        pop_code as pop_code,
        pop_name as pop_name,
        address as address,
        check_in_longitude as check_in_longitude,
        check_in_latitude as check_in_latitude,
        check_out_longitude as check_out_longitude,
        check_out_latitude as check_out_latitude,
        check_in_photo as check_in_photo,
        check_out_photo as check_out_photo,
        username as username,
        user_full_name as user_full_name,
        superior_username as superior_username,
        superior_name as superior_name,
        planned_visit as planned_visit,
        cancelled_visit as cancelled_visit,
        cancellation_reason as cancellation_reason,
        cancellation_note as cancellation_note,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_jp_executed_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='JP')
    {% endif %}
    union
    SELECT
        'SG' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        visit_date as visit_date,
        check_in_datetime as check_in_datetime,
        check_out_datetime as check_out_datetime,
        popdb_id as popdb_id,
        pop_code as pop_code,
        pop_name as pop_name,
        address as address,
        check_in_longitude as check_in_longitude,
        check_in_latitude as check_in_latitude,
        check_out_longitude as check_out_longitude,
        check_out_latitude as check_out_latitude,
        check_in_photo as check_in_photo,
        check_out_photo as check_out_photo,
        username as username,
        user_full_name as user_full_name,
        superior_username as superior_username,
        superior_name as superior_name,
        planned_visit as planned_visit,
        cancelled_visit as cancelled_visit,
        cancellation_reason as cancellation_reason,
        cancellation_note as cancellation_note,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm,
    FROM sdl_pop6_sg_executed_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='SG')
    {% endif %}
    union
    SELECT
        'TH' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        visit_date as visit_date,
        check_in_datetime as check_in_datetime,
        check_out_datetime as check_out_datetime,
        popdb_id as popdb_id,
        pop_code as pop_code,
        pop_name as pop_name,
        address as address,
        check_in_longitude as check_in_longitude,
        check_in_latitude as check_in_latitude,
        check_out_longitude as check_out_longitude,
        check_out_latitude as check_out_latitude,
        check_in_photo as check_in_photo,
        check_out_photo as check_out_photo,
        username as username,
        user_full_name as user_full_name,
        superior_username as superior_username,
        superior_name as superior_name,
        planned_visit as planned_visit,
        cancelled_visit as cancelled_visit,
        cancellation_reason as cancellation_reason,
        cancellation_note as cancellation_note,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    FROM sdl_pop6_th_executed_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TH')
    {% endif %}
),
final as
(
    select
        cntry_cd::varchar(10) as cntry_cd,
        src_file_date::varchar(10) as src_file_date,
        visit_id::varchar(255) as visit_id,
        visit_date::date as visit_date,
        check_in_datetime::timestamp_ntz(9) as check_in_datetime,
        check_out_datetime::timestamp_ntz(9) as check_out_datetime,
        popdb_id::varchar(255) as popdb_id,
        pop_code::varchar(50) as pop_code,
        pop_name::varchar(100) as pop_name,
        address::varchar(150) as address,
        check_in_longitude::number(18,5) as check_in_longitude,
        check_in_latitude::number(18,5) as check_in_latitude,
        check_out_longitude::number(18,5) as check_out_longitude,
        check_out_latitude::number(18,5) as check_out_latitude,
        check_in_photo::varchar(200) as check_in_photo,
        check_out_photo::varchar(200) as check_out_photo,
        username::varchar(50) as username,
        user_full_name::varchar(50) as user_full_name,
        superior_username::varchar(50) as superior_username,
        superior_name::varchar(50) as superior_name,
        planned_visit::number(18,0) as planned_visit,
        cancelled_visit::number(18,0) as cancelled_visit,
        cancellation_reason::varchar(255) as cancellation_reason,
        cancellation_note::varchar(255) as cancellation_note,
        file_name::varchar(100) as file_name,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final