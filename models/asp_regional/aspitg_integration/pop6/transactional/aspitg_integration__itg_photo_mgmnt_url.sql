with wks_photo_mgmnt_url_wrk1 as (
    select * from {{ ref('aspwks_integration__wks_photo_mgmnt_url_wrk1') }}
),
edw_rpt_sfa_pm as (
    select * from {{ source('ntaedw_integration', 'edw_rpt_sfa_pm') }} -- using it as a source because a loop is created
),
wks_photo_mgmnt_url_wrk2 as (
    select * from {{ ref('aspwks_integration__wks_photo_mgmnt_url_wrk2') }}
),
numbersequence as (
    with recursive numbers(number) as
    (
        select 1
        union all
        select number + 1 from numbers
        where number < (select max(url_cnt) from wks_photo_mgmnt_url_wrk2)
    )
    select * from numbers
),
wrk1 as (
    SELECT ORIGINAL_PHOTO_KEY,
        ORIGINAL_RESPONSE,
        PHOTO_KEY,
        RESPONSE,
        URL_CNT,
        RUN_ID,
        CREATE_DT,
        CASE
            WHEN PM.TASKID IS NULL THEN 'Y'
            ELSE UPLOAD_PHOTO_FLAG
        END UPLOAD_PHOTO_FLAG
    FROM WKS_PHOTO_MGMNT_URL_WRK1 WRK1,
        (
            SELECT DISTINCT SPLIT_PART(FILENAME, '.', 1) FILENAME,
                TASKID
            FROM EDW_RPT_SFA_PM
        ) PM
    WHERE WRK1.PHOTO_KEY = PM.FILENAME(+)
),
wrk2 as (
    SELECT fir.photo_key AS original_photo_key,
        fir.response AS original_response,
        fir.photo_key || '_' || ns.number AS photo_key,
        SPLIT_PART(fir.response, ',', ns.number::INT) AS response,
        fir.url_cnt,
        fir.run_id,
        current_timestamp() AS CREATE_DT,
        'Y' AS UPLOAD_PHOTO_FLAG
    FROM (
            SELECT --ns.n,
                photo_key,
                response,
                url_cnt,
                run_id
            FROM WKS_PHOTO_MGMNT_URL_WRK2
        ) fir
        JOIN numbersequence ns ON ns.number <= fir.url_cnt
),
transformed as (
    select * from wrk1
    union all
    select * from wrk2
),
final as (
    select
        original_photo_key::varchar(1031) as original_photo_key,
        original_response::varchar(65535) as original_response,
        photo_key::varchar(1053) as photo_key,
        response::varchar(65535) as response,
        url_cnt::number(18,0) as url_cnt,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        upload_photo_flag::varchar(1) as upload_photo_flag
    from transformed
)
select * from final