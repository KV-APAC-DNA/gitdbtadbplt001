with sdl_india_mds_log as 
(
    select * from {{ source('indsdl_raw', 'sdl_india_mds_log') }}
),
transformed as 
(
    SELECT 'IN' AS CNTRY_CD,
       REPLACE(REPLACE(SPLIT_PART(QUERY,' ',2),']',''),'[','') AS TABLE_NAME,
       RESULT AS RESULT,
       CASE
         WHEN RESULT LIKE 'Records affected%' THEN 'Success'
         ELSE 'Failure'
       END AS STATUS,
       CASE
         WHEN RESULT LIKE 'Records affected%' THEN CAST(SPLIT_PART(RESULT,' ',3) AS INT)
         ELSE NULL
       END AS REC_COUNT,
       crtd_dttm
FROM sdl_india_mds_log
WHERE RESULT IS NOT NULL
OR    QUERY IS NOT NULL
),
final as 
(
    select
        cntry_cd::varchar(10) as cntry_cd,
        table_name::varchar(255) as table_name,
        result::varchar(1000) as result,
        status::varchar(50) as status,
        rec_count::number(18,0) as rec_count,
        crtd_dttm::timestamp_ntz(9) as crtd_dt
    from transformed
)
select * from final
