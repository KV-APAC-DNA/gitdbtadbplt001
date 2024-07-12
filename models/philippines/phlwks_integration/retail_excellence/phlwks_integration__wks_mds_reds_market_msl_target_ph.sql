with mds_reds_market_msl_target as (
    select * from {{ source('aspitg_integration', 'mds_reds_market_msl_target') }}
),
 edw_vw_os_time_dim as(
   select * from  {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
 ),

 wks_mds_reds_market_msl_target_ph as 
 (
    SELECT MSL_TARGET.START_MONTH_NAME,
        OS_TIME.MNTH_ID AS start_month_id,
        msl_target.end_month_name,
        os_time1.mnth_id AS end_month_id,
        msl_target.mdp_target,
        msl_target.brand_code
    FROM (SELECT UPPER(start_month_name) AS start_month_name,
                UPPER(end_month_name) AS end_month_name,
                start_year_code,
                end_year_code,
                mdp_target,
                brand_code
        FROM mds_reds_market_msl_target
        WHERE market = 'Philippines'
        GROUP BY 1,2,3,4,5,6) msl_target
    inner  join (SELECT DISTINCT mnth_id,
                        mnth_long AS start_month_long,
                        "year" AS start_year
                FROM edw_vw_os_time_dim) os_time
            ON (start_month_name = start_month_long
            AND start_year = start_year_code)
    inner  join (SELECT DISTINCT mnth_id,
                        mnth_long AS end_month_long,
                        "year" AS end_year
                FROM edw_vw_os_time_dim) os_time1
            ON (end_month_name = end_month_long
            AND end_year = end_year_code)
    GROUP BY 1,2,3,4,5,6
)
--final select 

select * from wks_mds_reds_market_msl_target_ph
