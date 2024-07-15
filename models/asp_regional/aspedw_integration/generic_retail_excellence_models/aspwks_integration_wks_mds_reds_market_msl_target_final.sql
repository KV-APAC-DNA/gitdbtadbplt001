with mds_reds_market_msl_target as (
    select * from {{ source('aspitg_integration', 'mds_reds_market_msl_target') }}
),
 edw_vw_os_time_dim as(
   select * from  {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
 ),
 wks_mds_reds_market_msl_target_final as(

    SELECT market,MSL_TARGET.START_MONTH_NAME, OS_TIME.MNTH_ID as start_month_id,msl_target.end_month_name,os_time1.mnth_id as end_month_id,msl_target.mdp_target,msl_target.brand_code  from		
 (
    select market,upper(start_month_name) as start_month_name ,upper(end_month_name) as end_month_name,start_year_code,
            end_year_code,mdp_target,brand_code from mds_reds_market_msl_target 
            group by 1,2,3,4,5,6,7 )msl_target
        inner  join
             (select distinct mnth_id,mnth_long as start_month_long ,"year"as start_year from edw_vw_os_time_dim ) os_time on (start_month_name=start_month_long  and start_year=start_year_code)		
        inner  join 
            (select distinct mnth_id,mnth_long as end_month_long ,"year" as end_year from edw_vw_os_time_dim ) os_time1 on (end_month_name=end_month_long  and end_year=end_year_code)		
        group by 1,2,3,4,5,6,7 
 )
select * from wks_mds_reds_market_msl_target_final

