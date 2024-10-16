{{
    config(
        materialized="incremental",
        incremental_strategy= "append",    
    )
}}

with edw_rpt_sales_details as 
(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
)
,final as (
SELECT mth_mm, 'NA' AS channel_name, SUM(achievement_nr) AS achievement_nr, CURRENT_TIMESTAMP AS crt_dttm, 'Total' AS dataset
FROM edw_rpt_sales_details
WHERE mth_mm >= 202301
GROUP BY 1
UNION ALL 
SELECT mth_mm, channel_name, SUM(achievement_nr) AS achievement_nr, CURRENT_TIMESTAMP AS crt_dttm, 'Channel_level' AS dataset
FROM edw_rpt_sales_details
WHERE mth_mm >= 202301
GROUP BY 1,2
)
select 
    mth_mm,
    channel_name,
    achievement_nr,
    crt_dttm,
    dataset
from 
    final