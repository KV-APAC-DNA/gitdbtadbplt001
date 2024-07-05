with wks_skurecom_mi_target_retdim as
(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_target_retdim') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
cd as
(
    (SELECT fisc_yr, qtr
           FROM edw_retailer_calendar_dim
           WHERE day = TO_CHAR(to_date(convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)),'YYYYMMDD')                       -- Handle this via parameter table update
           GROUP BY fisc_yr,qtr)
),
final as(
    SELECT 
       rd.customer_code,
       rd.retailer_code,
       rd.rtruniquecode,
       rd.class_desc,
       rd.channel_name,
       rd.business_channel,
       rd.status_desc,
       rd.retailer_name,
       rd.retailer_category_name,
       rd.csrtrcode,
       rd.customer_name,
       rd.region_name,
       rd.zone_name,
       rd.territory_name,
       ---------------
       cd.qtr,
       cd.fisc_yr
---------------------
    FROM wks_skurecom_mi_target_retdim rd cross join cd
)
select * from final
