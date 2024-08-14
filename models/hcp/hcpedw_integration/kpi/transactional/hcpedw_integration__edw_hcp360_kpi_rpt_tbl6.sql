{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where rtrim(upper(source_system)) = 'GA360'
        and country = 'IN';
        {% endif %}"
    )
}}
with 
edw_ga360_aspac_sessions as 
(
    select * from {{ ref('hcpedw_integration__edw_ga360_aspac_sessions') }}
),
trans as 
(
    SELECT TEMP.COUNTRY,
    'GA360' AS source_system,
    'DIGITAL' AS channel,
    'WEBSITE' AS activity_type,
    NULL AS hcp_master_id,
    NULL AS employee_id,
    TEMP.brand,
    NULL AS brand_category,
    NULL AS speciality,
    NULL AS core_noncore,
    NULL AS classification,
    NULL AS territory,
    TEMP.region,
    NULL AS zone,
    NULL AS hcp_created_date,
    NULL AS activity_date,
    NULL AS call_source_id,
    NULL AS product_indication_name,
    NULL AS no_of_prescription_units,
    NULL AS first_prescription_date,
    NULL AS planned_call_cnt,
    NULL AS call_duration,
    NULL AS prescription_id,
    NULL AS noofprescritions,
    NULL AS noofprescribers,
    NULL AS email_name,
    NULL AS is_unique,
    NULL AS email_delivered_flag,
    current_timestamp() AS crt_dttm,
    current_timestamp() AS updt_dttm,
    NULL AS target_value,
    NULL AS target_kpi,
    NULL AS report_brand_reference,
    NULL AS diagnosis,
    NULL AS region_hq,
    NULL AS email_activity_type,
    NULL AS hcp_id,
    NULL AS transaction_flag,
    NULL AS iqvia_brand,
    NULL AS iqvia_pack_description,
    NULL AS iqvia_product_description,
    NULL AS iqvia_pack_volume,
    NULL AS iqvia_input_brand,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS field_rep_active,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL AS totalprescritions_by_brand,
    NULL AS totalprescribers_by_brand,
    NULL AS totalprescritions_jnj_brand,
    NULL AS call_type,
    NULL AS email_subject,
    NULL AS totalprescritions_by_speciality,
    NULL AS totalprescribers_by_speciality,
    NULL AS totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication,
    TEMP.year_month,
    TEMP.devicecategory,
    TEMP.channelgrouping,
    TEMP.visitor_country,
    NVL(TEMP.new_visitors, 0) as new_visitors,
    NVL(TEMP.REPEAT_VISITORS, 0) as REPEAT_VISITORS,
    NVL(TEMP.ALL_VISITOR, 0) as ALL_VISITOR,
    NVL(TEMP.unique_pageviews, 0) as unique_pageviews,
    NVL(TEMP.total_downloads, 0) as total_downloads -- pg.pages_per_session 
,
    NVL(TEMP.pages, 0) as pages,
    TEMP.page_sessions,
    NVL(TEMP.total_page_views, 0) as total_page_views --sr.bounce_rate 
    -- sr.avg_session_duration 
,
    NVL(TEMP.total_bounces, 0) as total_bounces,
    NVL(TEMP.total_session_duration, 0) as total_session_duration,
    TEMP.sessions
FROM (
        with base as (
            SELECT COUNTRY,
                brand,
                year_month,
                devicecategory,
                channelgrouping,
                visitor_country,
                region
            FROM edw_ga360_aspac_sessions
            where country = 'IN'
            GROUP BY COUNTRY,
                brand,
                year_month,
                devicecategory,
                channelgrouping,
                visitor_country,
                region
        ) ---new visitor
,
        nv as (
            SELECT COUNTRY,
                year_month,
                brand,
                channelgrouping,
                devicecategory,
                visitor_country,
                region,
                SUM(nvl (new_visitors, 0)) AS new_visitors,
                SUM(nvl (repeat_visitors, 0)) AS repeat_visitors,
                SUM(nvl (all_visitor, 0)) AS all_visitor
            FROM (
                    SELECT COUNTRY,
                        year_month,
                        brand,
                        channelgrouping,
                        devicecategory,
                        visitor_country,
                        region,
                        CASE
                            WHEN fct.new_visits = 1 THEN COUNT(DISTINCT fullvisitorid)
                            ELSE NULL
                        END AS new_visitors,
                        CASE
                            WHEN visitid > 1
                            and nvl (new_visits, 0) != 1 THEN COUNT(DISTINCT fullvisitorid)
                            /*Logic updated as per BA confirmation*/
                            ELSE NULL
                        END AS repeat_visitors,
                        COUNT(DISTINCT fullvisitorid) AS all_visitor
                    FROM edw_ga360_aspac_sessions fct
                    WHERE country = 'IN'
                        and hostname IN  ('www.orsl.in','professional.johnsonsbaby.in') --> added as part of phase 2 in parameterization
                        
                    GROUP BY year_month,
                        COUNTRY,
                        brand,
                        channelgrouping,
                        devicecategory,
                        visitor_country,
                        region,
                        new_visits,
                        visitid
                    ORDER BY year_month DESC
                )
            GROUP BY year_month,
                COUNTRY,
                brand,
                channelgrouping,
                devicecategory,
                visitor_country,
                region
        ),
        pg as (
            SELECT COUNTRY,
                year_month,
                brand,
                channelgrouping,
                devicecategory,
                visitor_country,
                region,
                COUNT(DISTINCT (pagepath)) AS unique_pageViews,
                SUM(nvl (downloads_cm31, 0)) AS total_downloads,
                COUNT(pagepath) AS pages,
                COUNT(DISTINCT COALESCE(fullvisitorid || '-' || visitid,null)) AS page_sessions,
                ROUND(
                    pages::numeric(15, 4) / page_sessions::numeric(15, 4),
                    2
                ) AS pages_per_session
            FROM edw_ga360_aspac_sessions
            WHERE TYPE = 'PAGE'
                AND hostname IN  ('www.orsl.in','professional.johnsonsbaby.in') --> added as part of phase 2 in parameterization
                and country = 'IN'
            GROUP BY year_month,
                COUNTRY,
                brand,
                channelgrouping,
                devicecategory,
                visitor_country,
                region
            ORDER BY year_month DESC
        ),
        tpg as (
            SELECT COUNTRY,
                year_month,
                brand,
                channelgrouping,
                devicecategory,
                visitor_country,
                region,
                SUM(page_views) AS total_page_views
            FROM (
                    select COUNTRY,
                        year_month,
                        brand,
                        channelgrouping,
                        devicecategory,
                        visitor_country,
                        region,
                        CASE
                            WHEN hitNumber = MIN(hitNumber) OVER (PARTITION BY fullVisitorId, hit_visitStartTime) THEN pageviews
                            ELSE 0
                        END AS page_views
                    FROM edw_ga360_aspac_sessions
                    WHERE country = 'IN'
                        and hostname IN  ('www.orsl.in','professional.johnsonsbaby.in') --> added as part of phase 2 in parameterization
                )
            GROUP BY year_month,
                COUNTRY,
                brand,
                channelgrouping,
                devicecategory,
                visitor_country,
                region
            ORDER BY year_month DESC
        ),
        sr as (
            SELECT COUNTRY,
                year_month,
                brand,
                channelgrouping,
                devicecategory,
                visitor_country,
                region,
                SUM(nvl (bounces, 0)) AS total_bounces,
                COUNT(DISTINCT (session_id)) AS sessions,
                ROUND(
                    total_bounces::numeric(15, 4) / sessions::numeric(15, 4),
                    2
                ) AS bounce_rate,
                SUM(time_on_site) AS total_session_duration,
                ROUND(
                    total_session_duration::numeric(15, 4) / sessions::numeric(15, 4),
                    2
                ) AS avg_session_duration
            FROM (
                    select COUNTRY,
                        year_month,
                        brand,
                        channelgrouping,
                        devicecategory,
                        visitor_country,
                        region,
                        CASE
                            WHEN hitNumber = MIN(
                                CASE
                                    WHEN isInteraction IS NOT NULL THEN hitNumber
                                    ELSE 0
                                END
                            ) OVER (PARTITION BY fullVisitorId, hit_visitStartTime) THEN bounces
                        END AS bounces,
                        CASE
                            WHEN hitNumber = MIN(hitNumber) OVER (PARTITION BY fullVisitorId, hit_visitStartTime) THEN timeonsite
                            ELSE 0
                        END AS time_on_site,
                        COALESCE(fullvisitorid || '-' || visitid,null) AS session_id
                    FROM edw_ga360_aspac_sessions
                    WHERE country = 'IN'
                        and hostname IN  ('www.orsl.in','professional.johnsonsbaby.in') 
                )
            GROUP BY year_month,
                COUNTRY,
                brand,
                channelgrouping,
                devicecategory,
                visitor_country,
                region
            ORDER BY year_month DESC
        )
        SELECT base.COUNTRY,
            base.brand,
            base.year_month,
            base.devicecategory,
            base.channelgrouping,
            base.visitor_country,
            base.region,
            nv.new_visitors,
            nv.repeat_visitors,
            nv.all_visitor,
            pg.unique_pageViews,
            pg.total_downloads,
            -- pg.pages_per_session ,
            pg.pages,
            pg.page_sessions,
            tpg.total_page_views,
            --sr.bounce_rate ,
            -- sr.avg_session_duration ,
            sr.total_bounces,
            sr.total_session_duration,
            sr.sessions
        FROM base
            LEFT OUTER JOIN nv ON base.COUNTRY = nv.COUNTRY
            and base.brand = nv.brand
            and base.year_month = nv.year_month
            and base.devicecategory = nv.devicecategory
            and base.channelgrouping = nv.channelgrouping
            and base.visitor_country = nv.visitor_country
            and base.region = nv.region
            LEFT OUTER JOIN pg ON base.COUNTRY = pg.COUNTRY
            and base.brand = pg.brand
            and base.year_month = pg.year_month
            and base.devicecategory = pg.devicecategory
            and base.channelgrouping = pg.channelgrouping
            and base.visitor_country = pg.visitor_country
            and base.region = pg.region
            LEFT OUTER JOIN tpg ON base.COUNTRY = tpg.COUNTRY
            and base.brand = tpg.brand
            and base.year_month = tpg.year_month
            and base.devicecategory = tpg.devicecategory
            and base.channelgrouping = tpg.channelgrouping
            and base.visitor_country = tpg.visitor_country
            and base.region = tpg.region
            LEFT OUTER JOIN sr ON base.COUNTRY = sr.COUNTRY
            and base.brand = sr.brand
            and base.year_month = sr.year_month
            and base.devicecategory = sr.devicecategory
            and base.channelgrouping = sr.channelgrouping
            and base.visitor_country = sr.visitor_country
            and base.region = sr.region
    ) TEMP
),
final as 
(
    select 
        country,
        source_system,
        channel,
        activity_type,
        hcp_master_id,
        employee_id,
        brand,
        brand_category,
        speciality,
        core_noncore,
        classification,
        territory,
        region,
        zone,
        hcp_created_date,
        activity_date,
        call_source_id,
        product_indication_namE,
        no_of_prescription_uniTS,
        first_prescription_datE,
        planned_call_cnt,
        call_duration,
        prescription_id,
        noofprescritions,
        noofprescribers,
        email_name,
        is_unique,
        email_delivered_flag,
        crt_dttm,
        updt_dttm,
        target_value,
        target_kpi,
        report_brand_reference,
        diagnosis,
        region_hq,
        email_activity_type,
        hcp_id,
        transaction_flag,
        iqvia_brand,
        iqvia_pack_description,
        iqvia_product_description,
        iqvia_pack_volume,
        iqvia_input_brand,
        mat_noofprescritions,
        mat_noofprescribers,
        field_rep_active,
        mat_totalprescritions_by_brand,
        mat_totalprescribers_by_brand,
        mat_totalprescritions_jnj_brand,
        totalprescritions_by_brand,
        totalprescribers_by_brand,
        totalprescritions_jnj_brand,
        call_type,
        email_subject,
        totalprescritions_by_speciality,
        totalprescribers_by_speciality,
        totalprescritions_jnj_speciality,
        totalprescritions_by_indication,
        totalprescribers_by_indication,
        totalprescritions_jnj_indication,
        year_month,
        devicecategory,
        channelgrouping,
        visitor_country,
        new_visitors,
        REPEAT_VISITORS,
        ALL_VISITOR,
        unique_pageviews,
        total_downloads,
        pages,
        page_sessions,
        total_page_views ,
        total_bounces,
        total_session_duration,
        sessions
    from trans
)
select * from final