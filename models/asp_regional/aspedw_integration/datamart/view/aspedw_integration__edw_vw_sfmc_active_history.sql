with itg_sfmc_consumer_master as (
    select * from {{ ref('aspitg_integration__itg_sfmc_consumer_master') }}
),

logon_month as 
(
    SELECT 
        itg_sfmc_consumer_master.subscriber_key,
        itg_sfmc_consumer_master.cntry_cd,
        itg_sfmc_consumer_master.last_logon_time,
        (
            to_char(
                (
                    to_date(
                        add_months(
                            itg_sfmc_consumer_master.last_logon_time,
                            (0)::bigint
                        )
                    )
                )::timestamp without time zone,
                'YYYYMM'::text
            )
        )::character varying AS active_month,
        to_date(itg_sfmc_consumer_master.crtd_dttm) AS file_dt
    FROM itg_sfmc_consumer_master
    WHERE (
            (
                (itg_sfmc_consumer_master.cntry_cd)::text = 'TW'::text
            )
            AND (
                itg_sfmc_consumer_master.last_logon_time IS NOT NULL
            )
        )
                            
),
month_1 as 
(
    SELECT 
        itg_sfmc_consumer_master.subscriber_key,
        itg_sfmc_consumer_master.cntry_cd,
        itg_sfmc_consumer_master.last_logon_time,
        (
            to_char(
                (
                    to_date(
                        add_months(
                            itg_sfmc_consumer_master.last_logon_time,
                            (1)::bigint
                        )
                    )
                )::timestamp without time zone,
                'YYYYMM'::text
            )
        )::character varying AS active_month,
        to_date(itg_sfmc_consumer_master.crtd_dttm) AS file_dt
    FROM itg_sfmc_consumer_master
    WHERE (
            (
                (itg_sfmc_consumer_master.cntry_cd)::text = 'TW'::text
            )
            AND (
                itg_sfmc_consumer_master.last_logon_time IS NOT NULL
            )
        )
                            
),
month_2 as 
(
    SELECT 
        itg_sfmc_consumer_master.subscriber_key,
        itg_sfmc_consumer_master.cntry_cd,
        itg_sfmc_consumer_master.last_logon_time,
        (
            to_char(
                (
                    to_date(
                        add_months(
                            itg_sfmc_consumer_master.last_logon_time,
                            (2)::bigint
                        )
                    )
                )::timestamp without time zone,
                'YYYYMM'::text
            )
        )::character varying AS active_month,
        to_date(itg_sfmc_consumer_master.crtd_dttm) AS file_dt
    FROM itg_sfmc_consumer_master
    WHERE (
            (
                (itg_sfmc_consumer_master.cntry_cd)::text = 'TW'::text
            )
            AND (
                itg_sfmc_consumer_master.last_logon_time IS NOT NULL
            )
        )
                            
),
month_3 as 
(
    SELECT 
        itg_sfmc_consumer_master.subscriber_key,
        itg_sfmc_consumer_master.cntry_cd,
        itg_sfmc_consumer_master.last_logon_time,
        (
            to_char(
                (
                    to_date(
                        add_months(
                            itg_sfmc_consumer_master.last_logon_time,
                            (3)::bigint
                        )
                    )
                )::timestamp without time zone,
                'YYYYMM'::text
            )
        )::character varying AS active_month,
        to_date(itg_sfmc_consumer_master.crtd_dttm) AS file_dt
    FROM itg_sfmc_consumer_master
    WHERE (
            (
                (itg_sfmc_consumer_master.cntry_cd)::text = 'TW'::text
            )
            AND (
                itg_sfmc_consumer_master.last_logon_time IS NOT NULL
            )
        )
                            
),
month_4 as 
(
    SELECT 
        itg_sfmc_consumer_master.subscriber_key,
        itg_sfmc_consumer_master.cntry_cd,
        itg_sfmc_consumer_master.last_logon_time,
        (
            to_char(
                (
                    to_date(
                        add_months(
                            itg_sfmc_consumer_master.last_logon_time,
                            (4)::bigint
                        )
                    )
                )::timestamp without time zone,
                'YYYYMM'::text
            )
        )::character varying AS active_month,
        to_date(itg_sfmc_consumer_master.crtd_dttm) AS file_dt
    FROM itg_sfmc_consumer_master
    WHERE (
            (
                (itg_sfmc_consumer_master.cntry_cd)::text = 'TW'::text
            )
            AND (
                itg_sfmc_consumer_master.last_logon_time IS NOT NULL
            )
        )
                            
),
month_5 as 
(
    SELECT 
        itg_sfmc_consumer_master.subscriber_key,
        itg_sfmc_consumer_master.cntry_cd,
        itg_sfmc_consumer_master.last_logon_time,
        (
            to_char(
                (
                    to_date(
                        add_months(
                            itg_sfmc_consumer_master.last_logon_time,
                            (5)::bigint
                        )
                    )
                )::timestamp without time zone,
                'YYYYMM'::text
            )
        )::character varying AS active_month,
        to_date(itg_sfmc_consumer_master.crtd_dttm) AS file_dt
    FROM itg_sfmc_consumer_master
    WHERE (
            (
                (itg_sfmc_consumer_master.cntry_cd)::text = 'TW'::text
            )
            AND (
                itg_sfmc_consumer_master.last_logon_time IS NOT NULL
            )
        )
                            
),
final as
(    
    SELECT 
        derived_table1.cntry_cd,
        derived_table1.subscriber_key,
        derived_table1.last_logon_time,
        derived_table1.active_month AS active_year_mnth,
        'Y' AS active_flag,
        derived_table1.file_dt
    FROM 
    (
        SELECT 
            abc.subscriber_key,
            abc.cntry_cd,
            abc.last_logon_time,
            abc.active_month,
            abc.file_dt,
            row_number() OVER( PARTITION BY abc.subscriber_key,abc.active_month ORDER BY abc.subscriber_key,abc.active_month
            ) AS rno
        FROM 
            (      
                select * from logon_month
                union
                select * from month_1
                UNION
                select * from month_2
                UNION
                select * from month_3
                UNION
                select * from month_4            
                UNION
                select * from month_5
            ) abc
    ) derived_table1
    WHERE (derived_table1.rno = 1)
)
select * from final