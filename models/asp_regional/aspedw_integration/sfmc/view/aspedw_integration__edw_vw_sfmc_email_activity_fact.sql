with itg_sfmc_sent_data as (
    select * from {{ ref('aspitg_integration__itg_sfmc_sent_data') }}
),
itg_sfmc_unsubscribe_data as (
    select * from {{ ref('aspitg_integration__itg_sfmc_unsubscribe_data') }}
),
itg_sfmc_bounce_data as (
    select * from {{ ref('aspitg_integration__itg_sfmc_bounce_data') }}
),
itg_sfmc_open_data as (
    select * from {{ ref('aspitg_integration__itg_sfmc_open_data') }}
),
itg_sfmc_click_data as (
    select * from {{ ref('aspitg_integration__itg_sfmc_click_data') }}
),
sent as(
    SELECT 
        'SENT' AS activity_type,
        sent.cntry_cd AS country_key,
        sent.subscriber_key,
        sent.job_id,
        sent.event_date AS activity_date,
        convert_timezone(
            ('America/Chicago'::character varying)::text,
            ('Asia/Singapore'::character varying)::text,
            sent.event_date
        ) AS local_timezone_activity_date,
        sent.event_date AS sent_activity_date,
        sent.email_id,
        sent.email_name,
        sent.email_subject,
        sent.email_name AS sent_email_name,
        (trim((sent.email_subject)::text))::character varying AS sent_email_subject,
        NULL AS email_delivered_flag,
        NULL AS is_unique,
        NULL AS url,
        NULL AS link_name,
        NULL AS link_content,
        NULL AS bounce_reason,
        NULL AS bounce_type,
        NULL AS bounce_description
    FROM itg_sfmc_sent_data sent
),
bounce as (
    SELECT 'BOUNCE' AS activity_type,
                sent.cntry_cd AS country_key,
                sent.subscriber_key,
                sent.job_id,
                bounce.event_date AS activity_date,
                convert_timezone(
                    ('America/Chicago'::character varying)::text,
                    ('Asia/Singapore'::character varying)::text,
                    bounce.event_date
                ) AS local_timezone_activity_date,
                sent.event_date AS sent_activity_date,
                sent.email_id,
                bounce.email_name,
                bounce.email_subject,
                sent.email_name AS sent_email_name,
                (trim((sent.email_subject)::text))::character varying AS sent_email_subject,
                CASE
                    WHEN (bounce.subscriber_key IS NULL) THEN 'Successful'::character varying
                    ELSE 'Failed'::character varying
                END AS email_delivered_flag,
                NULL AS is_unique,
                NULL AS url,
                NULL AS link_name,
                NULL AS link_content,
                bounce.bounce_subcategory AS bounce_reason,
                bounce.bounce_category AS bounce_type,
                bounce.smtp_bounce_reason AS bounce_description
            FROM (
                    itg_sfmc_sent_data sent
                    LEFT JOIN itg_sfmc_bounce_data bounce ON (
                        (
                            (
                                (
                                    (
                                        (
                                            upper((sent.subscriber_key)::text) = upper((bounce.subscriber_key)::text)
                                        )
                                        AND (
                                            (sent.email_name)::text = (bounce.email_name)::text
                                        )
                                    )
                                    AND ((sent.job_id)::text = (bounce.job_id)::text)
                                )
                                AND ((sent.batch_id)::text = (bounce.batch_id)::text)
                            )
                            AND ((sent.cntry_cd)::text = (bounce.cntry_cd)::text)
                        )
                    )
                )
),
open as (
    SELECT 'OPEN' AS activity_type,
            o.cntry_cd AS country_key,
            o.subscriber_key,
            o.job_id,
            o.event_date AS activity_date,
            convert_timezone(
                ('America/Chicago'::character varying)::text,
                ('Asia/Singapore'::character varying)::text,
                o.event_date
            ) AS local_timezone_activity_date,
            sent.event_date AS sent_activity_date,
            o.email_id,
            o.email_name,
            o.email_subject,
            sent.email_name AS sent_email_name,
            (trim((sent.email_subject)::text))::character varying AS sent_email_subject,
            NULL AS email_delivered_flag,
            o.is_unique,
            NULL AS url,
            NULL AS link_name,
            NULL AS link_content,
            NULL AS bounce_reason,
            NULL AS bounce_type,
            NULL AS bounce_description
        FROM (
                itg_sfmc_open_data o
                LEFT JOIN itg_sfmc_sent_data sent ON (
                    (
                        (
                            (
                                (
                                    (
                                        upper((sent.subscriber_key)::text) = upper((o.subscriber_key)::text)
                                    )
                                    AND ((sent.email_name)::text = (o.email_name)::text)
                                )
                                AND ((sent.job_id)::text = (o.job_id)::text)
                            )
                            AND ((sent.batch_id)::text = (o.batch_id)::text)
                        )
                        AND ((sent.cntry_cd)::text = (o.cntry_cd)::text)
                    )
                )
            )
),
click as (
    SELECT 'CLICK' AS activity_type,
        click.cntry_cd AS country_key,
        click.subscriber_key,
        click.job_id,
        click.event_date AS activity_date,
        convert_timezone(
            ('America/Chicago'::character varying)::text,
            ('Asia/Singapore'::character varying)::text,
            click.event_date
        ) AS local_timezone_activity_date,
        sent.event_date AS sent_activity_date,
        NULL AS email_id,
        click.email_name,
        click.email_subject,
        sent.email_name AS sent_email_name,
        (trim((sent.email_subject)::text))::character varying AS sent_email_subject,
        NULL AS email_delivered_flag,
        click.is_unique,
        click.url,
        click.link_name,
        click.link_content,
        NULL AS bounce_reason,
        NULL AS bounce_type,
        NULL AS bounce_description
    FROM (
            itg_sfmc_click_data click
            LEFT JOIN itg_sfmc_sent_data sent ON (
                (
                    (
                        (
                            (
                                (
                                    upper((sent.subscriber_key)::text) = upper((click.subscriber_key)::text)
                                )
                                AND (
                                    (sent.email_name)::text = (click.email_name)::text
                                )
                            )
                            AND ((sent.job_id)::text = (click.job_id)::text)
                        )
                        AND ((sent.batch_id)::text = (click.batch_id)::text)
                    )
                    AND ((sent.cntry_cd)::text = (click.cntry_cd)::text)
                )
            )
        )
),
unsubscribe as (
    SELECT 'UNSUBSCRIBE' AS activity_type,
    unsub.cntry_cd AS country_key,
    unsub.subscriber_key,
    unsub.job_id,
    unsub.event_date AS activity_date,
    convert_timezone(
        ('America/Chicago'::character varying)::text,
        ('Asia/Singapore'::character varying)::text,
        unsub.event_date
    ) AS local_timezone_activity_date,
    sent.event_date AS sent_activity_date,
    unsub.email_id,
    unsub.email_name,
    unsub.email_subject,
    sent.email_name AS sent_email_name,
    (trim((sent.email_subject)::text))::character varying AS sent_email_subject,
    NULL AS email_delivered_flag,
    NULL AS is_unique,
    NULL AS url,
    NULL AS link_name,
    NULL AS link_content,
    NULL AS bounce_reason,
    NULL AS bounce_type,
    NULL AS bounce_description
    FROM (
            itg_sfmc_unsubscribe_data unsub
            LEFT JOIN itg_sfmc_sent_data sent ON (
                (
                    (
                        (
                            (
                                (
                                    upper((sent.subscriber_key)::text) = upper((unsub.subscriber_key)::text)
                                )
                                AND (
                                    (sent.email_name)::text = (unsub.email_name)::text
                                )
                            )
                            AND ((sent.job_id)::text = (unsub.job_id)::text)
                        )
                        AND ((sent.batch_id)::text = (unsub.batch_id)::text)
                    )
                    AND ((sent.cntry_cd)::text = (unsub.cntry_cd)::text)
                )
            )
        )
),
final as (
    select * from sent
    union all
    select * from bounce
    union all
    select * from open
    union all
    select * from click
    union all
    select * from unsubscribe
)
select * from final