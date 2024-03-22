with edw_vw_sfmc_consumer_master as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_consumer_master') }}
),
edw_vw_sfmc_children_data as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_children_data') }}
),
edw_vw_sfmc_email_activity_fact as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_email_activity_fact') }} 
),
itg_mds_rg_sfmc_gender as (
    select * from {{ ref('aspitg_integration__itg_mds_rg_sfmc_gender') }}
),
children as (
    SELECT 'CHILDREN' AS data_src,
    ifc.country_code,
    CASE
        WHEN (
            (
                (ifc.country_code)::text = ('TH'::varchar)::text
            )
            OR (
                (ifc.country_code IS NULL)
                AND ('TH' IS NULL)
            )
        ) THEN 'THAILAND'::varchar
        ELSE ifc.country_code
    END AS country_name,
    to_date(ifc.created_date) AS created_date,
    NULL AS invoicec_created_date,
    NULL AS invoice_completed_date,
    to_date(ifc.last_logon_time) AS last_logon_time,
    NULL AS purchase_date,
    NULL AS redeemed_date,
    NULL AS redeemption_completed_date,
    ifc.first_name,
    ifc.last_name,
    COALESCE(ifc.full_name,concat(coalesce(ifc.first_name,''),' ',coalesce(ifc.last_name,''))) AS consumer_name,
    ifc.mobile_number,
    ifc.subscriber_key,
    ifc.mobile_country_code,
    ifc.address_country,
    ifc.address_region,
    ifc.address_1,
    ifc.address_2,
    ifc.line_id,
    concat(COALESCE(ifc.address_zipcode, ''),' ',COALESCE(ifc.address_country, ''),' ',COALESCE(ifc.address_city, ''),' ',COALESCE(ifc.address_1, '')) AS address,
    CASE
        WHEN (ifc.birth_month IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.birth_month
    END AS birth_month,
    CASE
        WHEN (ifc.birth_year IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.birth_year
    END AS birth_year,
    ifc.consumer_age,
    CASE
        WHEN ((ifc.consumer_age)::text = 'UNDEFINED'::text) THEN 'UNDEFINED'::varchar
        WHEN ((ifc.consumer_age)::integer < 13) THEN '<13'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 13)
            AND ((ifc.consumer_age)::integer <= 18)
        ) THEN '13-18'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 19)
            AND ((ifc.consumer_age)::integer <= 29)
        ) THEN '19-29'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 30)
            AND ((ifc.consumer_age)::integer <= 39)
        ) THEN '30-39'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 40)
            AND ((ifc.consumer_age)::integer <= 49)
        ) THEN '40-49'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 50)
            AND ((ifc.consumer_age)::integer <= 59)
        ) THEN '50-59'::varchar
        ELSE '>=60'::varchar
    END AS consumer_age_group,
    ifc.email,
    COALESCE(
        CASE
            WHEN (
                (ifc.gender_standard)::text = (''::varchar)::text
            ) THEN NULL::varchar
            ELSE ifc.gender_standard
        END,
        'UNDEFINED'::varchar
    ) AS gender,
    NULL AS channel_name,
    ifc.brand,
    NULL AS product_name,
    NULL AS product_category,
    CASE
        WHEN (
            (ifc.tier IS NOT NULL)
            OR (
                (ifc.tier)::text <> (''::varchar)::text
            )
        ) THEN ifc.tier_standard
        ELSE 'UNDEFINED'::varchar
    END AS tier,
    ifc.remaining_points,
    ifc.redeemed_points AS customer_redeemed_points,
    ifc.total_points,
    NULL AS invoive_number,
    NULL AS invoice_qty,
    NULL AS invoice_type,
    NULL AS invoice_seller_name,
    NULL AS invoice_points,
    NULL AS invoice_status,
    isc.child_name,
    CASE
        WHEN (isc.child_birth_month IS NULL) THEN 'UNDEFINED'::varchar
        ELSE isc.child_birth_month
    END AS child_birth_month,
    CASE
        WHEN (isc.child_birth_year IS NULL) THEN 'UNDEFINED'::varchar
        ELSE isc.child_birth_year
    END AS child_birth_year,
    COALESCE(
        CASE
            WHEN (
                (isc.child_gender)::text = (''::varchar)::text
            ) THEN NULL::varchar
            ELSE isc.child_gender
        END,
        'UNDEFINED'::varchar
    ) AS child_gender,
    isc.child_number,
    isc.child_age,
    CASE
        WHEN ((isc.child_age)::text = 'UNDEFINED'::text) THEN 'UNDEFINED'::varchar
        WHEN (
            ((isc.child_age)::integer >= 0)
            AND ((isc.child_age)::integer < 1)
        ) THEN 'NEWBORN(>=0,<1 Yrs)'::varchar
        WHEN (
            ((isc.child_age)::integer >= 1)
            AND ((isc.child_age)::integer < 3)
        ) THEN 'TODDLER(>=1,<3 Yrs)'::varchar
        WHEN (
            ((isc.child_age)::integer >= 3)
            AND ((isc.child_age)::integer < 6)
        ) THEN 'KIDS(>=3,<6 Yrs)'::varchar
        WHEN ((isc.child_age)::integer >= 6) THEN 'FAMILY(>=6 Yrs)'::varchar
        ELSE 'UNDEFINED'::varchar
    END AS child_group,
    CASE
        WHEN (isc.parent_key IS NULL) THEN 'Without Kids'::varchar
        ELSE 'With Kids'::varchar
    END AS child_flag,
    NULL AS redeemed_order_number,
    NULL AS redeemed_prod_name,
    NULL AS redemption_status,
    NULL AS redeemed_points,
    NULL AS redeemed_qty,
    NULL AS activity_date,
    NULL AS local_activity_date,
    NULL AS sent_activity_date,
    NULL AS email_name,
    NULL AS email_subject,
    NULL AS sent_email_name,
    NULL AS sent_email_subject,
    NULL AS email_delivered_flag,
    NULL AS is_unique,
    NULL AS url,
    NULL AS link_name,
    NULL AS link_content,
    NULL AS bounce_reason,
    NULL AS bounce_type,
    NULL AS bounce_description,
    convert_timezone('Asia/Singapore', ifc.crtd_dttm) AS data_refresh_time,
    CASE
        WHEN (ifc.smoker IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.smoker
    END AS smoker,
    CASE
        WHEN (ifc.expectant_mother IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.expectant_mother
    END AS expectant_mother,
    CASE
        WHEN (ifc.category_they_are_using IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.category_they_are_using
    END AS category_they_are_using,
    CASE
        WHEN (ifc.category_they_are_using_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.category_they_are_using_eng
    END AS category_they_are_using_eng,
    CASE
        WHEN (ifc.skin_condition IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.skin_condition
    END AS skin_condition,
    CASE
        WHEN (ifc.skin_condition_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.skin_condition_eng
    END AS skin_condition_eng,
    CASE
        WHEN (ifc.skin_problem IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.skin_problem
    END AS skin_problem,
    CASE
        WHEN (ifc.skin_problem_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.skin_problem_eng
    END AS skin_problem_eng,
    CASE
        WHEN (ifc.use_mouthwash IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.use_mouthwash
    END AS use_mouthwash,
    CASE
        WHEN (ifc.mouthwash_time IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.mouthwash_time
    END AS mouthwash_time,
    CASE
        WHEN (ifc.mouthwash_time_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.mouthwash_time_eng
    END AS mouthwash_time_eng,
    CASE
        WHEN (ifc.why_not_use_mouthwash IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.why_not_use_mouthwash
    END AS why_not_use_mouthwash,
    CASE
        WHEN (ifc.why_not_use_mouthwash_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.why_not_use_mouthwash_eng
    END AS why_not_use_mouthwash_eng,
    CASE
        WHEN (ifc.oral_problem IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.oral_problem
    END AS oral_problem,
    CASE
        WHEN (ifc.oral_problem_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.oral_problem_eng
    END AS oral_problem_eng
FROM (
        (
            SELECT ifc.country_code,
                ifc.first_name,
                ifc.last_name,
                ifc.mobile_number,
                ifc.mobile_country_code,
                ifc.birth_month,
                ifc.birth_year,
                ifc.address_1,
                ifc.address_2,
                ifc.address_city,
                ifc.address_zipcode,
                ifc.subscriber_key,
                ifc.website_unique_id,
                ifc.source,
                ifc.medium,
                ifc.brand,
                ifc.address_country,
                ifc.campaign_id,
                ifc.created_date,
                ifc.updated_date,
                ifc.unsubscribe_date,
                ifc.email,
                ifc.full_name,
                ifc.last_logon_time,
                ifc.remaining_points,
                ifc.redeemed_points,
                ifc.total_points,
                ifc.gender,
                ifc.line_id,
                ifc.line_name,
                ifc.line_email,
                ifc.line_channel_id,
                ifc.address_region,
                ifc.tier,
                ifc.opt_in_for_communication,
                ifc.crtd_dttm,
                ifc.tier AS tier_standard,
                gen.gender_standard,
                ifc.age,
                ifc.smoker,
                ifc.expectant_mother,
                ifc.category_they_are_using,
                ifc.category_they_are_using_eng,
                ifc.skin_condition,
                ifc.skin_condition_eng,
                ifc.skin_problem,
                ifc.skin_problem_eng,
                ifc.use_mouthwash,
                ifc.mouthwash_time,
                ifc.mouthwash_time_eng,
                ifc.why_not_use_mouthwash,
                ifc.why_not_use_mouthwash_eng,
                ifc.oral_problem,
                ifc.oral_problem_eng,
                CASE
                    WHEN (
                        (ifc.birth_year IS NOT NULL)
                        AND ((ifc.birth_year)::text <> '0'::text)
                    ) THEN (
                        (
                            (date_part(year, current_timestamp()::date))::numeric - (ifc.birth_year)::numeric
                        )
                    )::varchar
                    WHEN (
                        (ifc.age IS NOT NULL)
                        AND (ifc.birth_year IS NULL)
                    ) THEN (
                        (
                            ifc.age + (
                                (date_part(year, current_timestamp()::date)) - date_part(year, ifc.created_date)
                            )
                        )
                    )::varchar
                    ELSE 'UNDEFINED'::varchar
                END AS consumer_age
            FROM (
                    edw_vw_sfmc_consumer_master ifc
                    LEFT JOIN itg_mds_rg_sfmc_gender gen ON (
                        (
                            upper(trim((gen.gender_raw)::text)) = upper(trim((ifc.gender)::text))
                        )
                    )
                )
        ) ifc
        LEFT JOIN (
            SELECT a.country_code,
                a.parent_key,
                a.child_name,
                a.child_birth_month,
                a.child_birth_year,
                a.child_gender,
                a.child_number,
                CASE
                    WHEN (
                        (a.child_birth_year IS NULL)
                        OR ((a.child_birth_year)::text = '0'::text)
                    ) THEN 'UNDEFINED'::varchar
                    ELSE (
                        (
                            (date_part(year, current_timestamp()::date))::numeric - (a.child_birth_year)::numeric
                        )
                    )::varchar
                END AS child_age
            FROM edw_vw_sfmc_children_data a
            WHERE ((a.country_code)::text = 'TH'::text)
        ) isc ON (
            (
                (
                    trim(upper((ifc.subscriber_key)::text)) = trim(upper((isc.parent_key)::text))
                )
                AND (
                    trim((ifc.country_code)::text) = trim((isc.country_code)::text)
                )
            )
        )
    )
WHERE ((ifc.country_code)::text = 'TH'::text)
),
other as (
    SELECT sent.activity_type AS data_src,
    sent.country_key AS country_code,
    CASE
        WHEN (
            (
                (ifc.country_code)::text = ('TH'::varchar)::text
            )
            OR (
                (ifc.country_code IS NULL)
                AND ('TH' IS NULL)
            )
        ) THEN 'THAILAND'::varchar
        ELSE ifc.country_code
    END AS country_name,
    to_date(ifc.created_date) AS created_date,
    NULL AS invoicec_created_date,
    NULL AS invoice_completed_date,
    NULL AS last_logon_time,
    NULL AS purchase_date,
    NULL AS redeemed_date,
    NULL AS redeemption_completed_date,
    ifc.first_name,
    ifc.last_name,
    COALESCE(ifc.full_name,concat(coalesce(ifc.first_name,''),' ',coalesce(ifc.last_name,''))) AS consumer_name,
    ifc.mobile_number,
    sent.subscriber_key,
    ifc.mobile_country_code,
    ifc.address_country,
    ifc.address_region,
    ifc.address_1,
    ifc.address_2,
    ifc.line_id,
    concat(COALESCE(ifc.address_zipcode, ''),' ',COALESCE(ifc.address_country, ''),' ',COALESCE(ifc.address_city, ''),' ',COALESCE(ifc.address_1, '')) AS address,
    CASE
        WHEN (ifc.birth_month IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.birth_month
    END AS birth_month,
    CASE
        WHEN (ifc.birth_year IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.birth_year
    END AS birth_year,
    ifc.consumer_age,
    
    CASE
        WHEN ((ifc.consumer_age)::text = 'UNDEFINED'::text) THEN 'UNDEFINED'::varchar
        WHEN ((ifc.consumer_age)::integer < 13) THEN '<13'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 13)
            AND ((ifc.consumer_age)::integer <= 18)
        ) THEN '13-18'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 19)
            AND ((ifc.consumer_age)::integer <= 29)
        ) THEN '19-29'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 30)
            AND ((ifc.consumer_age)::integer <= 39)
        ) THEN '30-39'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 40)
            AND ((ifc.consumer_age)::integer <= 49)
        ) THEN '40-49'::varchar
        WHEN (
            ((ifc.consumer_age)::integer >= 50)
            AND ((ifc.consumer_age)::integer <= 59)
        ) THEN '50-59'::varchar
        ELSE '>=60'::varchar
    END AS consumer_age_group,
    ifc.email,
    COALESCE(
        CASE
            WHEN (
                (ifc.gender_standard)::text = (''::varchar)::text
            ) THEN NULL::varchar
            ELSE ifc.gender_standard
        END,
        'UNDEFINED'::varchar
    ) AS gender,
    NULL AS channel_name,
    ifc.brand,
    NULL AS product_name,
    NULL AS product_category,
    CASE
        WHEN (
            (ifc.tier IS NOT NULL)
            OR (
                (ifc.tier)::text <> (''::varchar)::text
            )
        ) THEN ifc.tier_standard
        ELSE 'UNDEFINED'::varchar
    END AS tier,
    ifc.remaining_points,
    ifc.redeemed_points AS customer_redeemed_points,
    ifc.total_points,
    NULL AS invoive_number,
    NULL AS invoice_qty,
    NULL AS invoice_type,
    NULL AS invoice_seller_name,
    NULL AS invoice_points,
    NULL AS invoice_status,
    NULL AS child_name,
    NULL AS child_birth_month,
    NULL AS child_birth_year,
    NULL AS child_gender,
    NULL AS child_number,
    NULL AS child_age,
    NULL AS child_group,
    NULL AS child_flag,
    NULL AS redeemed_order_number,
    NULL AS redeemed_prod_name,
    NULL AS redemption_status,
    NULL AS redeemed_points,
    NULL AS redeemed_qty,
    sent.activity_date,
    sent.local_timezone_activity_date AS local_activity_date,
    sent.sent_activity_date,
    sent.email_name,
    sent.email_subject,
    sent.sent_email_name,
    sent.sent_email_subject,
    sent.email_delivered_flag,
    sent.is_unique,
    sent.url,
    sent.link_name,
    sent.link_content,
    sent.bounce_reason,
    sent.bounce_type,
    sent.bounce_description,
    convert_timezone('Asia/Singapore', ifc.crtd_dttm) AS data_refresh_time,
    CASE
        WHEN (ifc.smoker IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.smoker
    END AS smoker,
    CASE
        WHEN (ifc.expectant_mother IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.expectant_mother
    END AS expectant_mother,
    CASE
        WHEN (ifc.category_they_are_using IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.category_they_are_using
    END AS category_they_are_using,
    CASE
        WHEN (ifc.category_they_are_using_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.category_they_are_using_eng
    END AS category_they_are_using_eng,
    CASE
        WHEN (ifc.skin_condition IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.skin_condition
    END AS skin_condition,
    CASE
        WHEN (ifc.skin_condition_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.skin_condition_eng
    END AS skin_condition_eng,
    CASE
        WHEN (ifc.skin_problem IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.skin_problem
    END AS skin_problem,
    CASE
        WHEN (ifc.skin_problem_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.skin_problem_eng
    END AS skin_problem_eng,
    CASE
        WHEN (ifc.use_mouthwash IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.use_mouthwash
    END AS use_mouthwash,
    CASE
        WHEN (ifc.mouthwash_time IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.mouthwash_time
    END AS mouthwash_time,
    CASE
        WHEN (ifc.mouthwash_time_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.mouthwash_time_eng
    END AS mouthwash_time_eng,
    CASE
        WHEN (ifc.why_not_use_mouthwash IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.why_not_use_mouthwash
    END AS why_not_use_mouthwash,
    CASE
        WHEN (ifc.why_not_use_mouthwash_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.why_not_use_mouthwash_eng
    END AS why_not_use_mouthwash_eng,
    CASE
        WHEN (ifc.oral_problem IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.oral_problem
    END AS oral_problem,
    CASE
        WHEN (ifc.oral_problem_eng IS NULL) THEN 'UNDEFINED'::varchar
        ELSE ifc.oral_problem_eng
    END AS oral_problem_eng
FROM (
        edw_vw_sfmc_email_activity_fact sent
        LEFT JOIN (
            SELECT ifc.country_code,
                ifc.first_name,
                ifc.last_name,
                ifc.mobile_number,
                ifc.mobile_country_code,
                ifc.birth_month,
                ifc.birth_year,
                ifc.address_1,
                ifc.address_2,
                ifc.address_city,
                ifc.address_zipcode,
                ifc.subscriber_key,
                ifc.website_unique_id,
                ifc.source,
                ifc.medium,
                ifc.brand,
                ifc.address_country,
                ifc.campaign_id,
                ifc.created_date,
                ifc.updated_date,
                ifc.unsubscribe_date,
                ifc.email,
                ifc.full_name,
                ifc.last_logon_time,
                ifc.remaining_points,
                ifc.redeemed_points,
                ifc.total_points,
                ifc.gender,
                ifc.line_id,
                ifc.line_name,
                ifc.line_email,
                ifc.line_channel_id,
                ifc.address_region,
                ifc.tier,
                ifc.opt_in_for_communication,
                ifc.crtd_dttm,
                NULL::varchar AS tier_standard,
                gen.gender_standard,
                ifc.age,
                ifc.smoker,
                ifc.expectant_mother,
                ifc.category_they_are_using,
                ifc.category_they_are_using_eng,
                ifc.skin_condition,
                ifc.skin_condition_eng,
                ifc.skin_problem,
                ifc.skin_problem_eng,
                ifc.use_mouthwash,
                ifc.mouthwash_time,
                ifc.mouthwash_time_eng,
                ifc.why_not_use_mouthwash,
                ifc.why_not_use_mouthwash_eng,
                ifc.oral_problem,
                ifc.oral_problem_eng,
                CASE
                    WHEN (
                        (ifc.birth_year IS NOT NULL)
                        AND ((ifc.birth_year)::text <> '0'::text)
                    ) THEN (
                        (
                            (date_part(year, current_timestamp()::date))::numeric - (ifc.birth_year)::numeric
                        )
                    )::varchar
                    WHEN (
                        (ifc.age IS NOT NULL)
                        AND (ifc.birth_year IS NULL)
                    ) THEN (
                        (
                            ifc.age + (
                                (date_part(year, current_timestamp()::date)) - date_part(year, ifc.created_date)
                            )
                        )
                    )::varchar
                    ELSE 'UNDEFINED'::varchar
                END AS consumer_age
            FROM (
                    edw_vw_sfmc_consumer_master ifc
                    LEFT JOIN itg_mds_rg_sfmc_gender gen ON (
                        (
                            upper(trim((gen.gender_raw)::text)) = upper(trim((ifc.gender)::text))
                        )
                    )
                )
        ) ifc ON (
            (
                (
                    trim(upper((ifc.subscriber_key)::text)) = upper(trim((sent.subscriber_key)::text))
                )
                AND (
                    trim((ifc.country_code)::text) = trim((sent.country_key)::text)
                )
            )
        )
    )
WHERE (
        ((sent.country_key)::text = 'TH'::text)
        AND ((ifc.country_code)::text = 'TH'::text)
    )
),
final as (
    select * from children
    union all
    select * from other
)
select * from final