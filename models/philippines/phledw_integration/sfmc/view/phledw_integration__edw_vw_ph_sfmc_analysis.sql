with
edw_vw_ph_sfmc_consumer_master as
(
    select * from {{ ref('aspedw_integration__edw_vw_ph_sfmc_consumer_master') }}
),
itg_mds_rg_sfmc_gender as
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_sfmc_gender') }}
),
edw_vw_sfmc_children_data as
(
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_children_data') }}
),
edw_vw_sfmc_email_activity_fact as
(
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_email_activity_fact') }}
),
edw_vw_ph_sfmc_subscriber_status_history as
(
    select * from  {{ ref('aspedw_integration__edw_vw_ph_sfmc_subscriber_status_history') }}
),
temp_a as
(
    SELECT 'CHILDREN' AS data_src,
        ifc.country_code,
        CASE
            WHEN (
                (ifc.country_code)::text = ('PH'::character varying)::text
            ) THEN 'PHILIPPINES'::character varying
            ELSE ifc.country_code
        END AS country_name,
        (ifc.created_date) AS created_date,
        NULL AS invoicec_created_date,
        NULL AS invoice_completed_date,
        (ifc.last_logon_time) AS last_logon_time,
        NULL AS purchase_date,
        NULL AS redeemed_date,
        NULL AS redeemption_completed_date,
        ifc.first_name,
        ifc.last_name,
        COALESCE(
            ifc.full_name,
            (
                (
                    (
                        (COALESCE(ifc.first_name, ''::character varying))::text || (' '::character varying)::text
                    ) || (COALESCE(ifc.last_name, ''::character varying))::text
                )
            )::character varying
        ) AS consumer_name,
        ifc.mobile_number,
        ifc.subscriber_key,
        ifc.mobile_country_code,
        ifc.address_country,
        ifc.address_region,
        ifc.address_1,
        ifc.address_2,
        ifc.line_id,
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        COALESCE(ifc.address_zipcode, ''::character varying)
                                    )::text || (' '::character varying)::text
                                ) || (
                                    COALESCE(ifc.address_country, ''::character varying)
                                )::text
                            ) || (' '::character varying)::text
                        ) || (
                            COALESCE(ifc.address_city, ''::character varying)
                        )::text
                    ) || (' '::character varying)::text
                ) || (COALESCE(ifc.address_1, ''::character varying))::text
            )
        )::character varying AS address,
        CASE
            WHEN (
                (ifc.birth_month IS NULL)
                OR (
                    (ifc.birth_month)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            ELSE ifc.birth_month
        END AS birth_month,
        CASE
            WHEN (
                (ifc.birth_year IS NULL)
                OR (
                    (ifc.birth_year)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            ELSE ifc.birth_year
        END AS birth_year,
        ifc.consumer_age,
        CASE
            WHEN (
                (
                    (
                        (ifc.consumer_age)::text = ('UNDEFINED'::character varying)::text
                    )
                    OR (ifc.consumer_age IS NULL)
                )
                OR (
                    (ifc.consumer_age)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                (ifc.consumer_age)::numeric(18, 0) < ((0)::numeric)::numeric(18, 0)
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((0)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((17)::numeric)::numeric(18, 0)
                )
            ) THEN '<=17'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((18)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((24)::numeric)::numeric(18, 0)
                )
            ) THEN '18-24'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((25)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((34)::numeric)::numeric(18, 0)
                )
            ) THEN '25-34'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((35)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((44)::numeric)::numeric(18, 0)
                )
            ) THEN '35-44'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((45)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((54)::numeric)::numeric(18, 0)
                )
            ) THEN '45-54'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((55)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((64)::numeric)::numeric(18, 0)
                )
            ) THEN '55-64'::character varying
            ELSE '>=65'::character varying
        END AS consumer_age_group,
        ifc.email,
        COALESCE(
            CASE
                WHEN (
                    (ifc.gender_standard)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE ifc.gender_standard
            END,
            'UNDEFINED'::character varying
        ) AS gender,
        NULL AS channel_name,
        ifc.brand,
        NULL AS product_name,
        NULL AS product_category,
        CASE
            WHEN (
                (ifc.tier IS NOT NULL)
                OR (
                    (ifc.tier)::text <> (''::character varying)::text
                )
            ) THEN ifc.tier_standard
            ELSE 'UNDEFINED'::character varying
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
            WHEN (
                (isc.child_birth_month IS NULL)
                OR (
                    (isc.child_birth_month)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            ELSE isc.child_birth_month
        END AS child_birth_month,
        CASE
            WHEN (
                (isc.child_birth_year IS NULL)
                OR (
                    (isc.child_birth_year)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            ELSE isc.child_birth_year
        END AS child_birth_year,
        isc.child_gender,
        isc.child_number,
        isc.child_age,
        CASE
            WHEN (
                (
                    (
                        (isc.child_age)::text = ('UNDEFINED'::character varying)::text
                    )
                    OR (isc.child_age IS NULL)
                )
                OR (
                    (isc.child_age)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                (
                    (isc.child_age)::numeric(18, 0) >= ((0)::numeric)::numeric(18, 0)
                )
                AND (
                    (isc.child_age)::numeric(18, 0) < ((1)::numeric)::numeric(18, 0)
                )
            ) THEN '0-11 Months'::character varying
            WHEN (
                (
                    (isc.child_age)::numeric(18, 0) >= ((1)::numeric)::numeric(18, 0)
                )
                AND (
                    (isc.child_age)::numeric(18, 0) < ((3)::numeric)::numeric(18, 0)
                )
            ) THEN '>=1,<3 Yrs'::character varying
            WHEN (
                (
                    (isc.child_age)::numeric(18, 0) >= ((3)::numeric)::numeric(18, 0)
                )
                AND (
                    (isc.child_age)::numeric(18, 0) < ((7)::numeric)::numeric(18, 0)
                )
            ) THEN '>=3,<6 Yrs'::character varying
            WHEN (
                (
                    (isc.child_age)::numeric(18, 0) >= ((7)::numeric)::numeric(18, 0)
                )
                AND (
                    (isc.child_age)::numeric(18, 0) < ((12)::numeric)::numeric(18, 0)
                )
            ) THEN '>=7,<12 Yrs'::character varying
            WHEN (
                (isc.child_age)::numeric(18, 0) >= ((12)::numeric)::numeric(18, 0)
            ) THEN '>=12 Yrs'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS child_group,
        CASE
            WHEN (isc.parent_key IS NULL) THEN 'Without Kids'::character varying
            ELSE 'With Kids'::character varying
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
        current_timestamp() AS data_refresh_time,
        ifc.subscriber_status,
        ifc.opt_in_for_jnj_communication,
        ifc.opt_in_for_campaign,
        ifc.viber_engaged
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
                    ifc.subscriber_status,
                    ifc.opt_in_for_jnj_communication,
                    ifc.opt_in_for_campaign,
                    ifc.viber_engaged,
                    CASE
                        WHEN (
                            (
                                (
                                    (ifc.birth_year IS NOT NULL)
                                    AND (length((ifc.birth_year)::text) = 4)
                                )
                                AND (
                                    (ifc.birth_year)::text > ('1800'::character varying)::text
                                )
                            )
                            AND (ifc.birth_month IS NOT NULL)
                        ) THEN (
                            (
                                (
                                    (
                                        datediff(
                                            month,
                                            (
                                                to_date((
                                                    (
                                                        (ifc.birth_month)::text || ('-01-'::character varying)::text
                                                    ) || (ifc.birth_year)::text
                                                ),'mm-dd-yyyy')
                                            )::timestamp without time zone,
                                            (
                                                to_date((
                                                    (
                                                        (
                                                            (
                                                                date_part(
                                                                    month,
                                                                    current_timestamp()::date
                                                                )
                                                            )::character varying
                                                        )::text || ('-01-'::character varying)::text
                                                    ) || (
                                                        (
                                                            date_part(
                                                                year,
                                                                current_timestamp()::date
                                                            )
                                                        )::character varying
                                                    )::text
                                                ),'mm-dd-yyyy')
                                            )::timestamp without time zone
                                        )
                                    )::numeric
                                )::numeric(18, 0) / ((12)::character varying)::numeric(18, 0)
                            )
                        )::character varying
                        ELSE CASE
                            WHEN (
                                (
                                    (
                                        (ifc.birth_year IS NOT NULL)
                                        AND (length((ifc.birth_year)::text) = 4)
                                    )
                                    AND (
                                        (ifc.birth_year)::text > ('1800'::character varying)::text
                                    )
                                )
                                AND (ifc.birth_month IS NULL)
                            ) THEN (
                                (
                                    (
                                        (
                                            datediff(
                                                month,
                                                (
                                                    (
                                                        (ifc.birth_year)::text|| ('-01-01'::character varying)::text 
                                                    )
                                                )::timestamp without time zone,
                                                (
                                                    to_date((
                                                        (
                                                            (
                                                                (
                                                                    date_part(
                                                                        month,
                                                                        current_timestamp()::date
                                                                    )
                                                                )::character varying
                                                            )::text || ('-01-'::character varying)::text
                                                        ) || (
                                                            (
                                                                date_part(
                                                                    year,
                                                                    current_timestamp()::date
                                                                )
                                                            )::character varying
                                                        )::text
                                                    ),'mm-dd-yyyy')
                                                )::timestamp without time zone
                                            )
                                        )::numeric
                                    )::numeric(18, 0) / ((12)::character varying)::numeric(18, 0)
                                )
                            )::character varying
                            ELSE 'UNDEFINED'::character varying
                        END
                    END AS consumer_age
                FROM (
                        edw_vw_ph_sfmc_consumer_master ifc
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
                    a.child_number,
                    CASE
                        WHEN (
                            (
                                (
                                    (a.child_birth_year IS NOT NULL)
                                    AND (length((a.child_birth_year)::text) = 4)
                                )
                                AND (
                                    (a.child_birth_year)::text > ('1800'::character varying)::text
                                )
                            )
                            AND (a.child_birth_month IS NOT NULL)
                        ) THEN (
                            (
                                (
                                    (
                                        datediff(
                                            month,
                                            (
                                                to_date((
                                                    (
                                                        (a.child_birth_month)::text || ('-01-'::character varying)::text
                                                    ) || (a.child_birth_year)::text
                                                ),'mm-dd-yyyy')
                                            )::timestamp without time zone,
                                            (
                                                to_date((
                                                    (
                                                        (
                                                            (
                                                                date_part(
                                                                    month,
                                                                    current_timestamp()::date
                                                                )
                                                            )::character varying
                                                        )::text || ('-01-'::character varying)::text
                                                    ) || (
                                                        (
                                                            date_part(
                                                                year,
                                                                current_timestamp()::date
                                                            )
                                                        )::character varying
                                                    )::text
                                                ),'mm-dd-yyyy')
                                            )::timestamp without time zone
                                        )
                                    )::numeric
                                )::numeric(18, 0) / ((12)::character varying)::numeric(18, 0)
                            )
                        )::character varying
                        ELSE CASE
                            WHEN (
                                (
                                    (
                                        (a.child_birth_year IS NOT NULL)
                                        AND (length((a.child_birth_year)::text) = 4)
                                    )
                                    AND (
                                        (a.child_birth_year)::text > ('1800'::character varying)::text
                                    )
                                )
                                AND (a.child_birth_month IS NULL)
                            ) THEN (
                                (
                                    (
                                        (
                                            datediff(
                                                month,
                                                (
                                                    (
                                                        (a.child_birth_year)::text|| ('-01-01'::character varying)::text 
                                                    )
                                                )::timestamp without time zone,
                                                (
                                                    to_date((
                                                        (
                                                            (
                                                                (
                                                                    date_part(
                                                                        month,
                                                                        current_timestamp()::date
                                                                    )
                                                                )::character varying
                                                            )::text || ('-01-'::character varying)::text
                                                        ) || (
                                                            (
                                                                date_part(
                                                                    year,
                                                                    current_timestamp()::date
                                                                )
                                                            )::character varying
                                                        )::text
                                                    ),'mm-dd-yyyy')
                                                )::timestamp without time zone
                                            )
                                        )::numeric
                                    )::numeric(18, 0) / ((12)::character varying)::numeric(18, 0)
                                )
                            )::character varying
                            ELSE 'UNDEFINED'::character varying
                        END
                    END AS child_age,
                    COALESCE(
                        CASE
                            WHEN (
                                (gen.gender_standard)::text = (''::character varying)::text
                            ) THEN NULL::character varying
                            ELSE gen.gender_standard
                        END,
                        'UNDEFINED'::character varying
                    ) AS child_gender
                FROM (
                        edw_vw_sfmc_children_data a
                        LEFT JOIN itg_mds_rg_sfmc_gender gen ON (
                            (
                                upper(trim((gen.gender_raw)::text)) = upper(trim((a.child_gender)::text))
                            )
                        )
                    )
                WHERE (
                        (a.country_code)::text = ('PH'::character varying)::text
                    )
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
    WHERE (
            (ifc.country_code)::text = ('PH'::character varying)::text
        )
    UNION ALL
    SELECT sent.activity_type AS data_src,
        sent.country_key AS country_code,
        CASE
            WHEN (
                (sent.country_key)::text = ('PH'::character varying)::text
            ) THEN 'PHILIPPINES'::character varying
            ELSE sent.country_key
        END AS country_name,
        (ifc.created_date) AS created_date,
        NULL AS invoicec_created_date,
        NULL AS invoice_completed_date,
        NULL AS last_logon_time,
        NULL AS purchase_date,
        NULL AS redeemed_date,
        NULL AS redeemption_completed_date,
        ifc.first_name,
        ifc.last_name,
        COALESCE(
            ifc.full_name,
            (
                (
                    (
                        (COALESCE(ifc.first_name, ''::character varying))::text || (' '::character varying)::text
                    ) || (COALESCE(ifc.last_name, ''::character varying))::text
                )
            )::character varying
        ) AS consumer_name,
        ifc.mobile_number,
        sent.subscriber_key,
        ifc.mobile_country_code,
        ifc.address_country,
        ifc.address_region,
        ifc.address_1,
        ifc.address_2,
        ifc.line_id,
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        COALESCE(ifc.address_zipcode, ''::character varying)
                                    )::text || (' '::character varying)::text
                                ) || (
                                    COALESCE(ifc.address_country, ''::character varying)
                                )::text
                            ) || (' '::character varying)::text
                        ) || (
                            COALESCE(ifc.address_city, ''::character varying)
                        )::text
                    ) || (' '::character varying)::text
                ) || (COALESCE(ifc.address_1, ''::character varying))::text
            )
        )::character varying AS address,
        CASE
            WHEN (
                (ifc.birth_month IS NULL)
                OR (
                    (ifc.birth_month)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            ELSE ifc.birth_month
        END AS birth_month,
        CASE
            WHEN (
                (ifc.birth_year IS NULL)
                OR (
                    (ifc.birth_year)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            ELSE ifc.birth_year
        END AS birth_year,
        ifc.consumer_age,
        CASE
            WHEN (
                (
                    (
                        (ifc.consumer_age)::text = ('UNDEFINED'::character varying)::text
                    )
                    OR (ifc.consumer_age IS NULL)
                )
                OR (
                    (ifc.consumer_age)::text = (''::character varying)::text
                )
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                (ifc.consumer_age)::numeric(18, 0) < ((0)::numeric)::numeric(18, 0)
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((0)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((17)::numeric)::numeric(18, 0)
                )
            ) THEN '<=17'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((18)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((24)::numeric)::numeric(18, 0)
                )
            ) THEN '18-24'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((25)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((34)::numeric)::numeric(18, 0)
                )
            ) THEN '25-34'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((35)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((44)::numeric)::numeric(18, 0)
                )
            ) THEN '35-44'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((45)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((54)::numeric)::numeric(18, 0)
                )
            ) THEN '45-54'::character varying
            WHEN (
                (
                    (ifc.consumer_age)::numeric(18, 0) >= ((55)::numeric)::numeric(18, 0)
                )
                AND (
                    (ifc.consumer_age)::numeric(18, 0) <= ((64)::numeric)::numeric(18, 0)
                )
            ) THEN '55-64'::character varying
            ELSE '>=65'::character varying
        END AS consumer_age_group,
        ifc.email,
        COALESCE(
            CASE
                WHEN (
                    (ifc.gender_standard)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE ifc.gender_standard
            END,
            'UNDEFINED'::character varying
        ) AS gender,
        NULL AS channel_name,
        ifc.brand,
        NULL AS product_name,
        NULL AS product_category,
        CASE
            WHEN (
                (ifc.tier IS NOT NULL)
                OR (
                    (ifc.tier)::text <> (''::character varying)::text
                )
            ) THEN ifc.tier_standard
            ELSE 'UNDEFINED'::character varying
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
        current_timestamp() AS data_refresh_time,
        ifc.subscriber_status,
        ifc.opt_in_for_jnj_communication,
        ifc.opt_in_for_campaign,
        ifc.viber_engaged
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
                    NULL::character varying AS tier_standard,
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
                    ifc.subscriber_status,
                    ifc.opt_in_for_jnj_communication,
                    ifc.opt_in_for_campaign,
                    ifc.viber_engaged,
                    CASE
                        WHEN (
                            (
                                (
                                    (ifc.birth_year IS NOT NULL)
                                    AND (length((ifc.birth_year)::text) = 4)
                                )
                                AND (
                                    (ifc.birth_year)::text > ('1800'::character varying)::text
                                )
                            )
                            AND (ifc.birth_month IS NOT NULL)
                        ) THEN (
                            (
                                (
                                    (
                                        datediff(
                                            month,
                                            (
                                                to_date((
                                                    (
                                                        (ifc.birth_month)::text || ('-01-'::character varying)::text
                                                    ) || (ifc.birth_year)::text
                                                ),'mm-dd-yyyy')
                                            )::timestamp without time zone,
                                            (
                                                to_date((
                                                    (
                                                        (
                                                            (
                                                                date_part(
                                                                    month,
                                                                    current_timestamp()::date
                                                                )
                                                            )::character varying
                                                        )::text || ('-01-'::character varying)::text
                                                    ) || (
                                                        (
                                                            date_part(
                                                                year,
                                                                current_timestamp()::date
                                                            )
                                                        )::character varying
                                                    )::text
                                                ),'mm-dd-yyyy')
                                            )::timestamp without time zone
                                        )
                                    )::numeric
                                )::numeric(18, 0) / ((12)::character varying)::numeric(18, 0)
                            )
                        )::character varying
                        ELSE CASE
                            WHEN (
                                (
                                    (
                                        (ifc.birth_year IS NOT NULL)
                                        AND (length((ifc.birth_year)::text) = 4)
                                    )
                                    AND (
                                        (ifc.birth_year)::text > ('1800'::character varying)::text
                                    )
                                )
                                AND (ifc.birth_month IS NULL)
                            ) THEN (
                                (
                                    (
                                        (
                                            datediff(
                                                month,
                                                (
                                                    (
                                                        (ifc.birth_year)::text|| ('-01-01'::character varying)::text 
                                                    )
                                                )::timestamp without time zone,
                                (to_date
                                ((
                                    (
                                                            (
                                                                (
                                                                    date_part(
                                                                        month,
                                                                        current_timestamp()::date
                                                                    )
                                                                )::character varying
                                                            )::text || ('-01-'::character varying)::text
                                                        ) || (
                                                            (
                                                                date_part(
                                                                    year,
                                                                    current_timestamp()::date
                                                                )
                                                            )::character varying
                                                        )::text
                                                    ),'mm-dd-yyyy')
                                                )::timestamp without time zone
                                            )
                                        )::numeric
                                    )::numeric(18, 0) / ((12)::character varying)::numeric(18, 0)
                                )
                            )::character varying
                            ELSE 'UNDEFINED'::character varying
                        END
                    END AS consumer_age
                FROM (
                        edw_vw_ph_sfmc_consumer_master ifc
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
            (sent.country_key)::text = ('PH'::character varying)::text
        )
),
temp_b as
(
SELECT
    'MEMBER_HIST' AS data_src,
    ifc.country_code,
    CASE
        WHEN (
            (ifc.country_code)::text = ('PH'::character varying)::text
        ) THEN 'PHILIPPINES'::character varying
        ELSE ifc.country_code
    END AS country_name,
    to_date(
        act.file_date,
        ('YYYYMMDD'::character varying)::text
    ) AS created_date,
    NULL AS invoicec_created_date,
    NULL AS invoice_completed_date,
    (ifc.last_logon_time) AS last_logon_time,
    NULL AS purchase_date,
    NULL AS redeemed_date,
    NULL AS redeemption_completed_date,
    ifc.first_name,
    ifc.last_name,
    COALESCE(
        ifc.full_name,
        (
            (
                (
                    (COALESCE(ifc.first_name, ''::character varying))::text || (' '::character varying)::text
                ) || (COALESCE(ifc.last_name, ''::character varying))::text
            )
        )::character varying
    ) AS consumer_name,
    ifc.mobile_number,
    ifc.subscriber_key,
    ifc.mobile_country_code,
    ifc.address_country,
    ifc.address_region,
    ifc.address_1,
    ifc.address_2,
    ifc.line_id,
    (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    COALESCE(ifc.address_zipcode, ''::character varying)
                                )::text || (' '::character varying)::text
                            ) || (
                                COALESCE(ifc.address_country, ''::character varying)
                            )::text
                        ) || (' '::character varying)::text
                    ) || (
                        COALESCE(ifc.address_city, ''::character varying)
                    )::text
                ) || (' '::character varying)::text
            ) || (COALESCE(ifc.address_1, ''::character varying))::text
        )
    )::character varying AS address,
    CASE
        WHEN (ifc.birth_month IS NULL) THEN 'UNDEFINED'::character varying
        ELSE ifc.birth_month
    END AS birth_month,
    CASE
        WHEN (ifc.birth_year IS NULL) THEN 'UNDEFINED'::character varying
        ELSE ifc.birth_year
    END AS birth_year,
    'UNDEFINED'::character varying AS consumer_age,
    'UNDEFINED'::character varying AS consumer_age_group,
    ifc.email,
    ifc.gender,
    NULL AS channel_name,
    ifc.brand,
    NULL AS product_name,
    NULL AS product_category,
    ifc.tier,
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
    current_timestamp() AS data_refresh_time,
    act.subscriber_status,
    ifc.opt_in_for_jnj_communication,
    ifc.opt_in_for_campaign,
    ifc.viber_engaged
 FROM (
        edw_vw_ph_sfmc_consumer_master ifc
        LEFT JOIN (
            SELECT ti.subscriber_status,
                ti.cntry_cd,
                ti.subscriber_key,
                ti.file_date
            FROM edw_vw_ph_sfmc_subscriber_status_history ti
        ) act ON (
            (
                (ifc.subscriber_key)::text = (act.subscriber_key)::text
            )
        )
    )
 WHERE (
        (ifc.country_code)::text = ('PH'::character varying)::text
    )
),
final as
(
select * from temp_a
union all
select * from temp_b
)
select * from final