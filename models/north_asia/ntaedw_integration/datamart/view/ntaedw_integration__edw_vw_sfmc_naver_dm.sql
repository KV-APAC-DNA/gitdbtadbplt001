with edw_sfmc_naver_data as
(
    select * from ref{{ ('ntaedw_integration__edw_sfmc_naver_data') }}
),
itg_query_parameters as
(
    select * from {{ source('ntaitg_integration', 'itg_query_parameters') }}
),
abc as
(
    SELECT
        a.cntry_cd,
        a.naver_id,
        a.birth_year,
        a.gender,
        a.total_purchase_amount,
        a.total_number_of_purchases,
        a.membership_grade_level,
        a.marketing_message_viewing_receiving,
        a.coupon_usage_issuance,
        a.number_of_reviews,
        a.number_of_comments,
        a.number_of_attendances,
        a.opt_in_for_jnj_communication,
        a.notification_subscription,
        a.acquisition_channel,
        a.reason_for_joining,
        a.skin_type_body,
        a.baby_skin_concerns,
        a.oral_health_concerns,
        a.skin_concerns_face,
        a.updated_date,
        a.membership_registration_date,
        a.file_name,
        a.crtd_dttm,
        CASE
            WHEN (
                (a.birth_year IS NOT NULL)
                OR (trim((a.birth_year)::text) <> '0'::text)
            ) THEN (
                (
                    (
                        (date_part(year, current_timestamp::date))::numeric - (a.birth_year)::numeric
                    ) + (1)::numeric
                )
            )::character varying
            ELSE 'UNDEFINED'::character varying
        END AS age,
        "substring"(
            (a.marketing_message_viewing_receiving)::text,
            1,
            CASE
                WHEN (
                    (
                        "position"(
                            (a.marketing_message_viewing_receiving)::text,
                            '/'::text
                        ) - 1
                    ) > 0
                ) THEN (
                    "position"(
                        (a.marketing_message_viewing_receiving)::text,
                        '/'::text
                    ) - 1
                )
                ELSE 0
            END
        ) AS msg_read_receive,
        "substring"(
            (a.coupon_usage_issuance)::text,
            1,
            CASE
                WHEN (
                    (
                        "position"((a.coupon_usage_issuance)::text, '/'::text) - 1
                    ) > 0
                ) THEN (
                    "position"((a.coupon_usage_issuance)::text, '/'::text) - 1
                )
                ELSE 0
            END
        ) AS coupon_usage_iss
    FROM edw_sfmc_naver_data a
    WHERE
        (
            ("substring"((a.file_name)::text, 35, 4))::numeric(18, 0) >= (
                select
                    date_part
                        (
                            year,
                            current_timestamp::timestamp without time zone
                        )::numeric(18, 0)
                    - (itg_query_parameters.parameter_value)::numeric(18, 0)
                from itg_query_parameters
                where (
                        (
                            (
                                (itg_query_parameters.country_code)::text = ('KR'::character varying)::text
                            )
                            AND (
                                (itg_query_parameters.parameter_name)::text = 'KR_SFMC'::text
                            )
                        )
                        AND (
                            (itg_query_parameters.parameter_type)::text = 'Report_data_retention'::text
                        )
                    )

            )
        )
),
final as
(
    SELECT
        abc.cntry_cd,
        abc.naver_id,
        abc.birth_year,
        abc.gender,
        (abc.total_purchase_amount)::numeric(38, 11) AS total_purchase_amount,
        (abc.total_number_of_purchases)::numeric(18, 0) AS total_number_of_purchases,
        abc.membership_grade_level,
        abc.marketing_message_viewing_receiving,
        abc.coupon_usage_issuance,
        (abc.number_of_reviews)::numeric(18, 0) AS number_of_reviews,
        (abc.number_of_comments)::numeric(18, 0) AS number_of_comments,
        (abc.number_of_attendances)::numeric(18, 0) AS number_of_attendances,
        abc.opt_in_for_jnj_communication,
        abc.notification_subscription,
        abc.acquisition_channel,
        abc.reason_for_joining,
        abc.skin_type_body,
        abc.baby_skin_concerns,
        abc.oral_health_concerns,
        abc.skin_concerns_face,
        (abc.updated_date)::date AS updated_date,
        (
            "substring"((abc.membership_registration_date)::text, 1, 10)
        )::date AS membership_registration_date,
        to_date(
            "substring"((abc.file_name)::text, 35, 8),
            'YYYYMMDD'::text
        ) AS file_date,
        abc.file_name,
        abc.crtd_dttm,
        abc.age,
        CASE
            WHEN ((abc.age)::text = 'UNDEFINED'::text) THEN 'UNDEFINED'::character varying
            WHEN (
                ((abc.age)::integer >= 0)
                AND ((abc.age)::integer < 10)
            ) THEN '0-9'::character varying
            WHEN (
                ((abc.age)::integer >= 10)
                AND ((abc.age)::integer < 20)
            ) THEN '10-19'::character varying
            WHEN (
                ((abc.age)::integer >= 20)
                AND ((abc.age)::integer < 30)
            ) THEN '20-29'::character varying
            WHEN (
                ((abc.age)::integer >= 30)
                AND ((abc.age)::integer < 40)
            ) THEN '30-39'::character varying
            WHEN (
                ((abc.age)::integer >= 40)
                AND ((abc.age)::integer < 50)
            ) THEN '40-49'::character varying
            WHEN (
                ((abc.age)::integer >= 50)
                AND ((abc.age)::integer < 60)
            ) THEN '50-59'::character varying
            WHEN (
                ((abc.age)::integer >= 60)
                AND ((abc.age)::integer < 70)
            ) THEN '60-69'::character varying
            WHEN ((abc.age)::integer >= 70) THEN '>=70'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS age_group,
        CASE
            WHEN (
                (abc.total_number_of_purchases)::text = 'UNDEFINED'::text
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                ((abc.total_number_of_purchases)::integer >= 0)
                AND ((abc.total_number_of_purchases)::integer < 10)
            ) THEN '0-9'::character varying
            WHEN (
                ((abc.total_number_of_purchases)::integer >= 10)
                AND ((abc.total_number_of_purchases)::integer < 20)
            ) THEN '10-19'::character varying
            WHEN (
                ((abc.total_number_of_purchases)::integer >= 20)
                AND ((abc.total_number_of_purchases)::integer < 30)
            ) THEN '20-29'::character varying
            WHEN (
                ((abc.total_number_of_purchases)::integer >= 30)
                AND ((abc.total_number_of_purchases)::integer < 40)
            ) THEN '30-39'::character varying
            WHEN (
                ((abc.total_number_of_purchases)::integer >= 40)
                AND ((abc.total_number_of_purchases)::integer < 50)
            ) THEN '40-49'::character varying
            WHEN (
                ((abc.total_number_of_purchases)::integer >= 50)
                AND ((abc.total_number_of_purchases)::integer < 60)
            ) THEN '50-59'::character varying
            WHEN (
                ((abc.total_number_of_purchases)::integer >= 60)
                AND ((abc.total_number_of_purchases)::integer < 70)
            ) THEN '60-69'::character varying
            WHEN ((abc.total_number_of_purchases)::integer >= 70) THEN '>=70'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS no_of_purchase_group,
        CASE
            WHEN (
                (abc.number_of_reviews)::text = 'UNDEFINED'::text
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                ((abc.number_of_reviews)::integer >= 0)
                AND ((abc.number_of_reviews)::integer < 10)
            ) THEN '0-9'::character varying
            WHEN (
                ((abc.number_of_reviews)::integer >= 10)
                AND ((abc.number_of_reviews)::integer < 20)
            ) THEN '10-19'::character varying
            WHEN (
                ((abc.number_of_reviews)::integer >= 20)
                AND ((abc.number_of_reviews)::integer < 30)
            ) THEN '20-29'::character varying
            WHEN (
                ((abc.number_of_reviews)::integer >= 30)
                AND ((abc.number_of_reviews)::integer < 40)
            ) THEN '30-39'::character varying
            WHEN (
                ((abc.number_of_reviews)::integer >= 40)
                AND ((abc.number_of_reviews)::integer < 50)
            ) THEN '40-49'::character varying
            WHEN (
                ((abc.number_of_reviews)::integer >= 50)
                AND ((abc.number_of_reviews)::integer < 60)
            ) THEN '50-59'::character varying
            WHEN (
                ((abc.number_of_reviews)::integer >= 60)
                AND ((abc.number_of_reviews)::integer < 70)
            ) THEN '60-69'::character varying
            WHEN ((abc.number_of_reviews)::integer >= 70) THEN '>=70'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS no_of_reviews_group,
        CASE
            WHEN (
                (abc.number_of_comments)::text = 'UNDEFINED'::text
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                ((abc.number_of_comments)::integer >= 0)
                AND ((abc.number_of_comments)::integer < 10)
            ) THEN '0-9'::character varying
            WHEN (
                ((abc.number_of_comments)::integer >= 10)
                AND ((abc.number_of_comments)::integer < 20)
            ) THEN '10-19'::character varying
            WHEN (
                ((abc.number_of_comments)::integer >= 20)
                AND ((abc.number_of_comments)::integer < 30)
            ) THEN '20-29'::character varying
            WHEN (
                ((abc.number_of_comments)::integer >= 30)
                AND ((abc.number_of_comments)::integer < 40)
            ) THEN '30-39'::character varying
            WHEN (
                ((abc.number_of_comments)::integer >= 40)
                AND ((abc.number_of_comments)::integer < 50)
            ) THEN '40-49'::character varying
            WHEN (
                ((abc.number_of_comments)::integer >= 50)
                AND ((abc.number_of_comments)::integer < 60)
            ) THEN '50-59'::character varying
            WHEN (
                ((abc.number_of_comments)::integer >= 60)
                AND ((abc.number_of_comments)::integer < 70)
            ) THEN '60-69'::character varying
            WHEN ((abc.number_of_comments)::integer >= 70) THEN '>=70'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS no_of_comments_group,
        CASE
            WHEN (
                (abc.number_of_attendances)::text = 'UNDEFINED'::text
            ) THEN 'UNDEFINED'::character varying
            WHEN (
                ((abc.number_of_attendances)::integer >= 0)
                AND ((abc.number_of_attendances)::integer < 10)
            ) THEN '0-9'::character varying
            WHEN (
                ((abc.number_of_attendances)::integer >= 10)
                AND ((abc.number_of_attendances)::integer < 20)
            ) THEN '10-19'::character varying
            WHEN (
                ((abc.number_of_attendances)::integer >= 20)
                AND ((abc.number_of_attendances)::integer < 30)
            ) THEN '20-29'::character varying
            WHEN (
                ((abc.number_of_attendances)::integer >= 30)
                AND ((abc.number_of_attendances)::integer < 40)
            ) THEN '30-39'::character varying
            WHEN (
                ((abc.number_of_attendances)::integer >= 40)
                AND ((abc.number_of_attendances)::integer < 50)
            ) THEN '40-49'::character varying
            WHEN (
                ((abc.number_of_attendances)::integer >= 50)
                AND ((abc.number_of_attendances)::integer < 60)
            ) THEN '50-59'::character varying
            WHEN (
                ((abc.number_of_attendances)::integer >= 60)
                AND ((abc.number_of_attendances)::integer < 70)
            ) THEN '60-69'::character varying
            WHEN ((abc.number_of_attendances)::integer >= 70) THEN '>=70'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS no_of_attendances_group,
        CASE
            WHEN (
                ((abc.msg_read_receive)::character varying)::text = 'UNDEFINED'::text
            ) OR abc.msg_read_receive =     '' THEN 'UNDEFINED'::character varying
            WHEN (
                ((abc.msg_read_receive)::integer >= 0)
                AND ((abc.msg_read_receive)::integer < 10)
            ) THEN '0-9'::character varying
            WHEN (
                ((abc.msg_read_receive)::integer >= 10)
                AND ((abc.msg_read_receive)::integer < 20)
            ) THEN '10-19'::character varying
            WHEN (
                ((abc.msg_read_receive)::integer >= 20)
                AND ((abc.msg_read_receive)::integer < 30)
            ) THEN '20-29'::character varying
            WHEN (
                ((abc.msg_read_receive)::integer >= 30)
                AND ((abc.msg_read_receive)::integer < 40)
            ) THEN '30-39'::character varying
            WHEN (
                ((abc.msg_read_receive)::integer >= 40)
                AND ((abc.msg_read_receive)::integer < 50)
            ) THEN '40-49'::character varying
            WHEN (
                ((abc.msg_read_receive)::integer >= 50)
                AND ((abc.msg_read_receive)::integer < 60)
            ) THEN '50-59'::character varying
            WHEN (
                ((abc.msg_read_receive)::integer >= 60)
                AND ((abc.msg_read_receive)::integer < 70)
            ) THEN '60-69'::character varying
            WHEN ((abc.msg_read_receive)::integer >= 70) THEN '>=70'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS msg_read,
        CASE
            WHEN (
                ((abc.coupon_usage_iss)::character varying)::text = 'UNDEFINED'::text
            ) OR abc.coupon_usage_iss = '' THEN 'UNDEFINED'::character varying
            WHEN (
                ((abc.coupon_usage_iss)::integer >= 0)
                AND ((abc.coupon_usage_iss)::integer < 10)
            ) THEN '0-9'::character varying
            WHEN (
                ((abc.coupon_usage_iss)::integer >= 10)
                AND ((abc.coupon_usage_iss)::integer < 20)
            ) THEN '10-19'::character varying
            WHEN (
                ((abc.coupon_usage_iss)::integer >= 20)
                AND ((abc.coupon_usage_iss)::integer < 30)
            ) THEN '20-29'::character varying
            WHEN (
                ((abc.coupon_usage_iss)::integer >= 30)
                AND ((abc.coupon_usage_iss)::integer < 40)
            ) THEN '30-39'::character varying
            WHEN (
                ((abc.coupon_usage_iss)::integer >= 40)
                AND ((abc.coupon_usage_iss)::integer < 50)
            ) THEN '40-49'::character varying
            WHEN (
                ((abc.coupon_usage_iss)::integer >= 50)
                AND ((abc.coupon_usage_iss)::integer < 60)
            ) THEN '50-59'::character varying
            WHEN (
                ((abc.coupon_usage_iss)::integer >= 60)
                AND ((abc.coupon_usage_iss)::integer < 70)
            ) THEN '60-69'::character varying
            WHEN ((abc.coupon_usage_iss)::integer >= 70) THEN '>=70'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS coupon_usage,
        add_months(
            ((abc.updated_date)::date)::timestamp without time zone,
            (-1)::bigint
        ) AS new_update_date
    FROM abc
)
select * from final