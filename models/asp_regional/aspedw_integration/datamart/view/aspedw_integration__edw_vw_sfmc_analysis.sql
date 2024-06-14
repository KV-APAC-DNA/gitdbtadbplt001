with edw_vw_sfmc_invoice_data as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_invoice_data') }}
),
edw_vw_sfmc_consumer_master as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_consumer_master') }}
),
itg_mds_tw_sfmc_member_tier as (
    select * from {{ ref('ntaitg_integration__itg_mds_tw_sfmc_member_tier') }}
),
edw_vw_sfmc_children_data as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_children_data') }}
),
itg_mds_tw_sfmc_gender as (
    select * from {{ ref('ntaitg_integration__itg_mds_tw_sfmc_gender') }}
),
itg_mds_tw_sfmc_channel as (
    select * from {{ ref('ntaitg_integration__itg_mds_tw_sfmc_channel') }}
),
edw_vw_sfmc_redemption_data as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_redemption_data') }}
),
edw_vw_sfmc_email_activity_fact as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_email_activity_fact') }}
),
itg_sfmc_consumer_master as (
    select * from {{ ref('aspitg_integration__itg_sfmc_consumer_master') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_vw_sfmc_active_history as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_active_history') }}
),
edw_vw_sfmc_tier_history as (
    select * from {{ ref('aspedw_integration__edw_vw_sfmc_tier_history') }}
),
--INVOICE
invoice as
(
    SELECT
        'INVOICE' AS data_src,
        isi.country_code,
        CASE
            WHEN (
                (
                    (isi.country_code)::text = ('TW'::character varying)::text
                )
                OR (
                    (isi.country_code IS NULL)
                    AND ('TW' IS NULL)
                )
            ) THEN 'TAIWAN'::character varying
            ELSE isi.country_code
        END AS country_name,
        to_date(ifc.created_date) AS created_date,
        to_date(isi.created_date) AS invoicec_created_date,
        to_date(isi.completed_date) AS invoice_completed_date,
        to_date(ifc.last_logon_time) AS last_logon_time,
        to_date(isi.purchase_date) AS purchase_date,
        null AS redeemed_date,
        null AS redeemption_completed_date,
        ifc.first_name,
        ifc.last_name,
        COALESCE
        (
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
        isi.subscriber_key,
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
                                ((COALESCE(ifc.address_zipcode, ''::character varying))::text || (' '::character varying)::text)
                                || (COALESCE(ifc.address_country, ''::character varying))::text) || (' '::character varying)::text
                        ) || (COALESCE(ifc.address_city, ''::character varying))::text) || (' '::character varying)::text
                ) || (COALESCE(ifc.address_1, ''::character varying))::text
            )
        )::character varying AS address,
        COALESCE
        (
            CASE
                WHEN (
                    (ifc.birth_month)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE ifc.birth_month
            END,
            'UNDEFINED'::character varying
        ) AS birth_month,
        COALESCE
        (
            CASE
                WHEN (
                    (ifc.birth_year)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE ifc.birth_year
            END,
            'UNDEFINED'::character varying
        ) AS birth_year,
        coalesce(age_cal,'UNDEFINED')::varchar AS consumer_age,
        CASE
            WHEN age_cal = 'UNDEFINED' THEN 'UNDEFINED'
            WHEN (floor(age_cal) < 17) AND(floor(age_cal) >= 0) THEN '17 Below'::character varying
            WHEN (floor(age_cal) >= 17) AND(floor(age_cal) <= 24) THEN '17-24'::character varying
            WHEN (floor(age_cal) >= 25) AND(floor(age_cal) <= 34) THEN '25-34'::character varying
            WHEN (floor(age_cal) >= 35) AND(floor(age_cal) <= 44) THEN '35-44'::character varying
            WHEN (floor(age_cal) >= 45) AND(floor(age_cal) <= 54) THEN '45-54'::character varying
            WHEN (floor(age_cal) >= 55) AND(floor(age_cal) <= 64) THEN '55-64'::character varying
            WHEN (floor(age_cal) >= 65) THEN '65+'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS consumer_age_group,
        ifc.email,
        gen.gender_standard AS gender,
        chnl.channel_standard AS channel_name,
        ifc.brand,
        isi.product_name,
        isi.product_category,
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
        isi.invoive_number,
        isi.quantity AS invoice_qty,
        isi.invoice_type,
        isi.seller_name AS invoice_seller_name,
        isi.points AS invoice_points,
        isi.status AS invoice_status,
        null AS child_name,
        null AS child_birth_month,
        null AS child_birth_year,
        null AS child_gender,
        null AS child_number,
        null AS child_age,
        null AS child_group,
        null AS child_flag,
        null AS redeemed_order_number,
        null AS redeemed_prod_name,
        null AS redemption_status,
        null AS redeemed_points,
        null AS redeemed_qty,
        null AS activity_date,
        null AS local_activity_date,
        null AS sent_activity_date,
        null AS email_name,
        null AS email_subject,
        null AS sent_email_name,
        null AS sent_email_subject,
        null AS email_delivered_flag,
        null AS is_unique,
        null AS url,
        null AS link_name,
        null AS link_content,
        null AS bounce_reason,
        null AS bounce_type,
        null AS bounce_description,
        ifc.crtd_dttm AS data_refresh_time,
        null AS active_flag,
        NULL::integer AS inv_cnt
    FROM
    edw_vw_sfmc_invoice_data isi
    LEFT JOIN
    (
        (
            SELECT
                ifc.country_code,
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
                tr.tier_standard,
                CASE
                    WHEN
                    (
                        (ifc.birth_year)::text <= ((date_part(year,current_timestamp::timestamp without time zone))::character varying)::text
                    )
                    THEN
                        (
                            (
                                datediff
                                (
                                    month,
                                    to_date
                                    (
                                        --year
                                        case when ifc.birth_year IS NULL or ifc.birth_year = 'UNDEFINED' THEN '9999'
                                        else ifc.birth_year end
                                            ||
                                            '-'
                                            ||
                                        --month
                                            case
                                                    when coalesce(ifc.birth_month, '')  = '' or ifc.birth_month = 'UNDEFINED' then '01'
                                                    else lpad(ifc.birth_month, 2, '0')
                                            end
                                            ||
                                        --date
                                            '-01'
                                    ),
                                    date_trunc('month', current_timestamp())
                                )::numeric
                            )::numeric(18, 0) / 12.00
                        )
                    ELSE (- ((1)::numeric)::numeric(18, 0))
                END as age_cal_temp,
                case
                    when age_cal_temp < 0 then 'UNDEFINED'
                    else age_cal_temp::varchar
                end as age_cal
            FROM edw_vw_sfmc_consumer_master ifc
            LEFT JOIN itg_mds_tw_sfmc_member_tier tr
            ON trim(upper((tr.tier_raw)::text)) = trim(COALESCE(upper((ifc.tier)::text),('NA'::character varying)::text))
        ) ifc
        LEFT JOIN itg_mds_tw_sfmc_gender gen
        ON upper(trim((gen.gender_raw)::text)) = upper(trim((COALESCE(ifc.gender, 'NA'::character varying))::text))
    ) ON  trim(upper((ifc.subscriber_key)::text)) = trim(upper((isi.subscriber_key)::text))

    LEFT JOIN itg_mds_tw_sfmc_channel chnl
    ON 
    upper(trim((chnl.channel_raw)::text)) = upper(trim(
                                                        REGEXP_REPLACE(
                                                        COALESCE(isi.channel_name, 'NA'::character varying),
                                                            '\\t+$',
                                                            '')
                                                    )   
                                                    )
    WHERE (isi.country_code)::text = 'TW'
),
 --CHILDREN

children as
(
    SELECT
        'CHILDREN' AS data_src,
        ifc.country_code,
        CASE
            WHEN (
                (
                    (ifc.country_code)::text = ('TW'::character varying)::text
                )
                OR (
                    (ifc.country_code IS NULL)
                    AND ('TW' IS NULL)
                )
            ) THEN 'TAIWAN'::character varying
            ELSE ifc.country_code
        END AS country_name,
        to_date(ifc.created_date) AS created_date,
        null AS invoicec_created_date,
        null AS invoice_completed_date,
        to_date(ifc.last_logon_time) AS last_logon_time,
        null AS purchase_date,
        null AS redeemed_date,
        null AS redeemption_completed_date,
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
                                ((COALESCE(ifc.address_zipcode, ''::character varying))::text || (' '::character varying)::text)
                                || (COALESCE(ifc.address_country, ''::character varying))::text) || (' '::character varying)::text
                        ) || (COALESCE(ifc.address_city, ''::character varying))::text) || (' '::character varying)::text
                ) || (COALESCE(ifc.address_1, ''::character varying))::text
            )
        )::character varying AS address,
        COALESCE(
            CASE
                WHEN (
                    (ifc.birth_month)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE ifc.birth_month
            END,
            'UNDEFINED'::character varying
        ) AS birth_month,
        COALESCE(
            CASE
                WHEN (
                    (ifc.birth_year)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE ifc.birth_year
            END,
            'UNDEFINED'::character varying
        ) AS birth_year,
        coalesce(ifc.age_cal,'UNDEFINED')::varchar AS consumer_age,
        CASE
            WHEN ifc.age_cal = 'UNDEFINED' THEN 'UNDEFINED'
            WHEN (floor(ifc.age_cal) < 17) AND(floor(ifc.age_cal) >= 0) THEN '17 Below'::character varying
            WHEN (floor(ifc.age_cal) >= 17) AND(floor(ifc.age_cal) <= 24) THEN '17-24'::character varying
            WHEN (floor(ifc.age_cal) >= 25) AND(floor(ifc.age_cal) <= 34) THEN '25-34'::character varying
            WHEN (floor(ifc.age_cal) >= 35) AND(floor(ifc.age_cal) <= 44) THEN '35-44'::character varying
            WHEN (floor(ifc.age_cal) >= 45) AND(floor(ifc.age_cal) <= 54) THEN '45-54'::character varying
            WHEN (floor(ifc.age_cal) >= 55) AND(floor(ifc.age_cal) <= 64) THEN '55-64'::character varying
            WHEN (floor(ifc.age_cal) >= 65) THEN '65+'::character varying
            ELSE 'UNDEFINED'::character varying
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
        null AS channel_name,
        ifc.brand,
        null AS product_name,
        null AS product_category,
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
        null AS invoive_number,
        null AS invoice_qty,
        null AS invoice_type,
        null AS invoice_seller_name,
        null AS invoice_points,
        null AS invoice_status,
        isc.child_name,
        COALESCE(
            CASE
                WHEN ( (isc.child_birth_month)::text = (''::character varying)::text ) THEN NULL::character varying
                ELSE isc.child_birth_month
            END,
            'UNDEFINED'::character varying
        ) AS child_birth_month,
        COALESCE
        (
            CASE
                WHEN ((isc.child_birth_year)::text = (''::character varying)::text) THEN NULL::character varying
                ELSE isc.child_birth_year
            END,
            'UNDEFINED'::character varying
        ) AS child_birth_year,
        COALESCE(
            CASE
                WHEN ((gen.gender_standard)::text = (''::character varying)::text) THEN NULL::character varying
                ELSE gen.gender_standard
            END,
            'UNDEFINED'::character varying
        ) AS child_gender,
        isc.child_number,
        coalesce(isc.age_cal,'UNDEFINED')::varchar as child_age,
        CASE
            WHEN isc.age_cal <>'UNDEFINED' then
            case
                WHEN (floor(isc.age_cal) >= 0) AND(floor(isc.age_cal) < 1) THEN 'NEWBORN(>=0,<1 Yrs)'::character varying
                WHEN (floor(isc.age_cal) >= 1) AND(floor(isc.age_cal) < 3) THEN 'TODDLER(>=1,<3 Yrs)'::character varying
                WHEN (floor(isc.age_cal) >= 3) AND(floor(isc.age_cal) < 6) THEN 'KIDS(>=3,<6 Yrs)'::character varying
                WHEN (floor(isc.age_cal) >= 6) THEN 'FAMILY(>=6 Yrs)'::character varying
                ELSE 'UNDEFINED'::character varying
            end
            else 'UNDEFINED'
        END AS child_group,
        CASE
            WHEN (isc.parent_key IS NULL) THEN 'Without Kids'::character varying
            ELSE 'With Kids'::character varying
        END AS child_flag,
        null AS redeemed_order_number,
        null AS redeemed_prod_name,
        null AS redemption_status,
        null AS redeemed_points,
        null AS redeemed_qty,
        null AS activity_date,
        null AS local_activity_date,
        null AS sent_activity_date,
        null AS email_name,
        null AS email_subject,
        null AS sent_email_name,
        null AS sent_email_subject,
        null AS email_delivered_flag,
        null AS is_unique,
        null AS url,
        null AS link_name,
        null AS link_content,
        null AS bounce_reason,
        null AS bounce_type,
        null AS bounce_description,
        ifc.crtd_dttm AS data_refresh_time,
        null AS active_flag,
        NULL::integer AS inv_cnt
    FROM
        (
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
                    tr.tier_standard,
                    gen.gender_standard,
                    CASE
                        WHEN
                        (
                            (ifc.birth_year)::text <= ((date_part(year,current_timestamp::timestamp without time zone))::character varying)::text
                        )
                        THEN
                            (
                                (
                                    datediff
                                    (
                                        month,
                                        to_date
                                        (
                                            --year
                                            case when ifc.birth_year IS NULL or ifc.birth_year = 'UNDEFINED' THEN '9999'
                                            else ifc.birth_year end
                                                ||
                                                '-'
                                                ||
                                            --month
                                                case
                                                        when coalesce(ifc.birth_month, '')  = '' or ifc.birth_month = 'UNDEFINED' then '01'
                                                        else lpad(ifc.birth_month, 2, '0')
                                                end
                                                ||
                                            --date
                                                '-01'
                                        ),
                                        date_trunc('month', current_timestamp())
                                    )::numeric
                                )::numeric(18, 0) / 12.00
                            )
                        ELSE (- ((1)::numeric)::numeric(18, 0))
                    END as age_cal_temp,
                    case
                        when age_cal_temp < 0 then 'UNDEFINED'
                        else age_cal_temp::varchar
                    end as age_cal
                FROM
                    (
                        (
                            edw_vw_sfmc_consumer_master ifc
                            LEFT JOIN itg_mds_tw_sfmc_gender gen ON (
                                (
                                    upper(trim((gen.gender_raw)::text)) = upper(trim((ifc.gender)::text))
                                )
                            )
                        )
                        LEFT JOIN itg_mds_tw_sfmc_member_tier tr ON (
                            (
                                trim(upper((tr.tier_raw)::text)) = trim(
                                    COALESCE(
                                        upper((ifc.tier)::text),
                                        ('NA'::character varying)::text
                                    )
                                )
                            )
                        )
                    )
            ) ifc
            LEFT JOIN
            (
                (
                    SELECT edw_vw_sfmc_children_data.country_code,
                        edw_vw_sfmc_children_data.parent_key,
                        edw_vw_sfmc_children_data.child_name,
                        edw_vw_sfmc_children_data.child_birth_month,
                        edw_vw_sfmc_children_data.child_birth_year,
                        edw_vw_sfmc_children_data.child_gender,
                        edw_vw_sfmc_children_data.child_number,
                        CASE
                            WHEN
                            (
                                (child_birth_year)::text <= ((date_part(year,current_timestamp::timestamp without time zone))::character varying)::text
                            )
                            THEN
                                (
                                    (
                                        datediff
                                        (
                                            month,
                                            to_date
                                            (
                                                --year
                                                case when child_birth_year IS NULL or child_birth_year = 'UNDEFINED' THEN '9999'
                                                else child_birth_year end
                                                    ||
                                                    '-'
                                                    ||
                                                --month
                                                    case
                                                            when coalesce(child_birth_month, '')  = '' or child_birth_month = 'UNDEFINED' then '01'
                                                            else lpad(child_birth_month, 2, '0')
                                                    end
                                                    ||
                                                --date
                                                    '-01'
                                            ),
                                            date_trunc('month', current_timestamp())
                                        )::numeric
                                    )::numeric(18, 0) / 12.00
                                )
                            ELSE (- ((1)::numeric)::numeric(18, 0))
                        END as age_cal_temp,
                        case
                            when age_cal_temp < 0 then 'UNDEFINED'
                            else age_cal_temp::varchar
                        end as age_cal
                    FROM edw_vw_sfmc_children_data
                    WHERE (
                            (edw_vw_sfmc_children_data.country_code)::text = ('TW'::character varying)::text
                        )
                ) isc
                LEFT JOIN itg_mds_tw_sfmc_gender gen ON (
                    (
                        upper(trim((gen.gender_raw)::text)) = upper(trim((isc.child_gender)::text))
                    )
                )
            ) ON
            (
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
    WHERE (ifc.country_code)::text = ('TW'::character varying)::text
),
--REDEMPTION
redemption as
(
    SELECT
        'REDEMPTION' AS data_src,
        isr.country_code,
        CASE
            WHEN (
                (
                    (isr.country_code)::text = ('TW'::character varying)::text
                )
                OR (
                    (isr.country_code IS NULL)
                    AND ('TW' IS NULL)
                )
            ) THEN 'TAIWAN'::character varying
            ELSE isr.country_code
        END AS country_name,
        to_date(ifc.created_date) AS created_date,
        null as invoicec_created_date,
        null as invoice_completed_date,
        to_date(ifc.last_logon_time) AS last_logon_time,
        null as purchase_date,
        to_date(isr.redeemed_date) AS redeemed_date,
        to_date(isr.completed_date) AS redeemption_completed_date,
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
        isr.subscriber_key,
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
        COALESCE
        (
            CASE
                WHEN ((ifc.birth_month)::text = (''::character varying)::text) THEN NULL::character varying ELSE ifc.birth_month
            END,'UNDEFINED'::character varying
        ) AS birth_month,
        COALESCE
        (
            CASE
                WHEN ((ifc.birth_year)::text = (''::character varying)::text) THEN NULL::character varying ELSE ifc.birth_year
            END,'UNDEFINED'::character varying
        ) AS birth_year,
       coalesce(age_cal,'UNDEFINED')::varchar AS consumer_age,
        CASE
            WHEN age_cal = 'UNDEFINED' THEN 'UNDEFINED'
            WHEN (floor(age_cal) < 17) AND(floor(age_cal) >= 0) THEN '17 Below'::character varying
            WHEN (floor(age_cal) >= 17) AND(floor(age_cal) <= 24) THEN '17-24'::character varying
            WHEN (floor(age_cal) >= 25) AND(floor(age_cal) <= 34) THEN '25-34'::character varying
            WHEN (floor(age_cal) >= 35) AND(floor(age_cal) <= 44) THEN '35-44'::character varying
            WHEN (floor(age_cal) >= 45) AND(floor(age_cal) <= 54) THEN '45-54'::character varying
            WHEN (floor(age_cal) >= 55) AND(floor(age_cal) <= 64) THEN '55-64'::character varying
            WHEN (floor(age_cal) >= 65) THEN '65+'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS consumer_age_group,
        ifc.email,
        COALESCE(
            CASE
                WHEN (
                    (gen.gender_standard)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE gen.gender_standard
            END,
            'UNDEFINED'::character varying
        ) AS gender,
        null as channel_name,
        ifc.brand,
        null as product_name,
        null as product_category,
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
        null as invoive_number,
        null as invoice_qty,
        null as invoice_type,
        null as invoice_seller_name,
        null as invoice_points,
        null as invoice_status,
        null as child_name,
        null as child_birth_month,
        null as child_birth_year,
        null as child_gender,
        null as child_number,
        null as child_age,
        null as child_group,
        null as child_flag,
        isr.order_number AS redeemed_order_number,
        isr.product_name AS redeemed_prod_name,
        isr.redemption_status,
        isr.redeemed_points,
        isr.quantity AS redeemed_qty,
        null as activity_date,
        null as local_activity_date,
        null as sent_activity_date,
        null as email_name,
        null as email_subject,
        null as sent_email_name,
        null as sent_email_subject,
        null as email_delivered_flag,
        null as is_unique,
        null as url,
        null as link_name,
        null as link_content,
        null as bounce_reason,
        null as bounce_type,
        null as bounce_description,
        ifc.crtd_dttm AS data_refresh_time,
        null as active_flag,
        NULL::integer AS inv_cnt
    FROM
        (
            edw_vw_sfmc_redemption_data isr
            LEFT JOIN
            (
                (
                    SELECT
                        ifc.country_code,
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
                        tr.tier_standard,
                        CASE
                            WHEN
                            (
                                (ifc.birth_year)::text <= ((date_part(year,current_timestamp::timestamp without time zone))::character varying)::text
                            )
                            THEN
                                (
                                    (
                                        datediff
                                        (
                                            month,
                                            to_date
                                            (
                                                --year
                                                case when ifc.birth_year IS NULL or ifc.birth_year = 'UNDEFINED' THEN '9999'
                                                else ifc.birth_year end
                                                    ||
                                                    '-'
                                                    ||
                                                --month
                                                    case
                                                            when coalesce(ifc.birth_month, '')  = '' or ifc.birth_month = 'UNDEFINED' then '01'
                                                            else lpad(ifc.birth_month, 2, '0')
                                                    end
                                                    ||
                                                --date
                                                    '-01'
                                            ),
                                            date_trunc('month', current_timestamp())
                                        )::numeric
                                    )::numeric(18, 0) / 12.00
                                )
                            ELSE (- ((1)::numeric)::numeric(18, 0))
                        END as age_cal_temp,
                        case
                            when age_cal_temp < 0 then 'UNDEFINED'
                            else age_cal_temp::varchar
                        end as age_cal
                    FROM
                        (
                            edw_vw_sfmc_consumer_master ifc
                            LEFT JOIN itg_mds_tw_sfmc_member_tier tr ON (
                                (
                                    trim(upper((tr.tier_raw)::text)) = trim(
                                        COALESCE(
                                            upper((ifc.tier)::text),
                                            ('NA'::character varying)::text
                                        )
                                    )
                                )
                            )
                        )
                ) ifc
                LEFT JOIN itg_mds_tw_sfmc_gender gen ON (
                    (
                        upper(trim((gen.gender_raw)::text)) = upper(trim((ifc.gender)::text))
                    )
                )
            ) ON (
                (
                    (
                        trim(upper((ifc.subscriber_key)::text)) = trim(upper((isr.subscriber_key)::text))
                    )
                    AND (
                        trim((ifc.country_code)::text) = trim((isr.country_code)::text)
                    )
                )
            )
        )
    WHERE (isr.country_code)::text = ('TW'::character varying)::text
),
-- CLICK,OPEN,BOUNCE,UNSUBSCRIBE,SENT
others as
(
    SELECT
        sent.activity_type AS data_src,
        sent.country_key AS country_code,
        CASE
            WHEN (
                (
                    (sent.country_key)::text = ('TW'::character varying)::text
                )
                OR (
                    (sent.country_key IS NULL)
                    AND ('TW' IS NULL)
                )
            ) THEN 'TAIWAN'::character varying
            ELSE sent.country_key
        END AS country_name,
        to_date(ifc.created_date) AS created_date,
        null as invoicec_created_date,
        null as invoice_completed_date,
        null as last_logon_time,
        null as purchase_date,
        null as redeemed_date,
        null as redeemption_completed_date,
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
        COALESCE(
            CASE
                WHEN (
                    (ifc.birth_month)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE ifc.birth_month
            END,
            'UNDEFINED'::character varying
        ) AS birth_month,
        COALESCE(
            CASE
                WHEN (
                    (ifc.birth_year)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE ifc.birth_year
            END,
            'UNDEFINED'::character varying
        ) AS birth_year,
        coalesce(ifc.age_cal,'UNDEFINED')::varchar AS consumer_age,
        CASE
            WHEN ifc.age_cal = 'UNDEFINED' THEN 'UNDEFINED'
            WHEN (floor(ifc.age_cal) < 17) AND(floor(ifc.age_cal) >= 0) THEN '17 Below'::character varying
            WHEN (floor(ifc.age_cal) >= 17) AND(floor(ifc.age_cal) <= 24) THEN '17-24'::character varying
            WHEN (floor(ifc.age_cal) >= 25) AND(floor(ifc.age_cal) <= 34) THEN '25-34'::character varying
            WHEN (floor(ifc.age_cal) >= 35) AND(floor(ifc.age_cal) <= 44) THEN '35-44'::character varying
            WHEN (floor(ifc.age_cal) >= 45) AND(floor(ifc.age_cal) <= 54) THEN '45-54'::character varying
            WHEN (floor(ifc.age_cal) >= 55) AND(floor(ifc.age_cal) <= 64) THEN '55-64'::character varying
            WHEN (floor(ifc.age_cal) >= 65) THEN '65+'::character varying
            ELSE 'UNDEFINED'::character varying
        END AS consumer_age_group,
        ifc.email,
        COALESCE(
            CASE
                WHEN (
                    (gen.gender_standard)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE gen.gender_standard
            END,
            'UNDEFINED'::character varying
        ) AS gender,
        null as channel_name,
        ifc.brand,
        null as product_name,
        null as product_category,
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
        null as invoive_number,
        null as invoice_qty,
        null as invoice_type,
        null as invoice_seller_name,
        null as invoice_points,
        null as invoice_status,
        null as child_name,
        null as child_birth_month,
        null as child_birth_year,
        null as child_gender,
        null as child_number,
        null as child_age,
        null as child_group,
        null as child_flag,
        null as redeemed_order_number,
        null as redeemed_prod_name,
        null as redemption_status,
        null as redeemed_points,
        null as redeemed_qty,
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
        ifc.crtd_dttm AS data_refresh_time,
        null as active_flag,
        NULL::integer AS inv_cnt
    FROM
        edw_vw_sfmc_email_activity_fact sent
        LEFT JOIN
        (
            (
                SELECT
                    ifc.country_code,
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
                    tr.tier_standard,
                    CASE
                        WHEN
                        (
                            (ifc.birth_year)::text <= ((date_part(year,current_timestamp::timestamp without time zone))::character varying)::text
                        )
                        THEN
                            (
                                (
                                    datediff
                                    (
                                        month,
                                        to_date
                                        (
                                            --year
                                            case when ifc.birth_year IS NULL or ifc.birth_year = 'UNDEFINED' THEN '9999'
                                            else ifc.birth_year end
                                                ||
                                                '-'
                                                ||
                                            --month
                                                case
                                                        when coalesce(ifc.birth_month, '')  = '' or ifc.birth_month = 'UNDEFINED' then '01'
                                                        else lpad(ifc.birth_month, 2, '0')
                                                end
                                                ||
                                            --date
                                                '-01'
                                        ),
                                        date_trunc('month', current_timestamp())
                                    )::numeric
                                )::numeric(18, 0) / 12.00
                            )
                        ELSE (- ((1)::numeric)::numeric(18, 0))
                    END as age_cal_temp,
                    case
                        when age_cal_temp < 0 then 'UNDEFINED'
                        else age_cal_temp::varchar
                    end as age_cal
                FROM
                    (
                        edw_vw_sfmc_consumer_master ifc
                        LEFT JOIN itg_mds_tw_sfmc_member_tier tr ON (
                            (
                                trim(upper((tr.tier_raw)::text)) = COALESCE(
                                    upper((ifc.tier)::text),
                                    ('NA'::character varying)::text
                                )
                            )
                        )
                    )
            ) ifc
            LEFT JOIN itg_mds_tw_sfmc_gender gen ON (
                (
                    upper(trim((gen.gender_raw)::text)) = upper(trim((ifc.gender)::text))
                )
            )
        ) ON (
            (
                (
                    trim(upper((ifc.subscriber_key)::text)) = upper(trim((sent.subscriber_key)::text))
                )
                AND (
                    trim((ifc.country_code)::text) = trim((sent.country_key)::text)
                )
            )
        )
        WHERE (sent.country_key)::text = ('TW'::character varying)::text
),
--MEMBER_HIST
member_hist as 
(
    SELECT 
        'MEMBER_HIST' AS data_src,
        ifc.country_code,
        CASE
            WHEN (
                (
                    (ifc.country_code)::text = ('TW'::character varying)::text
                )
                OR (
                    (ifc.country_code IS NULL)
                    AND ('TW' IS NULL)
                )
            ) THEN 'TAIWAN'::character varying
            ELSE ifc.country_code
        END AS country_name,
        to_date((act.created_date)::timestamp without time zone) AS created_date,
        null as invoicec_created_date,
        null as invoice_completed_date,
        to_date(ifc.last_logon_time) AS last_logon_time,
        null as purchase_date,
        null as redeemed_date,
        null as redeemption_completed_date,
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
        null as channel_name,
        ifc.brand,
        null as product_name,
        null as product_category,
        CASE
            WHEN (
                (act.tier_of_mnth IS NOT NULL)
                OR (
                    (act.tier_of_mnth)::text <> (''::character varying)::text
                )
            ) THEN act.tier_of_mnth
            ELSE 'UNDEFINED'::character varying
        END AS tier,
        ifc.remaining_points,
        ifc.redeemed_points AS customer_redeemed_points,
        ifc.total_points,
        null as invoive_number,
        null as invoice_qty,
        null as invoice_type,
        null as invoice_seller_name,
        null as invoice_points,
        null as invoice_status,
        null as child_name,
        null as child_birth_month,
        null as child_birth_year,
        null as child_gender,
        null as child_number,
        null as child_age,
        null as child_group,
        null as child_flag,
        null as redeemed_order_number,
        null as redeemed_prod_name,
        null as redemption_status,
        null as redeemed_points,
        null as redeemed_qty,
        null as activity_date,
        null as local_activity_date,
        null as sent_activity_date,
        null as email_name,
        null as email_subject,
        null as sent_email_name,
        null as sent_email_subject,
        null as email_delivered_flag,
        null as is_unique,
        null as url,
        null as link_name,
        null as link_content,
        null as bounce_reason,
        null as bounce_type,
        null as bounce_description,
        ifc.crtd_dttm AS data_refresh_time,
        act.active_flag,
        NULL::integer AS inv_cnt
    FROM 
        edw_vw_sfmc_consumer_master ifc
        LEFT JOIN 
        (
            SELECT act.subscriber_key,
                to_date((act.cal_mnth_id)::text, 'YYYYMM'::text) AS created_date,
                COALESCE(his.active_flag, 'N'::character varying) AS active_flag,
                tr.tier AS tier_of_mnth
            FROM 
                (
                    (
                        (
                            SELECT derived_table1.subscriber_key,
                                derived_table1.cal_mnth_id,
                                derived_table1.rno
                            FROM 
                                (
                                    SELECT abc.subscriber_key,
                                        cal.cal_mo_1 AS cal_mnth_id,
                                        row_number() OVER(PARTITION BY abc.subscriber_key,cal.cal_mo_1 ORDER BY abc.subscriber_key,cal.cal_mo_1) AS rno
                                    FROM 
                                        (
                                            (
                                                SELECT min(itg_sfmc_consumer_master.created_date) AS created_date,
                                                    itg_sfmc_consumer_master.subscriber_key
                                                FROM itg_sfmc_consumer_master
                                                WHERE 
                                                    (
                                                        (
                                                            (itg_sfmc_consumer_master.cntry_cd)::text = 'TW'::text
                                                        )
                                                        AND (
                                                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp without time zone
                                                        )
                                                    )
                                                GROUP BY itg_sfmc_consumer_master.subscriber_key
                                            ) abc
                                            LEFT JOIN (
                                                SELECT DISTINCT edw_calendar_dim.cal_mo_1,
                                                    edw_calendar_dim.cal_day
                                                FROM edw_calendar_dim
                                            ) cal ON (
                                                (
                                                    (cal.cal_day >= to_date(abc.created_date))
                                                    AND 
                                                    (
                                                        cal.cal_day <= 
                                                        (
                                                            SELECT to_date("max"(itg_sfmc_consumer_master.crtd_dttm)) AS crtd_dttm FROM itg_sfmc_consumer_master
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                ) derived_table1
                            WHERE (derived_table1.rno = 1)
                        ) act
                        LEFT JOIN edw_vw_sfmc_active_history his ON (
                            (
                                (
                                    (his.subscriber_key)::text = (act.subscriber_key)::text
                                )
                                AND (
                                    (his.active_year_mnth)::text = ((act.cal_mnth_id)::character varying)::text
                                )
                            )
                        )
                    )
                    LEFT JOIN 
                    (
                        SELECT mt.tier_standard AS tier,
                            ti.tier_year_mnth,
                            ti.cntry_cd,
                            ti.subscriber_key
                        FROM (
                                edw_vw_sfmc_tier_history ti
                                LEFT JOIN itg_mds_tw_sfmc_member_tier mt ON (((ti.tier)::text = (mt.tier_raw)::text))
                            )
                    ) tr ON 
                    (
                        (
                            (
                                (tr.subscriber_key)::text = (act.subscriber_key)::text
                            )
                            AND (
                                ((tr.tier_year_mnth)::character varying)::text = ((act.cal_mnth_id)::character varying)::text
                            )
                        )
                    )
                )
        ) act ON 
        (
            (
                (ifc.subscriber_key)::text = (act.subscriber_key)::text
            )
        )
    WHERE (ifc.country_code)::text = ('TW'::character varying)::text
),
final as 
( 
    select * from invoice
    union all
    select * from children
    union all
    select * from redemption
    union all
    select * from others
    union all
    select * from member_hist
)
select * from final