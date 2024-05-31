with 
itg_kr_sfmc_naver_data as (
select * from DEV_DNA_CORE.NTAITG_INTEGRATION.ITG_KR_SFMC_NAVER_DATA
),
itg_kr_sfmc_naver_data_additional as (
select * from DEV_DNA_CORE.NTAITG_INTEGRATION.ITG_KR_SFMC_NAVER_DATA_ADDITIONAL
),
final as (
SELECT 
  derived_table2.cntry_cd, 
  derived_table2.naver_id, 
  derived_table2.birth_year, 
  derived_table2.gender, 
  derived_table2.total_purchase_amount, 
  derived_table2.total_number_of_purchases, 
  derived_table2.membership_grade_level, 
  derived_table2.marketing_message_viewing_receiving, 
  derived_table2.coupon_usage_issuance, 
  derived_table2.number_of_reviews, 
  derived_table2.number_of_comments, 
  derived_table2.number_of_attendances, 
  derived_table2.opt_in_for_jnj_communication, 
  derived_table2.notification_subscription, 
  derived_table2.acquisition_channel, 
  derived_table2.reason_for_joining, 
  derived_table2.skin_type_body, 
  derived_table2.baby_skin_concerns, 
  derived_table2.oral_health_concerns, 
  derived_table2.skin_concerns_face, 
  derived_table2.updated_date, 
  derived_table2.membership_registration_date, 
  derived_table2.file_name, 
  derived_table2.crtd_dttm 
FROM 
  (
    (
      (
        (
          (
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
                a.updated_date, 
                a.membership_registration_date, 
                a.file_name, 
                a.crtd_dttm, 
                b.attribute_value AS acquisition_channel, 
                'NA' :: character varying AS reason_for_joining, 
                'NA' :: character varying AS skin_type_body, 
                'NA' :: character varying AS baby_skin_concerns, 
                'NA' :: character varying AS oral_health_concerns, 
                'NA' :: character varying AS skin_concerns_face 
              FROM 
                (
                  (
                    SELECT 
                      itg_kr_sfmc_naver_data.cntry_cd, 
                      itg_kr_sfmc_naver_data.naver_id, 
                      itg_kr_sfmc_naver_data.birth_year, 
                      itg_kr_sfmc_naver_data.gender, 
                      itg_kr_sfmc_naver_data.total_purchase_amount, 
                      itg_kr_sfmc_naver_data.total_number_of_purchases, 
                      itg_kr_sfmc_naver_data.membership_grade_level, 
                      itg_kr_sfmc_naver_data.marketing_message_viewing_receiving, 
                      itg_kr_sfmc_naver_data.coupon_usage_issuance, 
                      itg_kr_sfmc_naver_data.number_of_reviews, 
                      itg_kr_sfmc_naver_data.number_of_comments, 
                      itg_kr_sfmc_naver_data.number_of_attendances, 
                      itg_kr_sfmc_naver_data.opt_in_for_jnj_communication, 
                      itg_kr_sfmc_naver_data.notification_subscription, 
                      itg_kr_sfmc_naver_data.updated_date, 
                      itg_kr_sfmc_naver_data.membership_registration_date, 
                      itg_kr_sfmc_naver_data.file_name, 
                      itg_kr_sfmc_naver_data.crtd_dttm 
                    FROM 
                      itg_kr_sfmc_naver_data 
                    WHERE 
                      (
                        (
                          itg_kr_sfmc_naver_data.cntry_cd
                        ):: text = ('KR' :: character varying):: text
                      )
                  ) a 
                  LEFT JOIN (
                    SELECT 
                      itg_kr_sfmc_naver_data_additional.cntry_cd, 
                      itg_kr_sfmc_naver_data_additional.naver_id AS naver_id1, 
                      itg_kr_sfmc_naver_data_additional.attribute_name, 
                      itg_kr_sfmc_naver_data_additional.attribute_value, 
                      itg_kr_sfmc_naver_data_additional.file_name, 
                      itg_kr_sfmc_naver_data_additional.crtd_dttm 
                    FROM 
                      itg_kr_sfmc_naver_data_additional 
                    WHERE 
                      (
                        upper(
                          (
                            itg_kr_sfmc_naver_data_additional.attribute_name
                          ):: text
                        ) = 'ACQUISITION_CHANNEL' :: text
                      )
                  ) b ON (
                    (
                      (
                        (a.naver_id):: text = (b.naver_id1):: text
                      ) 
                      AND (
                        "substring"(
                          (a.file_name):: text, 
                          35, 
                          8
                        ) = "substring"(
                          (b.file_name):: text, 
                          38, 
                          8
                        )
                      )
                    )
                  )
                ) 
              UNION 
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
                a.updated_date, 
                a.membership_registration_date, 
                a.file_name, 
                a.crtd_dttm, 
                'NA' :: character varying AS acquisition_channel, 
                'NA' :: character varying AS reason_for_joining, 
                'NA' :: character varying AS skin_type_body, 
                b.attribute_value AS baby_skin_concerns, 
                'NA' :: character varying AS oral_health_concerns, 
                'NA' :: character varying AS skin_concerns_face 
              FROM 
                (
                  (
                    SELECT 
                      itg_kr_sfmc_naver_data.cntry_cd, 
                      itg_kr_sfmc_naver_data.naver_id, 
                      itg_kr_sfmc_naver_data.birth_year, 
                      itg_kr_sfmc_naver_data.gender, 
                      itg_kr_sfmc_naver_data.total_purchase_amount, 
                      itg_kr_sfmc_naver_data.total_number_of_purchases, 
                      itg_kr_sfmc_naver_data.membership_grade_level, 
                      itg_kr_sfmc_naver_data.marketing_message_viewing_receiving, 
                      itg_kr_sfmc_naver_data.coupon_usage_issuance, 
                      itg_kr_sfmc_naver_data.number_of_reviews, 
                      itg_kr_sfmc_naver_data.number_of_comments, 
                      itg_kr_sfmc_naver_data.number_of_attendances, 
                      itg_kr_sfmc_naver_data.opt_in_for_jnj_communication, 
                      itg_kr_sfmc_naver_data.notification_subscription, 
                      itg_kr_sfmc_naver_data.updated_date, 
                      itg_kr_sfmc_naver_data.membership_registration_date, 
                      itg_kr_sfmc_naver_data.file_name, 
                      itg_kr_sfmc_naver_data.crtd_dttm 
                    FROM 
                      itg_kr_sfmc_naver_data 
                    WHERE 
                      (
                        (
                          itg_kr_sfmc_naver_data.cntry_cd
                        ):: text = ('KR' :: character varying):: text
                      )
                  ) a 
                  LEFT JOIN (
                    SELECT 
                      itg_kr_sfmc_naver_data_additional.cntry_cd, 
                      itg_kr_sfmc_naver_data_additional.naver_id AS naver_id1, 
                      itg_kr_sfmc_naver_data_additional.attribute_name, 
                      itg_kr_sfmc_naver_data_additional.attribute_value, 
                      itg_kr_sfmc_naver_data_additional.file_name, 
                      itg_kr_sfmc_naver_data_additional.crtd_dttm 
                    FROM 
                      itg_kr_sfmc_naver_data_additional 
                    WHERE 
                      (
                        upper(
                          (
                            itg_kr_sfmc_naver_data_additional.attribute_name
                          ):: text
                        ) = 'BABY_SKIN_CONCERNS' :: text
                      )
                  ) b ON (
                    (
                      (
                        (a.naver_id):: text = (b.naver_id1):: text
                      ) 
                      AND (
                        "substring"(
                          (a.file_name):: text, 
                          35, 
                          8
                        ) = "substring"(
                          (b.file_name):: text, 
                          38, 
                          8
                        )
                      )
                    )
                  )
                )
            ) 
            UNION 
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
              a.updated_date, 
              a.membership_registration_date, 
              a.file_name, 
              a.crtd_dttm, 
              'NA' :: character varying AS acquisition_channel, 
              'NA' :: character varying AS reason_for_joining, 
              'NA' :: character varying AS skin_type_body, 
              'NA' :: character varying AS baby_skin_concerns, 
              b.attribute_value AS oral_health_concerns, 
              'NA' :: character varying AS skin_concerns_face 
            FROM 
              (
                (
                  SELECT 
                    itg_kr_sfmc_naver_data.cntry_cd, 
                    itg_kr_sfmc_naver_data.naver_id, 
                    itg_kr_sfmc_naver_data.birth_year, 
                    itg_kr_sfmc_naver_data.gender, 
                    itg_kr_sfmc_naver_data.total_purchase_amount, 
                    itg_kr_sfmc_naver_data.total_number_of_purchases, 
                    itg_kr_sfmc_naver_data.membership_grade_level, 
                    itg_kr_sfmc_naver_data.marketing_message_viewing_receiving, 
                    itg_kr_sfmc_naver_data.coupon_usage_issuance, 
                    itg_kr_sfmc_naver_data.number_of_reviews, 
                    itg_kr_sfmc_naver_data.number_of_comments, 
                    itg_kr_sfmc_naver_data.number_of_attendances, 
                    itg_kr_sfmc_naver_data.opt_in_for_jnj_communication, 
                    itg_kr_sfmc_naver_data.notification_subscription, 
                    itg_kr_sfmc_naver_data.updated_date, 
                    itg_kr_sfmc_naver_data.membership_registration_date, 
                    itg_kr_sfmc_naver_data.file_name, 
                    itg_kr_sfmc_naver_data.crtd_dttm 
                  FROM 
                    itg_kr_sfmc_naver_data 
                  WHERE 
                    (
                      (
                        itg_kr_sfmc_naver_data.cntry_cd
                      ):: text = ('KR' :: character varying):: text
                    )
                ) a 
                LEFT JOIN (
                  SELECT 
                    itg_kr_sfmc_naver_data_additional.cntry_cd, 
                    itg_kr_sfmc_naver_data_additional.naver_id AS naver_id1, 
                    itg_kr_sfmc_naver_data_additional.attribute_name, 
                    itg_kr_sfmc_naver_data_additional.attribute_value, 
                    itg_kr_sfmc_naver_data_additional.file_name, 
                    itg_kr_sfmc_naver_data_additional.crtd_dttm 
                  FROM 
                    itg_kr_sfmc_naver_data_additional 
                  WHERE 
                    (
                      upper(
                        (
                          itg_kr_sfmc_naver_data_additional.attribute_name
                        ):: text
                      ) = 'ORAL_HEALTH_CONCERNS' :: text
                    )
                ) b ON (
                  (
                    (
                      (a.naver_id):: text = (b.naver_id1):: text
                    ) 
                    AND (
                      "substring"(
                        (a.file_name):: text, 
                        35, 
                        8
                      ) = "substring"(
                        (b.file_name):: text, 
                        38, 
                        8
                      )
                    )
                  )
                )
              )
          ) 
          UNION 
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
            a.updated_date, 
            a.membership_registration_date, 
            a.file_name, 
            a.crtd_dttm, 
            'NA' :: character varying AS acquisition_channel, 
            b.attribute_value AS reason_for_joining, 
            'NA' :: character varying AS skin_type_body, 
            'NA' :: character varying AS baby_skin_concerns, 
            'NA' :: character varying AS oral_health_concerns, 
            'NA' :: character varying AS skin_concerns_face 
          FROM 
            (
              (
                SELECT 
                  itg_kr_sfmc_naver_data.cntry_cd, 
                  itg_kr_sfmc_naver_data.naver_id, 
                  itg_kr_sfmc_naver_data.birth_year, 
                  itg_kr_sfmc_naver_data.gender, 
                  itg_kr_sfmc_naver_data.total_purchase_amount, 
                  itg_kr_sfmc_naver_data.total_number_of_purchases, 
                  itg_kr_sfmc_naver_data.membership_grade_level, 
                  itg_kr_sfmc_naver_data.marketing_message_viewing_receiving, 
                  itg_kr_sfmc_naver_data.coupon_usage_issuance, 
                  itg_kr_sfmc_naver_data.number_of_reviews, 
                  itg_kr_sfmc_naver_data.number_of_comments, 
                  itg_kr_sfmc_naver_data.number_of_attendances, 
                  itg_kr_sfmc_naver_data.opt_in_for_jnj_communication, 
                  itg_kr_sfmc_naver_data.notification_subscription, 
                  itg_kr_sfmc_naver_data.updated_date, 
                  itg_kr_sfmc_naver_data.membership_registration_date, 
                  itg_kr_sfmc_naver_data.file_name, 
                  itg_kr_sfmc_naver_data.crtd_dttm 
                FROM 
                  itg_kr_sfmc_naver_data 
                WHERE 
                  (
                    (
                      itg_kr_sfmc_naver_data.cntry_cd
                    ):: text = ('KR' :: character varying):: text
                  )
              ) a 
              LEFT JOIN (
                SELECT 
                  itg_kr_sfmc_naver_data_additional.cntry_cd, 
                  itg_kr_sfmc_naver_data_additional.naver_id AS naver_id1, 
                  itg_kr_sfmc_naver_data_additional.attribute_name, 
                  itg_kr_sfmc_naver_data_additional.attribute_value, 
                  itg_kr_sfmc_naver_data_additional.file_name, 
                  itg_kr_sfmc_naver_data_additional.crtd_dttm 
                FROM 
                  itg_kr_sfmc_naver_data_additional 
                WHERE 
                  (
                    upper(
                      (
                        itg_kr_sfmc_naver_data_additional.attribute_name
                      ):: text
                    ) = 'REASON_FOR_JOINING' :: text
                  )
              ) b ON (
                (
                  (
                    (a.naver_id):: text = (b.naver_id1):: text
                  ) 
                  AND (
                    "substring"(
                      (a.file_name):: text, 
                      35, 
                      8
                    ) = "substring"(
                      (b.file_name):: text, 
                      38, 
                      8
                    )
                  )
                )
              )
            )
        ) 
        UNION 
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
          a.updated_date, 
          a.membership_registration_date, 
          a.file_name, 
          a.crtd_dttm, 
          'NA' :: character varying AS acquisition_channel, 
          'NA' :: character varying AS reason_for_joining, 
          'NA' :: character varying AS skin_type_body, 
          'NA' :: character varying AS baby_skin_concerns, 
          'NA' :: character varying AS oral_health_concerns, 
          b.attribute_value AS skin_concerns_face 
        FROM 
          (
            (
              SELECT 
                itg_kr_sfmc_naver_data.cntry_cd, 
                itg_kr_sfmc_naver_data.naver_id, 
                itg_kr_sfmc_naver_data.birth_year, 
                itg_kr_sfmc_naver_data.gender, 
                itg_kr_sfmc_naver_data.total_purchase_amount, 
                itg_kr_sfmc_naver_data.total_number_of_purchases, 
                itg_kr_sfmc_naver_data.membership_grade_level, 
                itg_kr_sfmc_naver_data.marketing_message_viewing_receiving, 
                itg_kr_sfmc_naver_data.coupon_usage_issuance, 
                itg_kr_sfmc_naver_data.number_of_reviews, 
                itg_kr_sfmc_naver_data.number_of_comments, 
                itg_kr_sfmc_naver_data.number_of_attendances, 
                itg_kr_sfmc_naver_data.opt_in_for_jnj_communication, 
                itg_kr_sfmc_naver_data.notification_subscription, 
                itg_kr_sfmc_naver_data.updated_date, 
                itg_kr_sfmc_naver_data.membership_registration_date, 
                itg_kr_sfmc_naver_data.file_name, 
                itg_kr_sfmc_naver_data.crtd_dttm 
              FROM 
                itg_kr_sfmc_naver_data 
              WHERE 
                (
                  (
                    itg_kr_sfmc_naver_data.cntry_cd
                  ):: text = ('KR' :: character varying):: text
                )
            ) a 
            LEFT JOIN (
              SELECT 
                itg_kr_sfmc_naver_data_additional.cntry_cd, 
                itg_kr_sfmc_naver_data_additional.naver_id AS naver_id1, 
                itg_kr_sfmc_naver_data_additional.attribute_name, 
                itg_kr_sfmc_naver_data_additional.attribute_value, 
                itg_kr_sfmc_naver_data_additional.file_name, 
                itg_kr_sfmc_naver_data_additional.crtd_dttm 
              FROM 
                itg_kr_sfmc_naver_data_additional 
              WHERE 
                (
                  upper(
                    (
                      itg_kr_sfmc_naver_data_additional.attribute_name
                    ):: text
                  ) = 'SKIN_CONCERNS_FACE' :: text
                )
            ) b ON (
              (
                (
                  (a.naver_id):: text = (b.naver_id1):: text
                ) 
                AND (
                  "substring"(
                    (a.file_name):: text, 
                    35, 
                    8
                  ) = "substring"(
                    (b.file_name):: text, 
                    38, 
                    8
                  )
                )
              )
            )
          )
      ) 
      UNION 
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
        a.updated_date, 
        a.membership_registration_date, 
        a.file_name, 
        a.crtd_dttm, 
        'NA' :: character varying AS acquisition_channel, 
        'NA' :: character varying AS reason_for_joining, 
        b.attribute_value AS skin_type_body, 
        'NA' :: character varying AS baby_skin_concerns, 
        'NA' :: character varying AS oral_health_concerns, 
        'NA' :: character varying AS skin_concerns_face 
      FROM 
        (
          (
            SELECT 
              itg_kr_sfmc_naver_data.cntry_cd, 
              itg_kr_sfmc_naver_data.naver_id, 
              itg_kr_sfmc_naver_data.birth_year, 
              itg_kr_sfmc_naver_data.gender, 
              itg_kr_sfmc_naver_data.total_purchase_amount, 
              itg_kr_sfmc_naver_data.total_number_of_purchases, 
              itg_kr_sfmc_naver_data.membership_grade_level, 
              itg_kr_sfmc_naver_data.marketing_message_viewing_receiving, 
              itg_kr_sfmc_naver_data.coupon_usage_issuance, 
              itg_kr_sfmc_naver_data.number_of_reviews, 
              itg_kr_sfmc_naver_data.number_of_comments, 
              itg_kr_sfmc_naver_data.number_of_attendances, 
              itg_kr_sfmc_naver_data.opt_in_for_jnj_communication, 
              itg_kr_sfmc_naver_data.notification_subscription, 
              itg_kr_sfmc_naver_data.updated_date, 
              itg_kr_sfmc_naver_data.membership_registration_date, 
              itg_kr_sfmc_naver_data.file_name, 
              itg_kr_sfmc_naver_data.crtd_dttm 
            FROM 
              itg_kr_sfmc_naver_data 
            WHERE 
              (
                (
                  itg_kr_sfmc_naver_data.cntry_cd
                ):: text = ('KR' :: character varying):: text
              )
          ) a 
          LEFT JOIN (
            SELECT 
              itg_kr_sfmc_naver_data_additional.cntry_cd, 
              itg_kr_sfmc_naver_data_additional.naver_id AS naver_id1, 
              itg_kr_sfmc_naver_data_additional.attribute_name, 
              itg_kr_sfmc_naver_data_additional.attribute_value, 
              itg_kr_sfmc_naver_data_additional.file_name, 
              itg_kr_sfmc_naver_data_additional.crtd_dttm 
            FROM 
              itg_kr_sfmc_naver_data_additional 
            WHERE 
              (
                upper(
                  (
                    itg_kr_sfmc_naver_data_additional.attribute_name
                  ):: text
                ) = 'SKIN_TYPE_BODY' :: text
              )
          ) b ON (
            (
              (
                (a.naver_id):: text = (b.naver_id1):: text
              ) 
              AND (
                "substring"(
                  (a.file_name):: text, 
                  35, 
                  8
                ) = "substring"(
                  (b.file_name):: text, 
                  38, 
                  8
                )
              )
            )
          )
        )
    ) 
    UNION 
    SELECT 
      derived_table1.cntry_cd, 
      derived_table1.naver_id, 
      derived_table1.birth_year, 
      derived_table1.gender, 
      derived_table1.total_purchase_amount, 
      derived_table1.total_number_of_purchases, 
      derived_table1.membership_grade_level, 
      derived_table1.marketing_message_viewing_receiving, 
      derived_table1.coupon_usage_issuance, 
      derived_table1.number_of_reviews, 
      derived_table1.number_of_comments, 
      derived_table1.number_of_attendances, 
      derived_table1.opt_in_for_jnj_communication, 
      derived_table1.notification_subscription, 
      derived_table1.updated_date, 
      derived_table1.membership_registration_date, 
      derived_table1.file_name, 
      derived_table1.crtd_dttm, 
      derived_table1.acquisition_channel, 
      derived_table1.reason_for_joining, 
      derived_table1.skin_type_body, 
      derived_table1.baby_skin_concerns, 
      derived_table1.oral_health_concerns, 
      derived_table1.skin_concerns_face 
    FROM 
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
          a.updated_date, 
          a.membership_registration_date, 
          a.file_name, 
          a.crtd_dttm, 
          'NA' :: character varying AS acquisition_channel, 
          'NA' :: character varying AS reason_for_joining, 
          'NA' :: character varying AS skin_type_body, 
          'NA' :: character varying AS baby_skin_concerns, 
          'NA' :: character varying AS oral_health_concerns, 
          'NA' :: character varying AS skin_concerns_face 
        FROM 
          (
            itg_kr_sfmc_naver_data a 
            LEFT JOIN (
              SELECT 
                DISTINCT itg_kr_sfmc_naver_data_additional.cntry_cd, 
                itg_kr_sfmc_naver_data_additional.naver_id AS naver_id1, 
                itg_kr_sfmc_naver_data_additional.file_name, 
                itg_kr_sfmc_naver_data_additional.crtd_dttm 
              FROM 
                itg_kr_sfmc_naver_data_additional
            ) b ON (
              (
                (
                  (a.naver_id):: text = (b.naver_id1):: text
                ) 
                AND (
                  "substring"(
                    (a.file_name):: text, 
                    35, 
                    8
                  ) = "substring"(
                    (b.file_name):: text, 
                    38, 
                    8
                  )
                )
              )
            )
          ) 
        WHERE 
          (b.naver_id1 IS NULL)
      ) derived_table1
  ) derived_table2
)
select * from final
