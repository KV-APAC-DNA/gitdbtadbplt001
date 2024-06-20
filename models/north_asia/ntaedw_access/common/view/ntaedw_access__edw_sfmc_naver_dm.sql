with source as (
    select * from {{ ref('ntaedw_integration__edw_sfmc_naver_dm') }}
),
final as (
    select
        cntry_cd as "cntry_cd",
        naver_id as "naver_id",
        birth_year as "birth_year",
        gender as "gender",
        total_purchase_amount as "total_purchase_amount",
        total_number_of_purchases as "total_number_of_purchases",
        membership_grade_level as "membership_grade_level",
        marketing_message_viewing_receiving as "marketing_message_viewing_receiving",
        coupon_usage_issuance as "coupon_usage_issuance",
        number_of_reviews as "number_of_reviews",
        number_of_comments as "number_of_comments",
        number_of_attendances as "number_of_attendances",
        opt_in_for_jnj_communication as "opt_in_for_jnj_communication",
        notification_subscription as "notification_subscription",
        acquisition_channel as "acquisition_channel",
        reason_for_joining as "reason_for_joining",
        skin_type_body as "skin_type_body",
        baby_skin_concerns as "baby_skin_concerns",
        oral_health_concerns as "oral_health_concerns",
        skin_concerns_face as "skin_concerns_face",
        updated_date as "updated_date",
        membership_registration_date as "membership_registration_date",
        file_date as "file_date",
        file_name as "file_name",
        crtd_dttm as "crtd_dttm",
        age as "age",
        age_group as "age_group",
        no_of_purchase_group as "no_of_purchase_group",
        no_of_reviews_group as "no_of_reviews_group",
        no_of_comments_group as "no_of_comments_group",
        no_of_attendances_group as "no_of_attendances_group",
        msg_read as "msg_read",
        coupon_usage as "coupon_usage",
        new_update_date as "new_update_date"
    from source
)
select * from final