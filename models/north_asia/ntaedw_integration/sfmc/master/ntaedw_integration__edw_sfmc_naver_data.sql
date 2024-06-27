with edw_vw_sfmc_naver_data as (
    select * from {{ ref('ntaedw_integration__edw_vw_sfmc_naver_data') }}
),
final as (
select
cntry_cd::varchar(100) as cntry_cd,
naver_id::varchar(50) as naver_id,
birth_year::varchar(4) as birth_year,
gender::varchar(50) as gender,
total_purchase_amount::varchar(50) as total_purchase_amount,
total_number_of_purchases::varchar(50) as total_number_of_purchases,
membership_grade_level::varchar(50) as membership_grade_level,
marketing_message_viewing_receiving::varchar(50) as marketing_message_viewing_receiving,
coupon_usage_issuance::varchar(50) as coupon_usage_issuance,
number_of_reviews::varchar(50) as number_of_reviews,
number_of_comments::varchar(50) as number_of_comments,
number_of_attendances::varchar(50) as number_of_attendances,
opt_in_for_jnj_communication::varchar(50) as opt_in_for_jnj_communication,
notification_subscription::varchar(50) as notification_subscription,
acquisition_channel::varchar(100) as acquisition_channel,
reason_for_joining::varchar(100) as reason_for_joining,
skin_type_body::varchar(100) as skin_type_body,
baby_skin_concerns::varchar(100) as baby_skin_concerns,
oral_health_concerns::varchar(100) as oral_health_concerns,
skin_concerns_face::varchar(100) as skin_concerns_face,
updated_date::varchar(50) as updated_date,
membership_registration_date::varchar(50) as membership_registration_date,
file_name::varchar(255) as file_name,
crtd_dttm::timestamp_ntz(9) as crtd_dttm
from edw_vw_sfmc_naver_data
)
select * from final