with itg_sfmc_consumer_master as (
select * from {{ ref('aspitg_integration__itg_sfmc_consumer_master') }} 
),
itg_sfmc_consumer_master_additional as (
select * from {{ ref('aspitg_integration__itg_sfmc_consumer_master_additional') }}
),
final as (
SELECT 
  itg_sfmc_consumer_master.cntry_cd AS country_code, 
  itg_sfmc_consumer_master.first_name, 
  itg_sfmc_consumer_master.last_name, 
  itg_sfmc_consumer_master.mobile_num AS mobile_number, 
  itg_sfmc_consumer_master.mobile_cntry_cd AS mobile_country_code, 
  itg_sfmc_consumer_master.birthday_mnth AS birth_month, 
  itg_sfmc_consumer_master.birthday_year AS birth_year, 
  itg_sfmc_consumer_master.address_1, 
  itg_sfmc_consumer_master.address_2, 
  itg_sfmc_consumer_master.address_city, 
  itg_sfmc_consumer_master.address_zipcode, 
  itg_sfmc_consumer_master.subscriber_key, 
  itg_sfmc_consumer_master.website_unique_id, 
  itg_sfmc_consumer_master.source, 
  itg_sfmc_consumer_master.medium, 
  itg_sfmc_consumer_master.brand, 
  itg_sfmc_consumer_master.address_cntry AS address_country, 
  itg_sfmc_consumer_master.campaign_id, 
  itg_sfmc_consumer_master.created_date, 
  itg_sfmc_consumer_master.updated_date, 
  itg_sfmc_consumer_master.unsubscribe_date, 
  itg_sfmc_consumer_master.email, 
  itg_sfmc_consumer_master.full_name, 
  itg_sfmc_consumer_master.last_logon_time, 
  itg_sfmc_consumer_master.remaining_points, 
  itg_sfmc_consumer_master.redeemed_points, 
  itg_sfmc_consumer_master.total_points, 
  itg_sfmc_consumer_master.gender, 
  itg_sfmc_consumer_master.line_id, 
  itg_sfmc_consumer_master.line_name, 
  itg_sfmc_consumer_master.line_email, 
  itg_sfmc_consumer_master.line_channel_id, 
  itg_sfmc_consumer_master.address_region, 
  itg_sfmc_consumer_master.tier, 
  itg_sfmc_consumer_master.opt_in_for_communication, 
  itg_sfmc_consumer_master.crtd_dttm, 
  itg_sfmc_consumer_master.have_kid, 
  itg_sfmc_consumer_master.age, 
  null AS smoker, 
  null AS expectant_mother, 
  null AS category_they_are_using, 
  null AS category_they_are_using_eng, 
  null AS skin_condition, 
  null AS skin_condition_eng, 
  null AS skin_problem, 
  null AS skin_problem_eng, 
  null AS use_mouthwash, 
  null AS mouthwash_time, 
  null AS mouthwash_time_eng, 
  null AS why_not_use_mouthwash, 
  null AS why_not_use_mouthwash_eng, 
  null AS oral_problem, 
  null AS oral_problem_eng, 
  itg_sfmc_consumer_master.subscriber_status, 
  itg_sfmc_consumer_master.opt_in_for_jnj_communication, 
  itg_sfmc_consumer_master.opt_in_for_campaign, 
  b.attribute_value AS viber_engaged 
FROM 
  
    itg_sfmc_consumer_master itg_sfmc_consumer_master 
    LEFT JOIN itg_sfmc_consumer_master_additional b ON 
    itg_sfmc_consumer_master.subscriber_key::text = b.subscriber_key:: text
  
WHERE 
 
        itg_sfmc_consumer_master.cntry_cd:: text = 'PH'::text
    
    AND 
      itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00' :: timestamp without time zone
  
  )
  select * from final