with itg_sfmc_consumer_master as (
    select * from {{ ref('aspitg_integration__itg_sfmc_consumer_master') }}
),
itg_sfmc_consumer_master_additional as (
    select * from {{ ref('aspitg_integration__itg_sfmc_consumer_master_additional') }}
),
itg_mds_rg_sfmc_attributes as (
    select * from {{ ref('aspitg_integration__itg_mds_rg_sfmc_attributes') }}
),
tw_final as (
    select itg_sfmc_consumer_master.cntry_cd as country_code,
        itg_sfmc_consumer_master.first_name,
        itg_sfmc_consumer_master.last_name,
        itg_sfmc_consumer_master.mobile_num as mobile_number,
        itg_sfmc_consumer_master.mobile_cntry_cd as mobile_country_code,
        itg_sfmc_consumer_master.birthday_mnth as birth_month,
        itg_sfmc_consumer_master.birthday_year as birth_year,
        itg_sfmc_consumer_master.address_1,
        itg_sfmc_consumer_master.address_2,
        itg_sfmc_consumer_master.address_city,
        itg_sfmc_consumer_master.address_zipcode,
        itg_sfmc_consumer_master.subscriber_key,
        itg_sfmc_consumer_master.website_unique_id,
        itg_sfmc_consumer_master.source,
        itg_sfmc_consumer_master.medium,
        itg_sfmc_consumer_master.brand,
        itg_sfmc_consumer_master.address_cntry as address_country,
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
        null as smoker,
        null as expectant_mother,
        null as category_they_are_using,
        null as category_they_are_using_eng,
        null as skin_condition,
        null as skin_condition_eng,
        null as skin_problem,
        null as skin_problem_eng,
        null as use_mouthwash,
        null as mouthwash_time,
        null as mouthwash_time_eng,
        null as why_not_use_mouthwash,
        null as why_not_use_mouthwash_eng,
        null as oral_problem,
        null as oral_problem_eng
    from itg_sfmc_consumer_master
    where (
            (
                trim(itg_sfmc_consumer_master.cntry_cd)::text = ('TW'::varchar)::text
            )
            and (
                itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
            )
        )
),
smoker as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select 
                    itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (trim(itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text)
                        and 
                        (itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9))
                    )
            ) a
            left join 
            (
                select itg_sfmc_consumer_master_additional.attribute_value as smoker,
                    null::varchar as expectant_mother,
                    null::varchar as category_they_are_using,
                    null::varchar as category_they_are_using_eng,
                    null::varchar as skin_condition,
                    null::varchar as skin_condition_eng,
                    null::varchar as skin_problem,
                    null::varchar as skin_problem_eng,
                    null::varchar as use_mouthwash,
                    null::varchar as mouthwash_time,
                    null::varchar as mouthwash_time_eng,
                    null::varchar as why_not_use_mouthwash,
                    null::varchar as why_not_use_mouthwash_eng,
                    null::varchar as oral_problem,
                    null::varchar as oral_problem_eng,
                    itg_sfmc_consumer_master_additional.subscriber_key as subscriber_key1
                from itg_sfmc_consumer_master_additional
                where (trim(itg_sfmc_consumer_master_additional.attribute_name)::text = ('Smoker'::varchar)::text)
            ) b 
            on ((trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text))
        )
),
expectant_mother as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (
                            trim(itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        and (
                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
                        )
                    )
            ) a
            left join (
                select null::varchar as smoker,
                    itg_sfmc_consumer_master_additional.attribute_value as expectant_mother,
                    null::varchar as category_they_are_using,
                    null::varchar as category_they_are_using_eng,
                    null::varchar as skin_condition,
                    null::varchar as skin_condition_eng,
                    null::varchar as skin_problem,
                    null::varchar as skin_problem_eng,
                    null::varchar as use_mouthwash,
                    null::varchar as mouthwash_time,
                    null::varchar as mouthwash_time_eng,
                    null::varchar as why_not_use_mouthwash,
                    null::varchar as why_not_use_mouthwash_eng,
                    null::varchar as oral_problem,
                    null::varchar as oral_problem_eng,
                    itg_sfmc_consumer_master_additional.subscriber_key as subscriber_key1
                from itg_sfmc_consumer_master_additional
                where (
                        trim(itg_sfmc_consumer_master_additional.attribute_name)::text = ('Expectant_Mother'::varchar)::text
                    )
            ) b on (
                (
                    trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text
                )
            )
        )
                                    
),
category_they_are_using as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (
                            trim(itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        and (
                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
                        )
                    )
            ) a
            left join (
                select null::varchar as smoker,
                    null::varchar as expectant_mother,
                    cm.attribute_value as category_they_are_using,
                    dim.mapped_value as category_they_are_using_eng,
                    null::varchar as skin_condition,
                    null::varchar as skin_condition_eng,
                    null::varchar as skin_problem,
                    null::varchar as skin_problem_eng,
                    null::varchar as use_mouthwash,
                    null::varchar as mouthwash_time,
                    null::varchar as mouthwash_time_eng,
                    null::varchar as why_not_use_mouthwash,
                    null::varchar as why_not_use_mouthwash_eng,
                    null::varchar as oral_problem,
                    null::varchar as oral_problem_eng,
                    cm.subscriber_key as subscriber_key1
                from itg_sfmc_consumer_master_additional cm,
                    itg_mds_rg_sfmc_attributes dim
                where (
                        (
                            (
                                (
                                    trim(cm.attribute_name)::text = ('Category_they_are_using'::varchar)::text
                                )
                                and (trim(cm.attribute_name)::text = trim(dim.subject)::text)
                            )
                            and (
                                trim(cm.attribute_value)::text = trim(dim.original_value)::text
                            )
                        )
                        and (
                            trim(dim.market)::text = ('TH'::varchar)::text
                        )
                    )
            ) b on (
                (
                    trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text
                )
            )
        )
),
skin_condition as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (
                            trim(itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        and (
                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
                        )
                    )
            ) a
            left join (
                select null::varchar as smoker,
                    null::varchar as expectant_mother,
                    null::varchar as category_they_are_using,
                    null::varchar as category_they_are_using_eng,
                    cm.attribute_value as skin_condition,
                    dim.mapped_value as skin_condition_eng,
                    null::varchar as skin_problem,
                    null::varchar as skin_problem_eng,
                    null::varchar as use_mouthwash,
                    null::varchar as mouthwash_time,
                    null::varchar as mouthwash_time_eng,
                    null::varchar as why_not_use_mouthwash,
                    null::varchar as why_not_use_mouthwash_eng,
                    null::varchar as oral_problem,
                    null::varchar as oral_problem_eng,
                    cm.subscriber_key as subscriber_key1
                from itg_sfmc_consumer_master_additional cm,
                    itg_mds_rg_sfmc_attributes dim
                where (
                        (
                            (
                                (
                                    trim(cm.attribute_name)::text = ('Skin_Condition'::varchar)::text
                                )
                                and (trim(cm.attribute_name)::text = trim(dim.subject)::text)
                            )
                            and (
                                trim(cm.attribute_value)::text = trim(dim.original_value)::text
                            )
                        )
                        and (
                            trim(dim.market)::text = ('TH'::varchar)::text
                        )
                    )
            ) b on (
                (
                    trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text
                )
            )
        )
),
skin_problem as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (
                            trim(itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        and (
                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
                        )
                    )
            ) a
            left join (
                select null::varchar AS smoker,
                    null::varchar AS expectant_mother,
                    null::varchar AS category_they_are_using,
                    null::varchar AS category_they_are_using_eng,
                    null::varchar AS skin_condition,
                    null::varchar AS skin_condition_eng,
                    cm.attribute_value AS skin_problem,
                    dim.mapped_value AS skin_problem_eng,
                    null::varchar AS use_mouthwash,
                    null::varchar AS mouthwash_time,
                    null::varchar AS mouthwash_time_eng,
                    null::varchar AS why_not_use_mouthwash,
                    null::varchar AS why_not_use_mouthwash_eng,
                    null::varchar AS oral_problem,
                    null::varchar AS oral_problem_eng,
                    cm.subscriber_key AS subscriber_key1
                from itg_sfmc_consumer_master_additional cm,
                    itg_mds_rg_sfmc_attributes dim
                where (
                        (
                            (
                                (
                                    trim(cm.attribute_name)::text = ('Skin_Problem'::varchar)::text
                                )
                                and (trim(cm.attribute_name)::text = trim(dim.subject)::text)
                            )
                            and (
                                trim(cm.attribute_value)::text = trim(dim.original_value)::text
                            )
                        )
                        and (
                            trim(dim.market)::text = ('TH'::varchar)::text
                        )
                    )
            ) b on (
                (
                    trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text
                )
            )
        )
),
use_mouthwash as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (
                            (itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        and (
                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
                        )
                    )
            ) a
            left join (
                select null::varchar AS smoker,
                    null::varchar AS expectant_mother,
                    null::varchar AS category_they_are_using,
                    null::varchar AS category_they_are_using_eng,
                    null::varchar AS skin_condition,
                    null::varchar AS skin_condition_eng,
                    null::varchar AS skin_problem,
                    null::varchar AS skin_problem_eng,
                    itg_sfmc_consumer_master_additional.attribute_value AS use_mouthwash,
                    null::varchar AS mouthwash_time,
                    null::varchar AS mouthwash_time_eng,
                    null::varchar AS why_not_use_mouthwash,
                    null::varchar AS why_not_use_mouthwash_eng,
                    null::varchar AS oral_problem,
                    null::varchar AS oral_problem_eng,
                    itg_sfmc_consumer_master_additional.subscriber_key AS subscriber_key1
                from itg_sfmc_consumer_master_additional
                where (
                        trim(
                            itg_sfmc_consumer_master_additional.attribute_name
                        )::text = ('Use_Mouthwash'::varchar)::text
                    )
            ) b on (
                (
                    trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text
                )
            )
        )
),
mouthwash_time as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (
                            trim(itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        and (
                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
                        )
                    )
            ) a
            left join (
                select null::varchar AS smoker,
                    null::varchar AS expectant_mother,
                    null::varchar AS category_they_are_using,
                    null::varchar AS category_they_are_using_eng,
                    null::varchar AS skin_condition,
                    null::varchar AS skin_condition_eng,
                    null::varchar AS skin_problem,
                    null::varchar AS skin_problem_eng,
                    null::varchar AS use_mouthwash,
                    cm.attribute_value AS mouthwash_time,
                    dim.mapped_value AS mouthwash_time_eng,
                    null::varchar AS why_not_use_mouthwash,
                    null::varchar AS why_not_use_mouthwash_eng,
                    null::varchar AS oral_problem,
                    null::varchar AS oral_problem_eng,
                    cm.subscriber_key AS subscriber_key1
                from itg_sfmc_consumer_master_additional cm,
                    itg_mds_rg_sfmc_attributes dim
                where (
                        (
                            (
                                (
                                    trim(cm.attribute_name)::text = ('Mouthwash_time'::varchar)::text
                                )
                                and (trim(cm.attribute_name)::text = trim(dim.subject)::text)
                            )
                            and (
                                trim(cm.attribute_value)::text = trim(dim.original_value)::text
                            )
                        )
                        and (
                            trim(dim.market)::text = ('TH'::varchar)::text
                        )
                    )
            ) b on (
                (
                    trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text
                )
            )
        )
),
why_not_use_mouthwash as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (
                            (itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        and (
                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
                        )
                    )
            ) a
            left join (
                select null::varchar AS smoker,
                    null::varchar AS expectant_mother,
                    null::varchar AS category_they_are_using,
                    null::varchar AS category_they_are_using_eng,
                    null::varchar AS skin_condition,
                    null::varchar AS skin_condition_eng,
                    null::varchar AS skin_problem,
                    null::varchar AS skin_problem_eng,
                    null::varchar AS use_mouthwash,
                    null::varchar AS mouthwash_time,
                    null::varchar AS mouthwash_time_eng,
                    cm.attribute_value AS why_not_use_mouthwash,
                    dim.mapped_value AS why_not_use_mouthwash_eng,
                    null::varchar AS oral_problem,
                    null::varchar AS oral_problem_eng,
                    cm.subscriber_key AS subscriber_key1
                from itg_sfmc_consumer_master_additional cm,
                    itg_mds_rg_sfmc_attributes dim
                where (
                        (
                            (
                                (
                                    trim(cm.attribute_name)::text = ('Why_not_use_Mouthwash'::varchar)::text
                                )
                                and (trim(cm.attribute_name)::text = trim(dim.subject)::text)
                            )
                            and (
                                trim(cm.attribute_value)::text = trim(dim.original_value)::text
                            )
                        )
                        and (
                            trim(dim.market)::text = ('TH'::varchar)::text
                        )
                    )
            ) b on (
                (
                    trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text
                )
            )
        )
),
oral_problem as (
    select 
        a.cntry_cd,
        a.first_name,
        a.last_name,
        a.mobile_num,
        a.mobile_cntry_cd,
        a.birthday_mnth,
        a.birthday_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.file_name,
        a.crtd_dttm,
        a.updt_dttm,
        a.have_kid,
        a.age,
        b.smoker,
        b.expectant_mother,
        b.category_they_are_using,
        b.category_they_are_using_eng,
        b.skin_condition,
        b.skin_condition_eng,
        b.skin_problem,
        b.skin_problem_eng,
        b.use_mouthwash,
        b.mouthwash_time,
        b.mouthwash_time_eng,
        b.why_not_use_mouthwash,
        b.why_not_use_mouthwash_eng,
        b.oral_problem,
        b.oral_problem_eng,
        b.subscriber_key1
    from (
            (
                select itg_sfmc_consumer_master.cntry_cd,
                    itg_sfmc_consumer_master.first_name,
                    itg_sfmc_consumer_master.last_name,
                    itg_sfmc_consumer_master.mobile_num,
                    itg_sfmc_consumer_master.mobile_cntry_cd,
                    itg_sfmc_consumer_master.birthday_mnth,
                    itg_sfmc_consumer_master.birthday_year,
                    itg_sfmc_consumer_master.address_1,
                    itg_sfmc_consumer_master.address_2,
                    itg_sfmc_consumer_master.address_city,
                    itg_sfmc_consumer_master.address_zipcode,
                    itg_sfmc_consumer_master.subscriber_key,
                    itg_sfmc_consumer_master.website_unique_id,
                    itg_sfmc_consumer_master.source,
                    itg_sfmc_consumer_master.medium,
                    itg_sfmc_consumer_master.brand,
                    itg_sfmc_consumer_master.address_cntry,
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
                    itg_sfmc_consumer_master.file_name,
                    itg_sfmc_consumer_master.crtd_dttm,
                    itg_sfmc_consumer_master.updt_dttm,
                    itg_sfmc_consumer_master.have_kid,
                    itg_sfmc_consumer_master.age
                from itg_sfmc_consumer_master
                where (
                        (
                            trim(itg_sfmc_consumer_master.cntry_cd)::text = ('TH'::varchar)::text
                        )
                        and (
                            itg_sfmc_consumer_master.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
                        )
                    )
            ) a
            left join (
                select null::varchar AS smoker,
                    null::varchar AS expectant_mother,
                    null::varchar AS category_they_are_using,
                    null::varchar AS category_they_are_using_eng,
                    null::varchar AS skin_condition,
                    null::varchar AS skin_condition_eng,
                    null::varchar AS skin_problem,
                    null::varchar AS skin_problem_eng,
                    null::varchar AS use_mouthwash,
                    null::varchar AS mouthwash_time,
                    null::varchar AS mouthwash_time_eng,
                    null::varchar AS why_not_use_mouthwash,
                    null::varchar AS why_not_use_mouthwash_eng,
                    cm.attribute_value AS oral_problem,
                    dim.mapped_value AS oral_problem_eng,
                    cm.subscriber_key AS subscriber_key1
                from itg_sfmc_consumer_master_additional cm,
                    itg_mds_rg_sfmc_attributes dim
                where (
                        (
                            (
                                (
                                    trim(cm.attribute_name)::text = ('Oral_Problem'::varchar)::text
                                )
                                and (trim(cm.attribute_name)::text = trim(dim.subject)::text)
                            )
                            and (
                                trim(cm.attribute_value)::text = trim(dim.original_value)::text
                            )
                        )
                        and (
                            trim(dim.market)::text = ('TH'::varchar)::text
                        )
                    )
            ) b on (
                (
                    trim(a.subscriber_key)::text = trim(b.subscriber_key1)::text
                )
            )
        )
),
combined as (
    select * from smoker
    union all
    select * from expectant_mother
    union all
    select * from category_they_are_using
    union all
    select * from skin_condition
    union all
    select * from skin_problem
    union all
    select * from use_mouthwash
    union all
    select * from mouthwash_time
    union all
    select * from why_not_use_mouthwash
    union all
    select * from oral_problem
),
transformed as (
    select 
        abc.cntry_cd as country_code,
        abc.first_name,
        abc.last_name,
        abc.mobile_num as mobile_number,
        abc.mobile_cntry_cd as mobile_country_code,
        abc.birthday_mnth as birth_month,
        abc.birthday_year as birth_year,
        abc.address_1,
        abc.address_2,
        abc.address_city,
        abc.address_zipcode,
        abc.subscriber_key,
        abc.website_unique_id,
        abc.source,
        abc.medium,
        abc.brand,
        abc.address_cntry as address_country,
        abc.campaign_id,
        abc.created_date,
        abc.updated_date,
        abc.unsubscribe_date,
        abc.email,
        abc.full_name,
        abc.last_logon_time,
        abc.remaining_points,
        abc.redeemed_points,
        abc.total_points,
        abc.gender,
        abc.line_id,
        abc.line_name,
        abc.line_email,
        abc.line_channel_id,
        abc.address_region,
        abc.tier,
        abc.opt_in_for_communication,
        abc.crtd_dttm,
        abc.have_kid,
        abc.age,
        abc.smoker,
        abc.expectant_mother,
        abc.category_they_are_using,
        abc.category_they_are_using_eng,
        abc.skin_condition,
        abc.skin_condition_eng,
        abc.skin_problem,
        abc.skin_problem_eng,
        abc.use_mouthwash,
        abc.mouthwash_time,
        abc.mouthwash_time_eng,
        abc.why_not_use_mouthwash,
        abc.why_not_use_mouthwash_eng,
        abc.oral_problem,
        abc.oral_problem_eng
    from combined as abc
    where (
            (
                (abc.cntry_cd)::text = ('TH'::varchar)::text
            )
            and 
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (abc.smoker is not null)
                                            or (abc.expectant_mother is not null)
                                        )
                                        or (abc.category_they_are_using is not null)
                                    )
                                    or (abc.skin_condition is not null)
                                )
                                or (abc.skin_problem is not null)
                            )
                            or (abc.use_mouthwash is not null)
                        )
                        or (abc.mouthwash_time is not null)
                    )
                    or (abc.why_not_use_mouthwash is not null)
                )
                or (abc.oral_problem is not null)
            )
        )
),
th as (
    select 
        a.cntry_cd as country_code,
        a.first_name,
        a.last_name,
        a.mobile_num as mobile_number,
        a.mobile_cntry_cd as mobile_country_code,
        a.birthday_mnth as birth_month,
        a.birthday_year as birth_year,
        a.address_1,
        a.address_2,
        a.address_city,
        a.address_zipcode,
        a.subscriber_key,
        a.website_unique_id,
        a.source,
        a.medium,
        a.brand,
        a.address_cntry as address_country,
        a.campaign_id,
        a.created_date,
        a.updated_date,
        a.unsubscribe_date,
        a.email,
        a.full_name,
        a.last_logon_time,
        a.remaining_points,
        a.redeemed_points,
        a.total_points,
        a.gender,
        a.line_id,
        a.line_name,
        a.line_email,
        a.line_channel_id,
        a.address_region,
        a.tier,
        a.opt_in_for_communication,
        a.crtd_dttm,
        a.have_kid,
        a.age,
        null as smoker,
        null as expectant_mother,
        null as category_they_are_using,
        null as category_they_are_using_eng,
        null as skin_condition,
        null as skin_condition_eng,
        null as skin_problem,
        null as skin_problem_eng,
        null as use_mouthwash,
        null as mouthwash_time,
        null as mouthwash_time_eng,
        null as why_not_use_mouthwash,
        null as why_not_use_mouthwash_eng,
        null as oral_problem,
        null as oral_problem_eng
    from itg_sfmc_consumer_master a
    where (
            (
                (
                    trim(a.cntry_cd)::text = ('TH'::varchar)::text
                )
                and (
                    not (
                        trim(a.subscriber_key) in (
                            select distinct trim(itg_sfmc_consumer_master_additional.subscriber_key)
                            from itg_sfmc_consumer_master_additional
                            where (
                                    trim(itg_sfmc_consumer_master_additional.cntry_cd)::text = ('TH'::varchar)::text
                                )
                        )
                    )
                )
            )
            and (
                a.valid_to = '9999-12-31 00:00:00'::timestamp_ntz(9)
            )
        )
),
th_final as (
    select * from th
    union all
    select * from transformed
),
final as (
    select * from tw_final
    union all
    select * from th_final
)
select * from final