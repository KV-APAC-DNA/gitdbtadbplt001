{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if var('job_to_execute') == 'th_crm_files' %}
                    delete from {{this}} where cntry_cd='TH' and crtd_dttm < (select min(crtd_dttm) from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master') }});
                    {% elif var('job_to_execute') == 'ph_crm_files' %}
                    {{build_itg_sfmc_consumer_master()}}
                    {% endif %}
                "
    )
}}
with
source as
(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master') }}
),
sdl_ph_sfmc_consumer_master as
(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from  {{ source('phlsdl_raw', 'sdl_ph_sfmc_consumer_master') }}
)
{% if var("job_to_execute") == 'th_crm_files' %}
,
final as
(
    select
        'TH'::varchar(10) as cntry_cd,
        first_name::varchar(200) as first_name,
        last_name::varchar(200) as last_name,
        mobile_num::varchar(30) as mobile_num,
        mobile_cntry_cd ::varchar(10) as mobile_cntry_cd,
        birthday_mnth::varchar(10) as birthday_mnth,
        birthday_year::varchar(10) as birthday_year,
        address_1::varchar(255) as address_1,
        address_2::varchar(255) as address_2,
        address_city::varchar(100) as address_city,
        address_zipcode::varchar(30) as address_zipcode,
        subscriber_key::varchar(100) as subscriber_key,
        website_unique_id::varchar(150) as website_unique_id,
        source::varchar(100) as source,
        medium::varchar(100) as medium,
        brand::varchar(200) as brand,
        address_cntry::varchar(100) as address_cntry,
        campaign_id::varchar(100) as campaign_id,
        created_date::timestamp_ntz(9) as created_date,
        updated_date::timestamp_ntz(9) as updated_date,
        unsubscribe_date::timestamp_ntz(9) as unsubscribe_date,
        email::varchar(100) as email,
        full_name::varchar(200) as full_name,
        last_logon_time::timestamp_ntz(9) as last_logon_time,
        remaining_points::number(10,4) as remaining_points,
        redeemed_points::number(10,4) as redeemed_points,
        total_points::number(10,4) as total_points,
        gender::varchar(20) as gender,
        line_id::varchar(50) as line_id,
        line_name::varchar(200) as line_name,
        line_email::varchar(100) as line_email,
        line_channel_id::varchar(50) as line_channel_id,
        address_region::varchar(100) as address_region,
        tier::varchar(100) as tier,
        opt_in_for_communication::varchar(100) as opt_in_for_communication,
        have_kid::varchar(20) as have_kid,
        age::number(18,0) as age,
        file_name::varchar(255) as file_name,
        null::varchar(10) as delete_flag,
        null::varchar(100) as subscriber_status,
        null::varchar(100) as opt_in_for_jnj_communication,
        null::varchar(100) as opt_in_for_campaign ,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        current_timestamp()::timestamp_ntz(9) as valid_from,
        '31-dec-9999'::timestamp_ntz(9) as valid_to
    from source
    where rnk=1
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        and source.crtd_dttm > (select max(crtd_dttm) from {{ this }})
    {% endif %}
)

select * from final
{% elif var("job_to_execute") == 'ph_crm_files' %}
,
itg_sfmc_consumer_master as
(
    select * from {{this}} 
    where file_name not in
    (
    select distinct file_name from sdl_ph_sfmc_consumer_master
    )
),
wks_ph_sfmc_consumer_master_temp1 as
(
    SELECT 
        temp.subscriber_key,
        temp.md5_sdl,
        temp.md5_itg,
        case
            when temp.md5_sdl = temp.md5_itg then 'NO_CHANGE'
            else 'UPDATED'
        END AS COMPARE
    FROM 
        (
            select itg.subscriber_key,
                md5(
                    nvl(lower(itg.subscriber_key), '') || 
                    nvl(lower(subscriber_status), '') || 
                    substring(file_name, 24, 6)
                ) md5_itg,
                md5_sdl
            from itg_sfmc_consumer_master itg,
            (
                select sdl.subscriber_key,
                    md5(
                        nvl(lower(sdl.subscriber_key), '') || nvl(lower(subscriber_status), '') || substring(file_name, 24, 6)
                    ) md5_sdl
                from sdl_ph_sfmc_consumer_master sdl
            ) as sdl
            where sdl.subscriber_key = itg.subscriber_key
            and itg.valid_to = '31-DEC-9999'
            and itg.cntry_cd = 'PH'
        ) temp
),
itg_temp as
(
    --  updating the end date in the existing ITG record 
    select
        itg.cntry_cd,
        itg.first_name,
        itg.last_name,
        itg.mobile_num,
        itg.mobile_cntry_cd,
        itg.birthday_mnth,
        itg.birthday_year,
        itg.address_1,
        itg.address_2,
        itg.address_city,
        itg.address_zipcode,
        itg.subscriber_key,
        itg.website_unique_id,
        itg.source,
        itg.medium,
        itg.brand,
        itg.address_cntry,
        itg.campaign_id,
        itg.created_date,
        itg.updated_date,
        itg.unsubscribe_date,
        itg.email,
        itg.full_name,
        itg.last_logon_time,
        itg.remaining_points,
        itg.redeemed_points,
        itg.total_points,
        itg.gender,
        itg.line_id,
        itg.line_name,
        itg.line_email,
        itg.line_channel_id,
        itg.address_region,
        itg.tier,
        itg.opt_in_for_communication,
        itg.have_kid,
        itg.age,
        itg.file_name,
        itg.crtd_dttm,
        itg.updt_dttm,
        itg.valid_from,
        case
            when temp.subscriber_key is not null
            and itg.valid_to = '31-dec-9999'
            then current_timestamp 
            else itg.valid_to
        end as valid_to,
        itg.delete_flag,
        itg.subscriber_status,
        itg.opt_in_for_jnj_communication,
        itg.opt_in_for_campaign
    from itg_sfmc_consumer_master itg
    left join wks_ph_sfmc_consumer_master_temp1 temp
    on itg.subscriber_key = temp.subscriber_key
    and temp.compare = 'UPDATED'
    where itg.cntry_cd = 'PH'
    union all
    --insert existing updated records
    select
        'PH' as cntry_cd,
        first_name,
        last_name,
        mobile_num,
        mobile_cntry_cd,
        birthday_mnth,
        birthday_year,
        address_1,
        address_2,
        address_city,
        address_zipcode,
        SDL.subscriber_key,
        website_unique_id,
        source,
        medium,
        brand,
        address_cntry,
        campaign_id,
        substring(created_date, 1, 19)::timestamp as created_date,
        substring(updated_date, 1, 19)::timestamp as updated_date,
        substring(unsubscribe_date, 1, 19)::timestamp as unsubscribe_date,
        email,
        full_name,
        substring(last_logon_time, 1, 19)::timestamp as last_logon_time,
        remaining_points,
        redeemed_points,
        total_points,
        gender,
        line_id,
        line_name,
        line_email,
        line_channel_id,
        address_region,
        tier,
        opt_in_for_communication,
        null as have_kid,
        null as age,
        file_name,
        crtd_dttm,
        current_timestamp,
        current_timestamp AS VALID_FROM,
        '31-DEC-9999' AS VALID_TO,
        null as delete_flag,
        subscriber_status,
        opt_in_for_jnj_communication,
        opt_in_for_campaign
    FROM sdl_PH_sfmc_consumer_master SDL,
        wks_ph_sfmc_consumer_master_temp1 TEMP
    where sdl.subscriber_key = temp.subscriber_key
    and temp.compare = 'UPDATED'
    union all
    --insert only new records
    select
        'PH' as cntry_cd,
        first_name,
        last_name,
        mobile_num,
        mobile_cntry_cd,
        birthday_mnth,
        birthday_year,
        address_1,
        address_2,
        address_city,
        address_zipcode,
        SDL.subscriber_key,
        website_unique_id,
        source,
        medium,
        brand,
        address_cntry,
        campaign_id,
        substring(created_date, 1, 19)::timestamp as created_date,
        substring(updated_date, 1, 19)::timestamp as updated_date,
        substring(unsubscribe_date, 1, 19)::timestamp as unsubscribe_date,
        email,
        full_name,
        substring(last_logon_time, 1, 19)::timestamp as last_logon_time,
        remaining_points,
        redeemed_points,
        total_points,
        gender,
        line_id,
        line_name,
        line_email,
        line_channel_id,
        address_region,
        tier,
        opt_in_for_communication,
        null as have_kid,
        null as age,
        file_name,
        crtd_dttm,
        current_timestamp,
        current_timestamp AS VALID_FROM,
        '31-DEC-9999' AS VALID_TO,
        null as delete_flag,
        subscriber_status,
        opt_in_for_jnj_communication,
        opt_in_for_campaign
    from sdl_ph_sfmc_consumer_master sdl
    left join wks_ph_sfmc_consumer_master_temp1 temp on sdl.subscriber_key = temp.subscriber_key
    where
        (
            temp.subscriber_key is null
            or temp.compare = 'NO_CHANGE'
        )
)
,
transformed as
(
    select
        cntry_cd,
        first_name,
        last_name,
        mobile_num,
        mobile_cntry_cd,
        birthday_mnth,
        birthday_year,
        address_1,
        address_2,
        address_city,
        address_zipcode,
        itg.subscriber_key,
        website_unique_id,
        source,
        medium,
        brand,
        address_cntry,
        campaign_id,
        created_date,
        updated_date,
        unsubscribe_date,
        email,
        full_name,
        last_logon_time,
        remaining_points,
        redeemed_points,
        total_points,
        gender,
        line_id,
        line_name,
        line_email,
        line_channel_id,
        address_region,
        tier,
        opt_in_for_communication,
        have_kid,
        age,
        file_name,
        crtd_dttm,
        current_timestamp as updt_dttm,
        valid_from,
        case 
            when delete_flag = 'Y' and valid_to = '31-DEC-9999' then current_timestamp
            else valid_to
        end as valid_to,
        case 
            when sdl.subscriber_key is null then 'Y'
            else itg.delete_flag
        end as delete_flag,
        subscriber_status,
        opt_in_for_jnj_communication,
        opt_in_for_campaign
    from
    itg_temp itg left join
    (SELECT subscriber_key FROM sdl_PH_sfmc_consumer_master) sdl
    on itg.subscriber_key = sdl.subscriber_key
),
final as
(
    select
        cntry_cd::varchar(10) as cntry_cd,
        first_name::varchar(200) as first_name,
        last_name::varchar(200) as last_name,
        mobile_num::varchar(30) as mobile_num,
        mobile_cntry_cd::varchar(10) as mobile_cntry_cd,
        birthday_mnth::varchar(10) as birthday_mnth,
        birthday_year::varchar(10) as birthday_year,
        address_1::varchar(255) as address_1,
        address_2::varchar(255) as address_2,
        address_city::varchar(100) as address_city,
        address_zipcode::varchar(30) as address_zipcode,
        subscriber_key::varchar(100) as subscriber_key,
        website_unique_id::varchar(150) as website_unique_id,
        source::varchar(100) as source,
        medium::varchar(100) as medium,
        brand::varchar(200) as brand,
        address_cntry::varchar(100) as address_cntry,
        campaign_id::varchar(100) as campaign_id,
        created_date::timestamp_ntz(9) as created_date,
        updated_date::timestamp_ntz(9) as updated_date,
        unsubscribe_date::timestamp_ntz(9) as unsubscribe_date,
        email::varchar(100) as email,
        full_name::varchar(200) as full_name,
        last_logon_time::timestamp_ntz(9) as last_logon_time,
        remaining_points::number(10,4) as remaining_points,
        redeemed_points::number(10,4) as redeemed_points,
        total_points::number(10,4) as total_points,
        gender::varchar(20) as gender,
        line_id::varchar(50) as line_id,
        line_name::varchar(200) as line_name,
        line_email::varchar(100) as line_email,
        line_channel_id::varchar(50) as line_channel_id,
        address_region::varchar(100) as address_region,
        tier::varchar(100) as tier,
        opt_in_for_communication::varchar(100) as opt_in_for_communication,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        have_kid::varchar(20) as have_kid,
        age::number(18,0) as age,
        valid_from::timestamp_ntz(9) as valid_from,
        valid_to::timestamp_ntz(9) as valid_to,
        delete_flag::varchar(10) as delete_flag,
        subscriber_status::varchar(100) as subscriber_status,
        opt_in_for_jnj_communication::varchar(100) as opt_in_for_jnj_communication,
        opt_in_for_campaign::varchar(100) as opt_in_for_campaign
    from transformed
)
select * from final

{% endif %}