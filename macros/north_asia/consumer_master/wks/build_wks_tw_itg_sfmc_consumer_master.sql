{% macro build_wks_tw_itg_sfmc_consumer_master(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started building wks_itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

    {% set build_wks_itg_model %}
        create or replace table
        {% if target.name=='prod' %}
                        ntawks_integration.wks_itg_sfmc_consumer_master
                    {% else %}
                        {{schema}}.ntawks_integration__wks_itg_sfmc_consumer_master
                    {% endif %}
        as(
        with itg_sfmc_consumer_master_temp as
        (
            select  * from
                {% if target.name=='prod' %}
                    ntaitg_integration.itg_sfmc_consumer_master_temp
                {% else %}
                    {{schema}}.ntaitg_integration__itg_sfmc_consumer_master_temp
                {% endif %}
            where cntry_cd = 'TW'
        ),
        sdl_tw_sfmc_consumer_master as (
            select * from {{ ref('ntawks_integration__wks_sdl_tw_sfmc_consumer_master') }}
        ),
        wks_tw_sfmc_consumer_master_temp1 as
        (
            select  * from
                {% if target.name=='prod' %}
                    ntawks_integration.wks_tw_sfmc_consumer_master_temp1
                {% else %}
                    {{schema}}.ntawks_integration__wks_tw_sfmc_consumer_master_temp1
                {% endif %}
        ),
        itg_temp as
        (
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
                    and itg.valid_to = '31-DEC-9999'::timestamp_ntz
                    then current_timestamp
                    else itg.valid_to
                end as valid_to,
                itg.delete_flag
            from itg_sfmc_consumer_master_temp itg
            left join wks_tw_sfmc_consumer_master_temp1 temp
            on itg.subscriber_key = temp.subscriber_key
            and temp.compare = 'UPDATED'
        ),
        itg_active_records as
        (
            select * from itg_temp
            where not (
                subscriber_key in
                (
                    select subscriber_key from wks_tw_sfmc_consumer_master_temp1
                    where compare = 'NO_CHANGE'
                )
                and valid_to = '31-DEC-9999'::timestamp_ntz
            )
        ),
        itg_union as
        (
            select * from itg_active_records

            union all
            --insert existing updated records
            select
                'TW' as cntry_cd,
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
                created_date as created_date,
                updated_date as updated_date,
                unsubscribe_date as unsubscribe_date,
                email,
                full_name,
                last_logon_time as last_logon_time,
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
                '31-DEC-9999'::timestamp_ntz AS VALID_TO,
                null as delete_flag
            FROM sdl_tw_sfmc_consumer_master SDL,
                wks_tw_sfmc_consumer_master_temp1 TEMP
            where sdl.subscriber_key = temp.subscriber_key
            and temp.compare = 'UPDATED'
            and file_name = '{{filename}}'
            union all
            --insert only new records
            select
                'TW' as cntry_cd,
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
                created_date as created_date,
                updated_date as updated_date,
                unsubscribe_date as unsubscribe_date,
                email,
                full_name,
                last_logon_time as last_logon_time,
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
                null as delete_flag
            from sdl_tw_sfmc_consumer_master sdl
            left join wks_tw_sfmc_consumer_master_temp1 temp on sdl.subscriber_key = temp.subscriber_key
            where
                (
                    temp.subscriber_key is null
                    or temp.compare = 'NO_CHANGE'
                )
            and file_name = '{{filename}}'
        )
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
                when sdl.subscriber_key is null and valid_to = '31-DEC-9999'::timestamp_ntz then current_timestamp
                else valid_to
            end as valid_to,
            case
                when sdl.subscriber_key is null then 'Y'
                else itg.delete_flag
            end as delete_flag
        from
        itg_union itg left join
        (SELECT subscriber_key FROM sdl_tw_sfmc_consumer_master) sdl
        on itg.subscriber_key = sdl.subscriber_key
        );
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query setting completed to build wks_itg table -> wks_itg_sfmc_consumer_master for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to build wks_itg table -> wks_itg_sfmc_consumer_master for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_wks_itg_model) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to build wks_itg table -> wks_itg_sfmc_consumer_master for file: "~ filename) }}
    {{ log("Setting query to delete records from wks staging table -> wks_tw_sfmc_consumer_master for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_wks_staging_data_by_file_query %}
    delete from {{ ref('ntawks_integration__wks_sdl_tw_sfmc_consumer_master') }} where file_name= '{{filename}}';
    {% endset %}
    {{ log("Started running query to delete records from wks staging table -> wks_tw_sfmc_consumer_master for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to delete if there is no change in records") }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_wks_staging_data_by_file_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to delete records from wks staging table -> wks_tw_sfmc_consumer_master for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
{% endmacro %}