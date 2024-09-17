{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where file_name in (select distinct file_name from 
        {{ source('ntasdl_raw','sdl_kr_dads_naver_search_channel') }} where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_dl_kr_dads_naver_search_channel__format_test') }})
        );
        {% endif %}"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_dads_naver_search_channel') }} where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_dl_kr_dads_naver_search_channel__format_test') }})
),
final as (
    select
        channel_properties::varchar(250) as channel_property,
        channel_groups::varchar(250) as channel_group,
        channel_name::varchar(250) as channel_name,
        keyword::varchar(250) as keyword,
        customers::varchar(250) as customer_count,
        inlet_water::varchar(250) as inflow_count,
        number_of_pages::varchar(250) as page_count,
        pages_per_inflow::varchar(250) as pages_per_inflow,
        number_of_payments::varchar(250) as number_of_payment,
        payment_rate_per_inflow::varchar(250) as payment_rate_per_inflow,
        payment_amount::varchar(250) as payment_amount,
        payment_amount_per_inflow::varchar(250) as payment_amount_per_inflow,
        no_of_payments_14d::varchar(250) as count_of_payments_14d,
        payment_rate_per_inflow_14d::varchar(250) as payment_rate_per_inflow_14d,
        payment_amount_14d::varchar(250) as payment_amount_14d,
        payment_amount_per_inflow_14d::varchar(250) as payment_amount_per_inflow_14d,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm,
        file_date::varchar(10) as file_date
    from source
)
select * from final