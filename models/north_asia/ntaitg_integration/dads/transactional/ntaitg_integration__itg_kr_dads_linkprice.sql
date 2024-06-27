{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where file_name in (select distinct file_name from {{ source('ntasdl_raw','sdl_kr_dads_linkprice') }});
        {% endif %}"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_dads_linkprice') }} 
),
final as (
    select
        campaign_name::varchar(100) as campaign_name,
        group_name::varchar(100) as group_name,
        material_id::varchar(100) as material_id,
        product_number::varchar(250) as product_number,
        product_name::varchar(100) as product_name,
        impressison_area::varchar(100) as impressison_area,
        keyword::varchar(100) as keyword,
        impression::varchar(100) as impression,
        click_count::varchar(100) as click_count,
        ctr::varchar(100) as ctr,
        impression_ranking::varchar(100) as impression_ranking,
        avg_click_rate::varchar(100) as avg_click_rate,
        consumed_cost::varchar(100) as consumed_cost,
        conversion_count::varchar(100) as conversion_count,
        conversion_rate::varchar(100) as conversion_rate,
        purchased_amount::varchar(100) as purchased_amount,
        roas::varchar(100) as roas,
        previous_roas::varchar(100) as previous_roas,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        file_date::varchar(10) as file_date
    from source
    where campaign_name is not null
)
select * from final