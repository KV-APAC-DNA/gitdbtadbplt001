{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where (year, month, country, subject_area) in (select distinct year, month, country, subject_area from {{ ref('aspwks_integration__wks_ecommerce_nts_regional') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ ref('aspwks_integration__wks_ecommerce_nts_regional') }}
),
final as
(
    select 
        convert_timezone('UTC',current_timestamp()) as load_date,
        year,
        month,
        country,
        gfo,
        need_state,
        brand,
        customer_name,
        ex_rt_to_usd,
        nts_lcy,
        nts_usd,
        subject_area,
        from_crncy 
    from source
)
select load_date::timestamp_ntz(9) as load_date,
    year::varchar(20) as year,
    month::varchar(20) as month,
    country::varchar(20) as country,
    gfo::varchar(255) as gfo,
    need_state::varchar(255) as need_state,
    brand::varchar(255) as brand,
    customer_name::varchar(255) as customer_name,
    ex_rt_to_usd::float as ex_rt_to_usd,
    nts_lcy::float as nts_lcy,
    nts_usd::float as nts_usd,
    subject_area::varchar(20) as subject_area,
    from_crncy::varchar(5) as from_crncy
 from final