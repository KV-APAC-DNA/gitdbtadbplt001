{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook =" {% if is_incremental() %}
                    delete from {{this}} WHERE 0 != (select count(*) from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_sales') }})
                    and data_source = 'ORSL' AND country = 'IN'; 
                    delete from {{this}} WHERE 0 != (select count(*) from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_aveeno_zone') }})
                    and data_source = 'Aveeno_body'
                    AND country = 'IN';
                    {% endif %}"
    )
}}

with sdl_hcp360_in_iqvia_sales as 
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_sales') }}
),
sdl_hcp360_in_iqvia_aveeno_zone as
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_aveeno_zone') }}
),
cte as
(
    SELECT 
        state::varchar(50) as state,
        region::varchar(50) as region,
        product::varchar(100) as product_description,
        product_grp::varchar(50) as brand,
        pack::varchar(100) as pack_description,
        category::varchar(20) as brand_category,
        year_month::date as year_month,
        total_units::number(18,5) as total_units,
        value::number(18,5) as value,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
        'ORSL' as data_source,
        'IN' as country
    FROM sdl_hcp360_in_iqvia_sales
),
cte1 as
(
    SELECT 
        state::varchar(50) as state,
        region::varchar(50) as region,
        product::varchar(100) as product_description,
        null::varchar(50) as brand,
        pack::varchar(100) as pack_description,
        null::varchar(20) as brand_category,
        year_month::date as year_month,
        total_units::number(18,5) as total_units,
        value::number(18,5) as value,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
        'Aveeno_body' as data_source,
        'IN' as country
    FROM sdl_hcp360_in_iqvia_aveeno_zone
),
transformed as 
(
    select * from cte
    union all
    select * from cte1
)
select * from transformed
