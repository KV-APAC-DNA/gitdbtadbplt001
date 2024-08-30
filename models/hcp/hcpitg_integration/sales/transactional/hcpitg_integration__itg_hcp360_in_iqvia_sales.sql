{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook =" {% if is_incremental() %}
                    delete from {{this}} WHERE 0 != (select count(*) 
                    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_sales') }}
                    where filename not in (
                    select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_iqvia_sales__test_format') }} )
                    )
                    and upper(data_source) = 'ORSL' AND country = 'IN'; 
                     delete from {{this}} WHERE 0 != (select count(*) 
                    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_aveeno_zone') }}
                    where filename not in (
                    select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_iqvia_aveeno_zone__test_format') }}  )
                    )
                    and upper(data_source) in (select distinct upper(substring(sheet_name,0,11)) 
                    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_aveeno_zone') }}
                    where filename not in (
                    select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_iqvia_aveeno_zone__test_format') }} )
                    );
                    {% endif %}"
    )
}}

with sdl_hcp360_in_iqvia_sales as 
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_sales') }}
     where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_iqvia_sales__test_format') }}
    )
),
sdl_hcp360_in_iqvia_aveeno_zone as
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_aveeno_zone') }}
    where filename not in (select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_iqvia_aveeno_zone__test_format') }} )
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
        'IN' as country,
        filename as filename
    FROM sdl_hcp360_in_iqvia_sales
),
aveeno_zone_transformed as(
    SELECT a.state as state,
        a.region as region,
        a.product as product,
        a.pack as pack,
        case
         WHEN UPPER(SUBSTRING(a.year_month,0,3))='JAN' THEN TO_DATE('01/01/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='FEB' THEN TO_DATE('01/02/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='MAR' THEN TO_DATE('01/03/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='APR' THEN TO_DATE('01/04/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='MAY' THEN TO_DATE('01/05/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='JUN' THEN TO_DATE('01/06/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='JUL' THEN TO_DATE('01/07/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='AUG' THEN TO_DATE('01/08/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='SEP' THEN TO_DATE('01/09/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='OCT' THEN TO_DATE('01/10/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='NOV' THEN TO_DATE('01/11/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')
            WHEN UPPER(SUBSTRING(a.year_month,0,3))='DEC' THEN TO_DATE('01/12/' || SUBSTRING(a.year_month,5,4),'DD/MM/YYYY')            ELSE null
            END as year_month,
        replace(a.qty,',','') as total_units,
        replace(b.qty,',','') as value,
        a.crt_dttm as crt_dttm,
        a.filename as filename,
        substring(a.sheet_name,0,11) as sheet_name
    FROM sdl_hcp360_in_iqvia_aveeno_zone a, sdl_hcp360_in_iqvia_aveeno_zone b
    WHERE a.data_source in ('Total_Units', 'Rxns')
    AND   b.data_source in ('Value', 'Rxers')
    AND   a.line_no = b.line_no
    AND   a.region = b.region
    AND   a.product = b.product
    AND   a.pack = b.pack
    AND   a.state = b.state
    AND   a.year_month = b.year_month
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
        to_date(year_month) as year_month,
        total_units::number(18,5) as total_units,
        value::number(18,5) as value,
        crt_dttm as crt_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
        sheet_name as data_source,
        'IN' as country,
        filename as filename
    FROM aveeno_zone_transformed
),
transformed as 
(
    select * from cte
    union all
    select * from cte1
)
select * from transformed
