{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["ean"]
    )
}}
with source_1 as 
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_gt_daiso_price') }}
),
source_2 as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_retailer_price_master') }}    
),
price_master as
(
    SELECT 
        'KR' as cntry_cd,
        dstr_nm,
        ean,
        unit_price::decimal,
        current_timestamp() as created_dt
    from source_1
),
daiso as
(
    select 
        'KR' as cntry_cd,
        trim(retailer_code) as dstr_nm,
        trim(ean) as ean,
        unit_price::decimal as unit_price,
        current_timestamp() as created_dt
    from source_2
    where upper(retailer_code) = 'DAISO'
),
final as 
(
    select 
        cntry_cd::varchar(2) as cntry_cd,
        dstr_nm::varchar(30) as dstr_nm,
        ean::varchar(50) as ean,
        unit_price::varchar(50) as unit_price,
        created_dt::timestamp_ntz(9) as created_dt
    from 
    (
        select * from daiso
        union all
        select * from price_master
        where ean not in( select distinct ean from daiso)
    )
)
select * from final