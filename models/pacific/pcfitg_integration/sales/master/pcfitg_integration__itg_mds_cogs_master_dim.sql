{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['jj_year','sku'],
        pre_hook= "delete from {{this}} where (jj_year, ltrim(sku, 0)) in (
        select jj_year,ltrim(sku, 0) from {{ source('pcfsdl_raw', 'sdl_mds_pacific_cogs_master') }});"
    )
}}
with source as (
    select * from {{ source('pcfsdl_raw','sdl_mds_pacific_cogs_master') }}
),
final as (
select 
    trim(jj_year)::varchar(10),
    trim(sku)::varchar(100),
    'SGD'::varchar(5) as crncy,
    au_cogs_per_unit::number(31,2) as au_cogs_per_unit,
    nz_cogs_per_unit::number(31,2) as nz_cogs_per_unit,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
from source
)
select * from source