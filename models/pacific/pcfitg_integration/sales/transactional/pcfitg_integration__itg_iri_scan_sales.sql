{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['ac_nielsencode','iri_ean','wk_end_dt'],
        pre_hook= "delete from {{this}} where (nvl(ac_nielsencode, '0') || nvl(iri_ean, '0') || nvl(wk_end_dt, '9999-12-31')) in (
        select distinct nvl(ac_nielsencode, '0') || nvl(iri_ean, '0') || nvl(to_date(wk_end_dt, 'dd/mm/yy'), '9999-12-31') from {{ source('pcfsdl_raw', 'sdl_iri_scan_sales') }});"
    )
}}
with source as 
(
    select *,
    dense_rank() over(partition by ac_nielsencode,iri_ean,wk_end_dt order by filename desc) as rnk
     from {{ source('pcfsdl_raw', 'sdl_iri_scan_sales') }}
),
final as 
(
select 
    iri_market::varchar(255) as iri_market,
    to_date(wk_end_dt, 'dd/mm/yy') as wk_end_dt,
    iri_prod_desc::varchar(255) as iri_prod_desc,
    iri_ean::varchar(100) as iri_ean,
    (scan_sales * 1000)::number(20,4) as scan_sales,
    (scan_units * 1000)::number(20,4) as scan_units,
    ac_nielsencode::varchar(100) as ac_nielsencode,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    filename::varchar(255) as filename
from source
where rnk=1
)
select * from final