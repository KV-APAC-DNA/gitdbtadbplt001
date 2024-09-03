{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['ac_nielsencode','iri_ean','wk_end_dt'],
        pre_hook= "{%if is_incremental()%}
        delete from {{this}} where (nvl(ac_nielsencode, '0') || nvl(iri_ean, '0') || nvl(wk_end_dt, '9999-12-31')) in (
        select distinct nvl(ac_nielsencode, '0') || nvl(iri_ean, '0') || nvl(to_date(wk_end_dt, 'dd/mm/yy'), '9999-12-31') from {{ source('pcfsdl_raw', 'sdl_iri_scan_sales') }} where filename not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_iri_scan_sales__duplicate_test')}}
    )); {%endif%}"
    )
}}
with source as 
(
    select *,
    dense_rank() over(partition by ac_nielsencode,iri_ean,wk_end_dt order by filename desc) as rnk
    from {{ source('pcfsdl_raw', 'sdl_iri_scan_sales') }} where filename not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_iri_scan_sales__duplicate_test')}}
    ) qualify rnk = 1
),
final as 
(
select 
    iri_market::varchar(255) as iri_market,
    to_date(wk_end_dt, 'dd/mm/yy') as wk_end_dt,
    iri_prod_desc::varchar(255) as iri_prod_desc,
    iri_ean::varchar(100) as iri_ean,
    (concat(split_part(scan_sales,'.',1),'.',left(split_part(scan_sales,'.',2),4))*1000)::number(20,4) as scan_sales,
    (concat(split_part(scan_units,'.',1),'.',left(split_part(scan_units,'.',2),4))*1000)::number(20,4) as scan_units,
    numeric_distribution::number(20,4) as numeric_distribution,
	weighted_distribution::number(20,4) as weighted_distribution,
	store_count_where_scanned::number(20,4) as store_count_where_scanned,
    ac_nielsencode::varchar(100) as ac_nielsencode,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    filename::varchar(255) as filename
from source
where rnk=1
)
select * from final