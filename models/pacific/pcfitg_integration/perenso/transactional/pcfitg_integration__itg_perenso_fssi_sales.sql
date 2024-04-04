{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['file_name'],
        pre_hook= "delete from {{this}} where split_part(file_name, '.', 1) in (select split_part(file_name, '.', 1) from {{ source('pcfsdl_raw', 'sdl_perenso_fssi_sales') }});"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_fssi_sales') }}
),
edw_time_dim as(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
fssi as(
    select
        cast(split_part(file_name, ' ', 3) as decimal) as jj_week,
        cast(right(split_part(file_name, '.', 1), 4) as decimal) as jj_year,
        article::varchar(30) as article,
        article_description::varchar(150) as article_description,
        old_article_num::varchar(30) as old_article_num,
        ean::varchar(30) as prod_ean,
        ship_to_store::varchar(20) as store_cd,
        store_name::varchar(100) as store_name,
        sales_volume::number(18,0) as sls_unit,
        sales_value::number(10,2) as sls_value,
        'NZD'::varchar(3) as crncy,
        file_name::varchar(100) as file_name,
    from source
),
final as(
    select
        etd.cal_dt::timestamp_ntz(9) as cal_dt,
        etd.jj_mnth_id::number(18,0) as jj_mnth_id,
        fssi.*,
        current_timestamp()::timestamp_ntz(9) as crtd_dt,
        current_timestamp()::timestamp_ntz(9) as updt_dt
    from fssi
    left join (
    select
        max(cal_date) as cal_dt,
        jj_wk,
        jj_mnth_id,
        jj_year
    from edw_time_dim
    group by
        jj_wk,
        jj_mnth_id,
        jj_year
    ) as etd
    on fssi.jj_week = etd.jj_wk and fssi.jj_year = etd.jj_year
)
select * from final