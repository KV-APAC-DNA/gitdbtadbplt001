{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " {% if is_incremental() %}
                    delete from {{this}} where file_name in 
                    (select distinct file_name from {{ source('thasdl_raw', 'sdl_jnj_osa_oos_report') }}
                    where file_name not in (
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_osa_oos_report__null_test') }}
                    union all
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_osa_oos_report__test_date_format_odd_eve') }}
                    )
                    )
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_osa_oos_report') }}
    where file_name not in (
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_osa_oos_report__null_test') }}
                    union all
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_osa_oos_report__test_date_format_odd_eve') }}
                    )
),
final as(
    select
        osa_oos_date::varchar(255) as osa_oos_date,
        week::varchar(255) as week,
        emp_address_pc::varchar(255) as emp_address_pc,
        pc_name::varchar(255) as pc_name,
        emp_address_supervisor::varchar(255) as emp_address_supervisor,
        supervisor_name::varchar(255) as supervisor_name,
        area::varchar(255) as area,
        channel::varchar(255) as channel,
        account::varchar(255) as account,
        store_id::varchar(255) as store_id,
        store_name::varchar(255) as store_name,
        shop_type::varchar(255) as shop_type,
        brand::varchar(255) as brand,
        category::varchar(255) as category,
        barcode::varchar(255) as barcode,
        trim(sku)::varchar(255) as sku,
        msl_price_tag::varchar(255) as msl_price_tag,
        oos::varchar(255) as oos,
        oos_reason::varchar(255) as oos_reason,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(255) as yearmo,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
  from source
)
select * from final