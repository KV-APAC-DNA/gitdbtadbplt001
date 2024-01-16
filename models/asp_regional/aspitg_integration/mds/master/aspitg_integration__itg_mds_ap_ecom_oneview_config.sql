--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_ecom_oneview_config') }}
),

--Logical CTE

--Final CTE
final as (
    select
        dataset::varchar(100) as dataset,
        dataset_area::varchar(100) as dataset_area,
        column_name::varchar(100) as column_name,
        filter_value::varchar(100) as filter_value,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

--Final select
select * from final
