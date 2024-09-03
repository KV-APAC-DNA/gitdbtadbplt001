{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['product_code'],
        pre_hook= [ 
        "{% if is_incremental() %}
            delete from {{this}} where product_code in ( select product_code from {{ source('vnmsdl_raw', 'sdl_vn_dms_product_dim') }} 
            where file_name not in (
            select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_product_dim__null_test')}}
            union all
            select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_product_dim__duplicate_test')}}));
        {% endif %}"]
    )
}}

with source as(
    select *, dense_rank() over (partition by product_code order by file_name desc) rnk 
     from {{ source('vnmsdl_raw', 'sdl_vn_dms_product_dim') }}
     where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_product_dim__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_product_dim__duplicate_test')}}
     ) qualify rnk = 1
),
final as(
    select
        product_code::varchar(50) as product_code,
        product_name::varchar(100) as product_name,
        productcodesap::varchar(40) as productcodesap,
        productnamesap::varchar(100) as productnamesap,
        unit::varchar(10) as unit,
        tax_rate::varchar(10) as tax_rate,
        weight::number(15,4) as weight,
        volume::number(15,4) as volume,
        groupjb::varchar(20) as group_jb,
        franchise::varchar(50) as franchise,
        brand::varchar(100) as brand,
        variant::varchar(100) as variant,
        product_group::varchar(200) as product_group,
        TRIM(active, ',')::varchar(1) as active,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    from source
)
select * from final