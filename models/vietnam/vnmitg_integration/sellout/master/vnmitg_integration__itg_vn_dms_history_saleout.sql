{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['user_id','rsm_name','group_jb','franchise','brand', 'variant', 'product_group', 'dmsproduct_group', 'product_code', 'productcodesap', 'dmsproductid', 'sku_name', 'tax', 'province', 'cycle'],
        pre_hook= "{% if is_incremental()%}
        delete from {{this}} where (user_id, rsm_name, group_jb, franchise, brand, variant, product_group, dmsproduct_group, product_code, productcodesap, dmsproductid, sku_name, tax, province, cycle) in ( select user_id, rsm_name, group_jb, franchise, brand, variant, product_group, dmsproduct_group, product_code, productcodesap, dmsproductid, sku_name, cast(tax as decimal(15, 4)), province, cast(cycle as int) from {{ source('vnmsdl_raw', 'sdl_vn_dms_history_saleout') }} 
        where SOURCE_FILE_NAME not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_history_saleout__duplicate_test')}}
        ));
        {% endif %}"
    )
}}

with source as(
    select *, 
    dense_rank() over (partition by user_id,rsm_name,group_jb,franchise,brand, variant, product_group, dmsproduct_group, product_code, productcodesap, dmsproductid,sku_name, tax, province, cycle order by SOURCE_FILE_NAME desc) rnk
    from {{ source('vnmsdl_raw', 'sdl_vn_dms_history_saleout') }}
    where SOURCE_FILE_NAME not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_history_saleout__duplicate_test')}}
    ) qualify rnk = 1
),
final as(
    select 
        user_id::varchar(100) as user_id,
        rsm_name::varchar(200) as rsm_name,
        group_jb::varchar(50) as group_jb,
        franchise::varchar(50) as franchise,
        brand::varchar(100) as brand,
        variant::varchar(100) as variant,
        product_group::varchar(200) as product_group,
        dmsproduct_group::varchar(200) as dmsproduct_group,
        product_code::varchar(50) as product_code,
        productcodesap::varchar(40) as productcodesap,
        dmsproductid::varchar(100) as dmsproductid,
        sku_name::varchar(100) as sku_name,
        tax::number(15,4) as tax,
        province::varchar(100) as province,
        cycle::number(18,0) as cycle,
        sellout_afvat_bfdisc::number(15,4) as sellout_afvat_bfdisc,
        sellout_afvat_afdisc::number(15,4) as sellout_afvat_afdisc,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id,
        SOURCE_FILE_NAME as file_name
    from source
)
select * from final