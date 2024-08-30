{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        unique_key=["year","mnth_no","inv_week","item_cd"],
        pre_hook=
        [           
        "{% if is_incremental() %}
                delete from {{this}} where nvl(year, 'NA') || nvl(mnth_no, 'NA') || nvl(inv_week, 'NA') || nvl(item_cd, '#') in ( select distinct nvl(substring(filename, 17, 4), 'NA') || substring(filename, 21, 2) || nvl(substring(filename, 23, 2), 'NA') || nvl(item_cd, '#') from {{ source('phlsdl_raw', 'sdl_ph_as_watsons_inventory') }}
                where filename not in (
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_null__ff')}}
                union all
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_duplicate__ff')}}
                union all
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_lookup__ff')}})
                );
        {%endif%}"]
        


    )
}}
with source as (
    select *, dense_rank() over(partition by null order by filename desc) as rnk from  {{ source('phlsdl_raw', 'sdl_ph_as_watsons_inventory') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_null__ff')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_duplicate__ff')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_lookup__ff')}}
    )
    qualify rnk =1
),
final as
(
    select 
        year::varchar(10) as year,
        mnth_no::varchar(10) as mnth_no,
        inv_week::varchar(10) as inv_week,
        item_cd::varchar(100) as item_cd,
        item_desc::varchar(255) as item_desc,
        cast(total_units as numeric(20, 4)) as total_units,
        cast(total_cost as numeric(20, 4)) as total_cost,
        cast(avg_sales_cost as numeric(20, 4)) as avg_sales_cost,
        replace(wks_sup, 'ERROR 7', '0.00')::varchar(255) as wks_sup,
        remarks::varchar(255) as remarks,
        br_ol_pl_ex::varchar(255) as br_ol_pl_ex,
        group_name::varchar(255) as group_name,
        dept_name::varchar(255) as dept_name,
        class_name::varchar(255) as class_name,
        sub_class_name::varchar(255) as sub_class_name,
        br_ol_pl_ex_subclass::varchar(255) as br_ol_pl_ex_subclass,
        subclass::varchar(255) as subclass,
        catman::varchar(255) as catman,
        item_status::varchar(255) as item_status,
        item_class::varchar(255) as item_class,
        hold_reason_code::varchar(100) as hold_reason_code,
        site_code::varchar(100) as site_code,
        site_name::varchar(255) as site_name,
        gwp::varchar(255) as gwp,
        ret_non_ret::varchar(255) as ret_non_ret,
        good_bad_13_wks::varchar(100) as good_bad_13_wks,
        filename::varchar(100) as filename,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    from source
)
select * from final
