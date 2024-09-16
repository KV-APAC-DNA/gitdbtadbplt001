{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_id','salesrep_id'],
        pre_hook= "{%if is_incremental()%}
        delete from {{this}} where (dstrbtr_id, salesrep_id) in ( select dstrbtr_id, salesrep_id from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_org_dim') }} where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__duplicate_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__test_format')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__test_format2')}}));
        {% endif %}"
    )
}}

with source as(
    select *, dense_rank() over (partition by dstrbtr_id, salesrep_id order by file_name desc) rnk 
    from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_org_dim') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__duplicate_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__test_format')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__test_format2')}}
    ) qualify rnk = 1
),
final as(
    select
        dstrbtr_id::varchar(30) as dstrbtr_id,
        salesrep_id::varchar(30) as salesrep_id,
        salesrep_name::varchar(100) as salesrep_name,
        sup_code::varchar(50) as supervisor_code,
        to_date(salesrep_crtdate,  'MM/DD/YYYY HH12:MI:SS AM') as salesrep_crtdate,
        to_date(salesrep_dateoff,  'MM/DD/YYYY HH12:MI:SS AM') as salesrep_dateoff,
        sup_name::varchar(100) as supervisor_name,
        trim(sup_active, ',')::varchar(1) as sup_active,
        to_date(sup_crtdate,  'MM/DD/YYYY HH12:MI:SS AM') as sup_crtdate,
        to_date(sup_dateoff, 'MM/DD/YYYY HH12:MI:SS AM') as sup_dateoff,
        asm_id::varchar(50) as asm_id,
        asm_name::varchar(100) as asm_name,
        trim(active, ',')::varchar(1) as active,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    from source
)
select * from final