{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['year','mnth_id','matl_num'],
        pre_hook =" delete from {{this}} itg where itg.file_name in in (select sdl.file_name from
        {{ source('myssdl_raw','sdl_my_as_watsons_inventory') }} sdl where file_name not in
            ( 
            select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_as_watsons_inventory__duplicate_test') }}
            union all
            select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_as_watsons_inventory__null_test') }}
            ) "
    )
}}

--import CTE
with source as (
    select *, dense_rank() over(partition by year,mnth_id,matl_num order by file_name desc) as rnk from {{ source('myssdl_raw','sdl_my_as_watsons_inventory') }}  where filename not in
    ( 
      select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_as_watsons_inventory__duplicate_test') }}
      union all
      select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_as_watsons_inventory__null_test') }}
    ) qualify rnk =1
),

final as (
    SELECT
        cust_cd::varchar(30) as cust_cd,
        store_cd::varchar(30) as store_cd,
        year::varchar(10) as year,
        mnth_id::varchar(10) as mnth_id,
        matl_num::varchar(100) as matl_num,
        cast(inv_qty_pc AS decimal(20, 4)) AS inv_qty_pc,
        cast(inv_value AS decimal(20, 4)) AS inv_value,
        filename::varchar(100) as filename,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final