
--Import CTE
with source as (
    select *
    from {{ source('sgpsdl_raw', 'sdl_sg_zuellig_customer_mapping') }}
    where file_name not in 
    (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_sg_zuellig_customer_mapping__null_test') }}
        union all
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_sg_zuellig_customer_mapping__duplicate_test') }}
        union all
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_sg_zuellig_customer_mapping__lookup_test') }}
    )
),

--Logical CTE

--Final CTE
final as (
    select
  zuellig_customer::varchar(255) as zuellig_customer,
  regional_banner::varchar(255) as regional_banner,
  merchandizing::varchar(255) as merchandizing,
  cdl_dttm::varchar(20) as cdl_dttm,
  curr_dt::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  file_name::varchar(255) as file_name,
  run_id::number(14,0) as run_id
  from source
)

--Final select
select * from final
