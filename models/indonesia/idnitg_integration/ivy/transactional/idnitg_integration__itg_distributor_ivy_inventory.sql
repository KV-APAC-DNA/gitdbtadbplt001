{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["create_dt"],
        pre_hook = "delete from {{this}} where CREATE_DT IN (SELECT DISTINCT(trim((DATEADD(DAY, -1, TO_DATE(SUBSTRING(CDL_DTTM,1,10),'YYYY-MM-DD')))))
 from {{ source('idnsdl_raw', 'sdl_distributor_ivy_inventory') }});"
    )
}}
with source as
(
    select * from {{source('idnsdl_raw','sdl_distributor_ivy_inventory')}}
),
final as 
(
    select 
        distributor_code::varchar(25) as distributor_code,
        warehouse_code::varchar(25) as warehouse_code,
        product_code::varchar(25) as product_code,
        batch_code::varchar(50) as batch_code,
        to_char(to_timestamp_ntz(to_date(batch_expiry_date, 'mm/dd/yy hh12:mi:ss am')), 'yyyy/mm/dd t00:00:00z') as batch_expiry_date,
        uom::varchar(15) as uom,
        qty::number(10,0) as qty,
        cdl_dttm::varchar(50) as create_dt,
        run_id::number(14,0) as run_id  
    from source
)

select * from final
