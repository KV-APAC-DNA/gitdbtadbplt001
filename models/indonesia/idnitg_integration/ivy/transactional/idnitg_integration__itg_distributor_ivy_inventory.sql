{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["create_dt"],
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where CREATE_DT IN (SELECT DISTINCT(trim((DATEADD(DAY, -1, TO_DATE(SUBSTRING(CDL_DTTM,1,10),'YYYY-MM-DD')))))
        from {{ source('idnsdl_raw', 'sdl_distributor_ivy_inventory') }});
        {% endif %}"
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
        TO_TIMESTAMP_NTZ(TO_DATE(TO_CHAR(BATCH_EXPIRY_DATE), 'MM/DD/YY HH12:MI:SS AM')) as batch_expiry_date,
        uom::varchar(15) as uom,
        qty::number(10,0) as qty,
        trim((DATEADD(DAY, -1, TO_DATE(SUBSTRING(CDL_DTTM,1,10),'YYYY-MM-DD'))))::varchar(50) as create_dt,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    from source
)

select * from final