{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_gt_schedule') }}
),
final as(
    select   
        cntry_cd as cntry_cd,
        crncy_cd as crncy_cd,
        employeeid as employeeid,
        routeid as routeid,
        schedule_date as schedule_date,
        approved as approved,
        saleunit as saleunit,
        filename as filename,
        run_id as run_id,
        crt_dttm as crt_dttm
   from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)
select * from final