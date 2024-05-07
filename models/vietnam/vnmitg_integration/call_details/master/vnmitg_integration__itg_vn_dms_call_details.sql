{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_id', 'salesrep_id', 'outlet_id', 'visit_date', 'checkin_time', 'ordervisit'],
        pre_hook= "delete from {{this}} where (dstrbtr_id, salesrep_id, outlet_id, visit_date, checkin_time, coalesce(ordervisit,'')) in ( select dstrbtr_id, salesrep_id, outlet_id, to_date(visit_date, 'MM/DD/YYYY HH12:MI:SS AM') as visit_date, to_timestamp(checkin_time , 'MM/DD/YYYY HH12:MI:SS AM') as checkin_time, coalesce(ordervisit,'') from {{ source('vnmsdl_raw', 'sdl_vn_dms_call_details') }} );"
    )
}}

with source as(
    select *,
        dense_rank() over(partition by dstrbtr_id, salesrep_id, outlet_id, to_date(visit_date, 'MM/DD/YYYY HH12:MI:SS AM'), to_timestamp(checkin_time , 'MM/DD/YYYY HH12:MI:SS AM'), ordervisit order by source_file_name desc) as rnk
    from {{ source('vnmsdl_raw', 'sdl_vn_dms_call_details') }}
),
final as(
    select
        dstrbtr_id::varchar(30) as dstrbtr_id,
        salesrep_id::varchar(30) as salesrep_id,
        outlet_id::varchar(30) as outlet_id,
        to_date(visit_date, 'MM/DD/YYYY HH12:MI:SS AM') as visit_date,
        to_timestamp(checkin_time , 'MM/DD/YYYY HH12:MI:SS AM') as checkin_time,
        to_timestamp(checkout_time , 'MM/DD/YYYY HH12:MI:SS AM') as checkout_time,
        reason::varchar(100) as reason,
        cast(distance as int) as distance,
        ordervisit::varchar(30) as ordervisit,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id
    from source
    where rnk=1
)
select * from final