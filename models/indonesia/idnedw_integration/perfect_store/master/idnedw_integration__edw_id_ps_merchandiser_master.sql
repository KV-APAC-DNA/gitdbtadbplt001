with source as 
(
    select * from {{ref('idnedw_integration__edw_id_ps_msl_osa')}}
),
transformed as 
(
    select
        merchandiser_id::varchar(20) as merchandiser_id,
        merchandiser_name::varchar(50) as merchandiser_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        row_number() over (partition by merchandiser_id order by input_date desc) as rn
    from source
),
final as 
(
    select
        merchandiser_id,
        merchandiser_name,
        crt_dttm
    from transformed
    where rn = 1
)
select * from final
