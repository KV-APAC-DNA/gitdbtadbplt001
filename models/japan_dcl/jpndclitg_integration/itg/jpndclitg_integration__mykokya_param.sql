{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook='{{build_mykokya_param()}}'
    )
}}

with mykokya_param as (
    select * from {{source('jpdclsdl_raw', 'mykokya_param')}}
),

last_file_id as (
    select max(file_id) as last_file_id
    from {{ this }}
),
transformed as (

    select
        filename,
        purpose_type,
        upload_by,
        customer_no_type,
        'Src_File_Dt' as source_file_date,
        (
            select last_file_id + 1
            from last_file_id
        ) as next_file_id
    from mykokya_param
),
final as
(
    select
    next_file_id::number(18,0)  as file_id,
    filename::varchar(100) as filename,
    purpose_type::varchar(30) as purpose_type,
    upload_by::varchar(50) as upload_by,
    customer_no_type::varchar(30) as customer_no_type,
    source_file_date::varchar(30) as source_file_date,
    TO_CHAR(convert_timezone('Asia/Tokyo', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))), 'MM-DD-YYYY')::varchar(10) as upload_dt,
    TO_CHAR(convert_timezone('Asia/Tokyo', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))), 'hh24:mi:ss')::varchar(8) as upload_time
    from
    transformed
)
select * from final
