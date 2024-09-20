{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['unique_id']
    )
}}

with source 
as (
    select * from {{source('jpdclsdl_raw','affiliate_cancel_receive')}}
),

final as 
    (
    select
        achievement::varchar(10) as achievement,
        click_dt::varchar(19) as click_dt,
        accrual_dt::varchar(19) as accrual_dt,
        asp::varchar(10) as asp,
        unique_id::varchar(12) as unique_id,
        media_name::varchar(100) as media_name,
        asp_control_no::varchar(100) as asp_control_no,
        sale_num::number(18,0) as sale_num,
        amount_including_tax::number(18,0) as amount_including_tax,
        amount_excluded_tax::number(18,0) as amount_excluded_tax,
        orderdate::varchar(19) as orderdate,
        webid::varchar(8) as webid,
        null::varchar(10) as status,
        'Src_File_Dt'::varchar(18) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        current_timestamp()::timestamp_ntz(9) as updated_date
    from source
)

select *
from final



    