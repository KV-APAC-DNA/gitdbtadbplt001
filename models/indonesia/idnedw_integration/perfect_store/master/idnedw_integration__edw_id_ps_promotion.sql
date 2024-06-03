with source as
(
    select * from {{ ref('idnitg_integration__itg_id_ps_promotion') }}
),
final as
(
    select
    outlet_id::varchar(10) as outlet_id,
    outlet_name::varchar(100) as outlet_name,
    province::varchar(50) as province,
    city::varchar(50) as city,
    channel::varchar(50) as channel,
    merchandiser_id::varchar(20) as merchandiser_id,
    merchandiser_name::varchar(50) as merchandiser_name,
    cust_group::varchar(50) as cust_group,
    to_date(input_date) as input_date,
    day_name::varchar(20) as day_name,
    promo_desc::varchar(255) as promo_desc,
    photo_link::varchar(100) as photo_link,
    posm_execution_flag::varchar(5) as posm_execution_flag,
    crt_dttm::timestamp_ntz as crt_dttm
	from source
)
select * from final