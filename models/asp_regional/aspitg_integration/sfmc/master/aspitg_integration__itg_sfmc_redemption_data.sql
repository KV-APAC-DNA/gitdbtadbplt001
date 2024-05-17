with source as(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('ntasdl_raw', 'sdl_tw_sfmc_redemption_data') }}
),

final as
(
    select
        'TW'::varchar(10) as cntry_cd,
        prod_nm::varchar(255) as prod_nm,
        redeemed_points::number(20,4) as redeemed_points,
        qty::number(20,4) as qty,
        redeemed_date::timestamp_ntz(9) as redeemed_date,
        status::varchar(100) as status,
        completed_date::timestamp_ntz(9) as completed_date,
        subscriber_key::varchar(100) as subscriber_key,
        created_date::timestamp_ntz(9) as created_date,
        order_num::varchar(50) as order_num,
        website_unique_id::varchar(50) as website_unique_id,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from source
    where rnk=1
)
select * from final