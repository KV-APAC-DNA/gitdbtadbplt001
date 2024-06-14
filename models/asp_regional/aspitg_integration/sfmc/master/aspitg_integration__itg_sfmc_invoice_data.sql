with source as(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('ntasdl_raw','sdl_tw_sfmc_invoice_data') }}
),
final as(
    select
        'TW'::varchar(10) as cntry_cd,
        purchase_date::timestamp_ntz(9) as purchase_date,
        channel::varchar(200) as channel,
        product::varchar(200) as product,
        status::varchar(50) as status,
        created_date::timestamp_ntz(9) as created_date,
        completed_date::timestamp_ntz(9) as completed_date,
        subscriber_key::varchar(100) as subscriber_key,
        points::number(18,0) as points,
        show_record::varchar(20) as show_record,
        qty::number(20,4) as qty,
        invoice_type::varchar(200) as invoice_type,
        seller_nm::varchar(255) as seller_nm,
        product_category::varchar(200) as product_category,
        website_unique_id::varchar(150) as website_unique_id,
        invoice_num::varchar(50) as invoice_num,
        epsilon_price_per_unit::number(20,4) as epsilon_price_per_unit,
        epsilon_amount::number(20,4) as epsilon_amount,
        epsilon_total_amount::number(20,4) as epsilon_total_amount,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where rnk=1
)
select * from final
