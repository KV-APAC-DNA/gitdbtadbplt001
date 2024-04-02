with source as(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_sellin_exception') }}
),
final as(
    select 
        data_source::varchar(6) as data_source,
        data_type::varchar(7) as data_type,
        mnth_id::varchar(23) as mnth_id,
        productid::varchar(100) as productid,
        product_name::varchar(500) as product_name,
        barcode::varchar(100) as barcode,
        custcode::varchar(200) as custcode,
        name::varchar(500) as name,
        sub_channel::varchar(1125) as sub_channel,
        "region"::varchar(1125) as region,
        province::varchar(1125) as province,
        kam::varchar(450) as kam,
        retail_environment::varchar(450) as retail_environment,
        group_account::varchar(1125) as group_account,
        "account"::varchar(1125) as account,
        franchise::varchar(1125) as franchise,
        category::varchar(1125) as category,
        sub_category::varchar(1125) as sub_category,
        sub_brand::varchar(900) as sub_brand,
        sales_amt_lcy::number(38,5) as sales_amt_lcy,
        sales_amt_usd::number(38,10) as sales_amt_usd,
        target_lcy::number(28,10) as target_lcy,
        target_usd::number(28,10) as target_usd
    from source
)
select * from final