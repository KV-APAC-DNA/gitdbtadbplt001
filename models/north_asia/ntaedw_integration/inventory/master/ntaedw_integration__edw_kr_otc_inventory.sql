with source as 
(
    select * from snapntaitg_integration.itg_kr_otc_inventory
),
final as 
(
    select 
        mnth_id::varchar(20) as mnth_id,
        matl_num::varchar(50) as matl_num,
        brand::varchar(255) as brand,
        product_name::varchar(255) as product_name,
        distributor_cd::varchar(50) as distributor_cd,
        unit_price::number(20,4) as unit_price,
        inv_qty::number(20,4) as inv_qty,
        inv_amt::number(20,4) as inv_amt,
        filename::varchar(255) as filename,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final