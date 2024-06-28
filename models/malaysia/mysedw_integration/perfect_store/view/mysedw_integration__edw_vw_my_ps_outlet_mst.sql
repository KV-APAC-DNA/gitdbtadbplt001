with source as 
(
    select * from snaposeitg_integration.itg_my_perfectstore_outlet_mst
),

final as 
(
    SELECT 
        outlet_no,
        name,
        zone_no,
        chain_no,
        channel_no,
        address,
        postcode,
        latitude,
        longitude
    FROM source
)
select * from final