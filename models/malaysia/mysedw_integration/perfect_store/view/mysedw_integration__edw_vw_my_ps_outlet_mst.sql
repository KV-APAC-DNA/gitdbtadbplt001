with source as 
(
    select * from  {{ ref('mysitg_integration__itg_my_perfectstore_outlet_mst') }}
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