with source as
(
    select * from snapindedw_integration.edw_retailer_dim
    --{{ ref('indedw_integration__edw_retailer_dim') }}
),
final as
(
    SELECT rd.*
    FROM (SELECT ret.retailer_code,
             ret.start_date,
             ret.end_date,
             ret.customer_code,
             ret.customer_name,
             ret.retailer_name,
             ret.retailer_address1,
             ret.retailer_address2,
             ret.retailer_address3,
             ret.region_code,
             ret.region_name,
             ret.zone_code,
             ret.zone_name,
             ret.zone_classification,
             ret.territory_code,
             ret.territory_name,
             ret.territory_classification,
             ret.state_code,
             ret.state_name,
             ret.town_code,
             ret.town_name,
             ret.town_classification,
             ret.class_code,
             ret.class_desc,
             ret.outlet_type,
             ret.channel_code,
             ret.channel_name,
             ret.business_channel,
             ret.loyalty_desc,
             ret.registration_date,
             ret.status_cd,
             ret.status_desc,
             ret.csrtrcode,
             ret.crt_dttm,
             ret.updt_dttm,
             ret.actv_flg,
             ret.retailer_category_cd,
             ret.retailer_category_name,
             ret.rtrlatitude,
             ret.rtrlongitude,
             ret.rtruniquecode,
             ret.createddate,
             ret.file_rec_dt,
             row_number() OVER (PARTITION BY ret.customer_code,ret.retailer_code ORDER BY ret.end_date DESC) AS rn
      FROM source ret) rd
      WHERE rd.rn = 1
)
select * from final