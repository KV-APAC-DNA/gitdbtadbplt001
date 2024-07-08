with edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
itg_mds_in_international_customer_details as
(
    select * from {{ ref('inditg_integration__itg_mds_in_international_customer_details') }}
),
final as 
(
SELECT COALESCE(cust.customer_code, mds.code) AS customer_code,
    cust.region_code,
    COALESCE(mds.region_name_code, cust.region_name) AS region_name,
    cust.zone_code,
    COALESCE(mds.zone_name_code, cust.zone_name) AS zone_name,
    cust.zone_classification,
    cust.territory_code,
    COALESCE(mds.territory_name_code, cust.territory_name) AS territory_name,
    cust.territory_classification,
    cust.state_code,
    cust.state_name,
    cust.town_name,
    cust.town_classification,
    cust.city,
    cust.type_code,
    COALESCE(mds.distributor_type_code, cust.type_name) AS type_name,
    COALESCE(mds.name, cust.customer_name) AS customer_name,
    cust.customer_address1,
    cust.customer_address2,
    cust.customer_address3,
    cust.active_flag,
    cust.active_start_date,
    cust.wholesalercode,
    cust.super_stockiest,
    cust.direct_account_flag,
    cust.abi_code,
    cust.abi_name,
    cust.rds_size,
    cust.crt_dttm,
    cust.updt_dttm,
    cust.num_of_retailers,
    cust.customer_type,
    cust.psnonps,
    cust.suppliedby,
    cust.cfa,
    cust.cfa_name,
    cust.town_code
FROM (
    edw_customer_dim cust FULL JOIN itg_mds_in_international_customer_details mds ON (((cust.customer_code)::TEXT = (mds.code)::TEXT))
    )
)
select * from final
