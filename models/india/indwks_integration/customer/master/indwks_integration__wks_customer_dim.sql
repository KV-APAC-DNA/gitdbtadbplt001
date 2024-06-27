with itg_xdm_distributor as
(
    select * from {{ ref('inditg_integration__itg_xdm_distributor') }}
),
itg_xdm_distributor_supplier as
(
    select * from {{ ref('inditg_integration__itg_xdm_distributor_supplier') }}
),
itg_xdm_supplier as
(
    select * from {{ ref('inditg_integration__itg_xdm_supplier') }}
),
itg_xdm_salesheirarchy as
(
    select * from {{ ref('inditg_integration__itg_xdm_salesheirarchy') }}
),
itg_xdm_geohierarchy as
(
    select * from {{ ref('inditg_integration__itg_xdm_geohierarchy') }}
),
itg_xdm_channelmaster as
(
    select * from {{ ref('inditg_integration__itg_xdm_channelmaster') }}
),
edw_customer_dim as
(
    select * from {{ source('indedw_integration', 'edw_customer_dim') }}
),
final as
(
    SELECT src.customer_code,
       region_code,
       region_name,
       zone_code,
       zone_name,
       zone_classification,
       territory_code,
       territory_name,
       territory_classification,
       state_code,
       state_name,
       town_code,
       town_name,
       town_classification,
       city,
       type_code,
       type_name,
       customer_name,
       customer_address1,
       customer_address2,
       customer_address3,
       active_flag,
       active_start_date,
       wholesalercode,
       super_stockiest,
       direct_account_flag,
       abi_code,
       abi_name,
       rds_size,
       CASE
         WHEN tgt.crt_dttm IS NULL THEN src.crt_dttm
         ELSE tgt.crt_dttm
       END AS crt_dttm,
       CASE
         WHEN tgt.crt_dttm IS NULL THEN NULL
         ELSE convert_timezone('UTC',current_timestamp())::timestamp_ntz
       END AS updt_dttm,
       CASE
         WHEN tgt.crt_dttm IS NULL THEN 'I'
         ELSE 'U'
       END AS chng_flg,
       psnonps,
       suppliedby,
       cfa,
       cfa_name
        FROM (SELECT rd.UserCode AS customer_code,
                    CASE
                    WHEN hie.RSMCode IS NULL THEN '-1'
                    ELSE hie.RSMCode
                    END AS region_code,
                    COALESCE(hie.RSMName,'Unknown') AS region_name,
                    CASE
                    WHEN hie.FLMASMCode IS NULL THEN '-1'
                    ELSE hie.FLMASMCode
                    END AS zone_code,
                    COALESCE(hie.FLMASMName,'Unknown') AS zone_name,
                    NULL AS zone_classification,
                    CASE
                    WHEN hie.ABICode IS NULL THEN '-1'
                    ELSE hie.ABICode
                    END AS territory_code,
                    COALESCE(hie.ABIName,'Unknown') AS territory_name,
                    NULL AS territory_classification,
                    CASE
                    WHEN GEO.StateCode IS NULL THEN '-1'
                    ELSE GEO.StateCode
                    END AS state_code,
                    COALESCE(geo.StateName,'Unknown') AS state_name,
                    CASE
                    WHEN geo.CityCode IS NULL THEN '-1'
                    ELSE geo.CityCode
                    END AS town_code,
                    COALESCE(geo.CityName,'Unknown') AS town_name,
                    NULL AS town_classification,
                    GEO.CityName AS city,
                    CASE
                    WHEN RD.TypeCode IS NULL THEN '-1'
                    ELSE RD.TypeCode
                    END AS type_code,
                    COALESCE(cm.ChannelName,'Unknown') AS type_name,
                    rd.DistrName AS customer_name,
                    COALESCE(rd.DistrBrAddr1,'Unknown') AS customer_address1,
                    COALESCE(rd.DistrBrAddr2,'Unknown') AS customer_address2,
                    COALESCE(rd.DistrBrAddr3,'Unknown') AS customer_address3,
                    rd.IsActive AS active_flag,
                    TO_TIMESTAMP(NULL,'YYYY-MM-DD HH24:MI:SS') AS active_start_date,
                    NULL AS wholesalercode,
                    CASE
                    WHEN cm.clicktype = 8 THEN 'Y'
                    ELSE 'N'
                    END AS super_stockiest,
                    COALESCE(rd.isdirectacct,'Unknown') AS direct_account_flag,
                    CASE
                    WHEN CAST(hie.ABICode AS INTEGER) IS NULL THEN -1
                    ELSE CAST(hie.ABICode AS INTEGER)
                    END AS abi_code,
                    hie.ABIName AS abi_name,
                    NULL AS rds_size,
                    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS crt_dttm,
                    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS updt_dttm,
                    rd.PSnonPS,
                    dsup.DistrCode AS suppliedby,
                    SUBSTRING(dsup.SupCode,4,4) AS cfa, --- New column , CFA added on 13082021.Change as per AEBU-7333, IN Sprint 7              
                    sup.supname AS cfa_name
            FROM itg_xdm_distributor rd
                LEFT JOIN itg_xdm_distributor_supplier dsup
                    ON rd.usercode = dsup.DistrCode
                    AND IsDefault = 'Y'
                LEFT JOIN itg_xdm_supplier sup ON dsup.SupCode = sup.SupCode
                LEFT JOIN itg_xdm_salesheirarchy hie ON rd.usercode = hie.DistributorCode
                LEFT JOIN itg_xdm_geohierarchy geo ON rd.usercode = geo.DistributorCode
                LEFT JOIN itg_xdm_channelmaster cm ON rd.TypeCode = cm.ClickType) src
        LEFT OUTER JOIN (SELECT customer_code, crt_dttm FROM edw_customer_dim) tgt ON src.customer_code = tgt.customer_code
 
)

select customer_code::varchar(50) as customer_code,
    region_code::varchar(50) as region_code,
    region_name::varchar(100) as region_name,
    zone_code::varchar(50) as zone_code,
    zone_name::varchar(100) as zone_name,
    zone_classification::varchar(1) as zone_classification,
    territory_code::varchar(50) as territory_code,
    territory_name::varchar(100) as territory_name,
    territory_classification::varchar(1) as territory_classification,
    state_code::varchar(50) as state_code,
    state_name::varchar(100) as state_name,
    town_code::varchar(50) as town_code,
    town_name::varchar(100) as town_name,
    town_classification::varchar(1) as town_classification,
    city::varchar(100) as city,
    type_code::varchar(50) as type_code,
    type_name::varchar(100) as type_name,
    customer_name::varchar(150) as customer_name,
    customer_address1::varchar(200) as customer_address1,
    customer_address2::varchar(200) as customer_address2,
    customer_address3::varchar(200) as customer_address3,
    active_flag::varchar(1) as active_flag,
    active_start_date::timestamp_tz(9) as active_start_date,
    wholesalercode::varchar(1) as wholesalercode,
    super_stockiest::varchar(1) as super_stockiest,
    direct_account_flag::varchar(7) as direct_account_flag,
    abi_code::number(18,0) as abi_code,
    abi_name::varchar(100) as abi_name,
    rds_size::varchar(1) as rds_size,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    chng_flg::varchar(1) as chng_flg,
    psnonps::varchar(1) as psnonps,
    suppliedby::varchar(50) as suppliedby,
    cfa::varchar(16) as cfa,
    cfa_name::varchar(100) as cfa_name
 from final