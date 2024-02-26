--Import CTE
with source as (
    select * from {{ source('mds_access', 'mds_gcgh_geographyhierarchy') }}
),

--Logical CTE

--Final CTE
final as (
    select 
    region_name::varchar(50) as region ,
    cluster1_name::varchar(30) as cluster ,
    subcluster_name::varchar(30) as subcluster ,
    gcgh_country_mst_name::varchar(30) as market ,
    case gcgh_country_mst_name
    WHEN 'Australia' THEN 'AU'
    WHEN 'Bangladesh' THEN 'BD'
    WHEN 'Bhutan' THEN 'BT'
    WHEN 'Brunei' THEN 'BN'
    WHEN 'Cambodia' THEN 'KH'
    WHEN 'China' THEN 'CN'
    WHEN 'Federated States of Micronesia' THEN 'FM'
    WHEN 'Fiji' THEN 'FJ'
    WHEN 'Hong Kong SAR' THEN 'HK'
    WHEN 'India' THEN 'IN'
    WHEN 'Indonesia' THEN 'ID'
    WHEN 'Japan' THEN 'JP'
    WHEN 'Kiribati' THEN 'KI'
    WHEN 'Laos' THEN 'LA'
    WHEN 'Macau' THEN 'MO'
    WHEN 'Malaysia' THEN 'MY'
    WHEN 'Maldives' THEN 'MV'
    WHEN 'Marshall Islands' THEN 'MH'
    WHEN 'Myanmar' THEN 'MM'
    WHEN 'Nauru' THEN 'NR'
    WHEN 'Nepal' THEN 'NP'
    WHEN 'New Zealand' THEN 'NZ'
    WHEN 'North Korea' THEN 'KP'
    WHEN 'Palau' THEN 'PW'
    WHEN 'Papua New Guinea' THEN 'PG'
    WHEN 'Philippines' THEN 'PH'
    WHEN 'Samoa' THEN 'WS'
    WHEN 'Singapore' THEN 'SG'
    WHEN 'Solomon Islands' THEN 'SB'
    WHEN 'South Korea' THEN 'KR'
    WHEN 'Sri Lanka' THEN 'LK'
    WHEN 'Taiwan ROC' THEN 'TW'
    WHEN 'Thailand' THEN 'TH'
    WHEN 'Timor-Leste' THEN 'TL'
    WHEN 'Tonga' THEN 'TO'
    WHEN 'Tuvalu' THEN 'TV'
    WHEN 'Vanuatu' THEN 'VU'
    WHEN 'Vietnam' THEN 'VN'
  END AS country_code_iso2,
    CASE gcgh_country_mst_name
    WHEN 'Bangladesh' THEN 'BGD'
    WHEN 'Bhutan' THEN 'BTN'
    WHEN 'Brunei' THEN 'BRN'
    WHEN 'Cambodia' THEN 'KHM'
    WHEN 'China' THEN 'CHN'
    WHEN 'Federated States of Micronesia' THEN 'FSM'
    WHEN 'Fiji' THEN 'FJI'
    WHEN 'Hong Kong SAR' THEN 'HKG'
    WHEN 'India' THEN 'IND'
    WHEN 'Indonesia' THEN 'IDN'
    WHEN 'Japan' THEN 'JPN'
    WHEN 'Kiribati' THEN 'KIR'
    WHEN 'Laos' THEN 'LAO'
    WHEN 'Macau' THEN 'MAC'
    WHEN 'Malaysia' THEN 'MYS'
    WHEN 'Maldives' THEN 'MDV'
    WHEN 'Marshall Islands' THEN 'MHL'
    WHEN 'Myanmar' THEN 'MMR'
    WHEN 'Nauru' THEN 'NRU'
    WHEN 'Nepal' THEN 'NPL'
    WHEN 'New Zealand' THEN 'NZL'
    WHEN 'North Korea' THEN 'PRK'
    WHEN 'Palau' THEN 'PLW'
    WHEN 'Papua New Guinea' THEN 'PNG'
    WHEN 'Philippines' THEN 'PHL'
    WHEN 'Samoa' THEN 'WSM'
    WHEN 'Singapore' THEN 'SGP'
    WHEN 'Solomon Islands' THEN 'SLB'
    WHEN 'South Korea' THEN 'KOR'
    WHEN 'Sri Lanka' THEN 'LKA'
    WHEN 'Taiwan ROC' THEN 'TWN'
    WHEN 'Thailand' THEN 'THA'
    WHEN 'Timor-Leste' THEN 'TLS'
    WHEN 'Tonga' THEN 'TON'
    WHEN 'Tuvalu' THEN 'TUV'
    WHEN 'Vanuatu' THEN 'VUT'
    WHEN 'Vietnam' THEN 'VNM'
  END AS country_code_iso3 ,
  null as market_type ,
  null as cdl_datetime ,
  null as cdl_source_file ,
  null as load_key 
    from source
    where region ilike 'apac'
    and _deleted_='F'
)

--Final select
select * from final