{{ config(
  materialized='view',
  secure='true'
) }}

select
    Channel::VARCHAR(500) as Channel,
    Description::VARCHAR(500) as Description,
    Week::NUMBER(38, 0) as Week,
    "Month Desc"::VARCHAR(100) as "Month Desc",
    "Week Day"::VARCHAR(100) as "Week Day",
    "Week Day Group"::VARCHAR(100) as "Week Day Group",
    "Master Programme"::VARCHAR(1000) as "Master Programme",
    "Commercial Programme Genre"::VARCHAR(1000) as "Commercial Programme Genre",
    "Commercial Programme Theme"::VARCHAR(1000) as "Commercial Programme Theme",
    "Advertiser Group"::VARCHAR(1000) as "Advertiser Group",
    Advertiser::VARCHAR(500) as Advertiser,
    Brand::VARCHAR(500) as Brand,
    Sector::VARCHAR(500) as Sector,
    Category::VARCHAR(1000) as Category,
    Position::VARCHAR(1000) as Position,
    "Channel Language"::VARCHAR(1000) as "Channel Language",
    "Spot language"::VARCHAR(1000) as "Spot language",
    File_name::VARCHAR(500) as File_name,
    "Market Cluster & Target Group"::VARCHAR(1000)
        as "Market Cluster & Target Group",
    "GRP Competes"::FLOAT as "GRP Competes",
    "Report Cadence Type"::VARCHAR(50) as "Report Cadence Type",
    Channel_type::VARCHAR(50) as Channel_type,
    Country::VARCHAR(50) as Country,
    "Type of Data"::VARCHAR(50) as "Type of Data",
    Gcph_brand::VARCHAR(16777216) as Gcph_brand,
    Gcgh_country::VARCHAR(1000) as Gcgh_country,
    Brand_harmonized_by::VARCHAR(16777216) as Brand_harmonized_by,
    Load_date::TIMESTAMP_NTZ(9) as Load_date,
    "Start Time"::TIME as "Start Time",
    Year::INT as Year,
    Month::INT as Month,
    Nins::INT as Nins,
    "Length (sec)"::INT as "Length (sec)",
    No::INT as No,
    "To"::INT as "To",
    Date,
    "Norm_GRP Competes"::FLOAT as "Norm_GRP Competes",
    "HOUR",
    "PT/NPT"::VARCHAR(16777216) as "PT/NPT"
from {{ ref('inditg_integration__fct_tv_grp_spends_global_monthly') }}