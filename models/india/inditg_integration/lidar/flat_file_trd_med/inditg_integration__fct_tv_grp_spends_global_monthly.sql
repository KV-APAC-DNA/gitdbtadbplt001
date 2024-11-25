{{ config(
   materialized='incremental',
  incremental_strategy='append',
  transient=false,
  post_hook="{{ get_harmonized_brand('TRADITIONAL_COMPETITIVE_TV_GRP_SPENDS', 'Y') }}"
) }}

select
    "Channel"::VARCHAR(500) as Channel,
    "Description"::VARCHAR(500) as Description,
    "Week"::NUMBER(38, 0) as Week,
    "Month Desc"::VARCHAR(100) as "Month Desc",
    "Week Day"::VARCHAR(100) as "Week Day",
    "Week Day Group"::VARCHAR(100) as "Week Day Group",
    "Master Programme"::VARCHAR(1000) as "Master Programme",
    "Commercial Programme Genre"::VARCHAR(1000) as "Commercial Programme Genre",
    "Commercial Programme Theme"::VARCHAR(1000) as "Commercial Programme Theme",
    "Advertiser Group"::VARCHAR(1000) as "Advertiser Group",
    "Advertiser"::VARCHAR(500) as Advertiser,
    "Brand"::VARCHAR(500) as Brand,
    "Sector"::VARCHAR(500) as Sector,
    "Category"::VARCHAR(1000) as Category,
    "Position"::VARCHAR(1000) as Position,
    "Channel Language"::VARCHAR(1000) as "Channel Language",
    "Spot language"::VARCHAR(1000) as "Spot language",
    File_name::VARCHAR(500) as File_name,
    "kpi"::VARCHAR(1000) as "Market Cluster & Target Group",
    "kpi_1"::FLOAT as "GRP Competes",
    'MMM'::VARCHAR(50) as "Report Cadence Type",
    'TV'::VARCHAR(50) as Channel_type,
    'IN'::VARCHAR(50) as Country,
    'Competitive Data'::VARCHAR(50) as "Type of Data",
    Gcph_brand::VARCHAR(16777216) as Gcph_brand,
    'India'::VARCHAR(1000) as Gcgh_country,
    Brand_harmonized_by::VARCHAR(16777216) as Brand_harmonized_by,
    Load_date::TIMESTAMP_NTZ(9) as Load_date,
    "Start Time"::TIME as "Start Time",
    "Year"::INT as Year,
    "Month"::INT as Month,
    "NIns"::INT as Nins,
    "Length (sec)"::INT as "Length (sec)",
    "No"::INT as No,
    "To"::INT as "To",
    TO_DATE("Date", 'DD/MM/YYYY') as Date,
    ("kpi_1" * "Length (sec)") / 30::FLOAT as "Norm_GRP Competes",
    DATE_PART(hour, '7:23:08'::TIME) as "HOUR",
    case
        when
            "Week Day" in (
                'Monday', 'Wednesday', 'Tuesday', 'Friday', 'Thursday'
            )
            and DATE_PART(hour, "Start Time"::TIME) >= 18
            and DATE_PART(hour, "Start Time"::TIME) < 24
            then 'PT'
        when
            "Week Day" in (
                'Monday', 'Wednesday', 'Tuesday', 'Friday', 'Thursday'
            )
            and DATE_PART(hour, "Start Time"::TIME) >= 7
            and DATE_PART(hour, "Start Time"::TIME) < 18
            then 'NPT'
        when
            "Week Day" in ('Saturday', 'Sunday')
            and DATE_PART(hour, "Start Time"::TIME) >= 7
            and DATE_PART(hour, "Start Time"::TIME) < 24
            then 'PT'
        when
            "Week Day" in (
                'Monday',
                'Saturday',
                'Wednesday',
                'Tuesday',
                'Friday',
                'Sunday',
                'Thursday'
            )
            and DATE_PART(hour, "Start Time"::TIME) = 24
            then 'NPT'
        when
            "Week Day" in (
                'Monday',
                'Saturday',
                'Wednesday',
                'Tuesday',
                'Friday',
                'Sunday',
                'Thursday'
            )
            and DATE_PART(hour, "Start Time"::TIME) < 7
            then 'NPT'
    end as "PT/NPT"
from {{ source(
      "indsdl_raw",
      "sdl_lidar_ff_tv_grp_spends"
    ) }}
{% if is_incremental() %}
    where Load_date > (select MAX(Load_date) from {{ this }})
{% endif %}
