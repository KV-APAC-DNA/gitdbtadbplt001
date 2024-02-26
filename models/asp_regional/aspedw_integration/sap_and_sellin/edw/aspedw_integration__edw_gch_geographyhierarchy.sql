
--Import CTE
with source as (
   select * from {{ ref('aspitg_integration__vw_stg_sdl_gcgh_geo_hier') }}
    
),

--Logical CTE

--Final CTE
final as (
    select
        region::varchar(50) as "region",
        cluster::varchar(30) as "cluster",
        subcluster::varchar(30) as subcluster,
        market::varchar(30) as market,
        country_code_iso2::varchar(10) as country_code_iso2,
        country_code_iso3::varchar(10) as country_code_iso3,
        market_type::varchar(30) as market_type,
        cdl_datetime::varchar(30) as cdl_datetime,
        cdl_source_file::varchar(50) as cdl_source_file,
        load_key::varchar(100) as load_key,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

--Final select
select * from final 