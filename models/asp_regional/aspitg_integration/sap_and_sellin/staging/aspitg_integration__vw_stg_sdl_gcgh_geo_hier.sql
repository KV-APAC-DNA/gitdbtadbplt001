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
    gcgh_country_mst_name::varchar(30) as market 
    -- country_code_iso2::varchar(10) as country_code_iso2 ,
    -- country_code_iso3::varchar(10) as country_code_iso3 ,
    -- market_type::varchar(30) as market_type ,
    -- cdl_datetime::varchar(30) as cdl_datetime ,
    -- cdl_source_file::varchar(50) as cdl_source_file ,
    -- load_key::varchar(100) as load_key 
    from source
)

--Final select
select * from final