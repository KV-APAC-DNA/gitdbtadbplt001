with edw_gch_geographyhierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_geographyhierarchy') }}
),
final as (
    Select "region" as "region",
    "cluster" as "cluster",
    subcluster as "subcluster",
    market as "market",
    country_code_iso2 as "country_code_iso2",
    country_code_iso3 as "country_code_iso3",
    market_type as "market_type",
    cdl_datetime as "cdl_datetime",
    cdl_source_file as "cdl_source_file",
    load_key as "load_key",
    crt_dttm as "crt_dttm",
    updt_dttm as "updt_dttm"
    From edw_gch_geographyhierarchy
)

select * from final