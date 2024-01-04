{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table",
        transient= false
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_edw_gch_geographyhierarchy') }}
),

--Logical CTE

--Final CTE
final as (
    select
        region,
        cluster,
        subcluster,
        market,
        country_code_iso2,
        country_code_iso3,
        market_type,
        cdl_datetime,
        cdl_source_file,
        load_key,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final 