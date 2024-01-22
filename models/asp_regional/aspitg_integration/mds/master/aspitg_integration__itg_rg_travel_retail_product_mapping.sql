--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_apac_dcl_products') }}
),

--Final CTE
final as (
    select
        upper(trim(country_code)) :: varchar(5) as ctry_cd,
        trim(country_name) ::varchar(30) as country_name,
        trim("dcl code") :: varchar(30) as dcl_code,
        trim("sap code") :: varchar(30) as sap_code,
        trim("sales channel_name") ::varchar(50) as sales_channel,
        coalesce(cast(trim("srp usd") as decimal(21, 5)), 0) as srp_usd,
        trim(enterdatetime) :: timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

--Final select
select * from final
