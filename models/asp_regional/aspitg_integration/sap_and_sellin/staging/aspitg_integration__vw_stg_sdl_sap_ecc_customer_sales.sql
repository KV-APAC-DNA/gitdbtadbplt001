

--import CTE
with sources as(
    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_customer_sales') }}
),

--logical CTE
final as(
    select * from sources
)
--final select
select * from final

