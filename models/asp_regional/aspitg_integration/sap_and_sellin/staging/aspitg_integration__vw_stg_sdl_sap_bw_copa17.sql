

--import CTE
with sources as(
    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_copa17') }}
),
--logical CTE
final as(
    select * from sources
)
--final select
select * from final
