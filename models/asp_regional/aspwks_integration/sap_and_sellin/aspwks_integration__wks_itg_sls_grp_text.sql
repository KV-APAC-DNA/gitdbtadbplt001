--import CTE

with sources as(
    SELECT *
    FROM {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_sales_group_text') }}
),
--logical CTE
final as(
    select 
        mandt,
        spras,
        vkgrp,
        bezei,
        --crt_dttm,
        updt_dttm
     from sources
)
--final select
select * from final