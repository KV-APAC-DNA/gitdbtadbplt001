

--import CTE
with source as(
    select * from {{ source('bwa_access', 'bwa_tsales_grp') }}
),
--logical CTE
final as(
    select 
        '888' as mandt,
        langu as spras,
        sales_grp as vkgrp,
        txtsh as bezei,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source
    where langu = 'E' and _deleted_= 'F'
)
--final select
select * from final
