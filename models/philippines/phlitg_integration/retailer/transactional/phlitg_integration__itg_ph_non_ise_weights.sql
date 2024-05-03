with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_non_ise_weights') }}
),
final as
(
select
    'PH'::varchar(20) as ctry_cd,
    cast(trim(weight) as numeric(38, 5)) weight,
    upper(trim(kpi))::varchar(100) as kpi,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    current_timestamp::timestamp_ntz(9) as crt_dttm
from source
)
select * from final