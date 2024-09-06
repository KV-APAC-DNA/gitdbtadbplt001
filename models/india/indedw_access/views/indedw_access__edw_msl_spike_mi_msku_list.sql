with source as 
(
    select * from {{ source('indedw_integration', 'edw_msl_spike_mi_msku_list') }}
)
select
    region_name as "region_name",
    zone_name as "zone_name",
    total_subd as "total_subd",
    mothersku_name as "mothersku_name",
    period as "period"
from source
