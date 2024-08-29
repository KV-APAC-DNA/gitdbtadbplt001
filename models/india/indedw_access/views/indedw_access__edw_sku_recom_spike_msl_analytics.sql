with source as 
(
    select * from {{ source('indedw_integration', 'edw_sku_recom_spike_msl_analytics') }}
)
select
    mth_mm as "mth_mm",
    count_of_retailers as "count_of_retailers",
    recos as "recos",
    hits as "hits",
    no_of_orange_stores as "no_of_orange_stores",
    orange_store_perc as "orange_store_perc",
    store_tag as "store_tag"
from source
