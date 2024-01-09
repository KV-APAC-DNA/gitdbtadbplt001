
with source as (
    select * from {{ ref('aspwks_integration__wks_edw_list_price') }}
)


select * from source
