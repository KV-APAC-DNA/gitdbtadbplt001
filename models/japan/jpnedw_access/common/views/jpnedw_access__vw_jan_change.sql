with source as(
    select * from {{ ref('jpnedw_integration__vw_jan_change') }}
),
transformed as(
    SELECT 
    jan_cd as "jan_cd",
    item_cd as "item_cd"
    FROM source
)
select * from transformed