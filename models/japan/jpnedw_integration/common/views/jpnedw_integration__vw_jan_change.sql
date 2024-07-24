with edi_item_m as(
    select * from {{ ref('jpnedw_integration__edi_item_m') }}
),
transformed as(
    SELECT edi_item_m.jan_cd_so AS jan_cd
        ,min((edi_item_m.item_cd)::TEXT) AS item_cd
    FROM edi_item_m
    WHERE (edi_item_m.jan_cd_so IS NOT NULL)
    GROUP BY edi_item_m.jan_cd_so
)
select * from transformed