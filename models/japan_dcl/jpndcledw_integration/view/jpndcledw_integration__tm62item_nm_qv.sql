with tm62item_n
as
(
select * from jpdcledw_integration.tm62item_nm
),
tm62item_nm_add_qv
as
(select * from jpdcledw_integration.tm62item_nm_add_qv 
)
final as
(
    SELECT tm62item_nm.code,
        tm62item_nm.name
    FROM tm62item_nm

    UNION ALL

    SELECT tm62item_nm_add_qv.code,
        tm62item_nm_add_qv.name
    FROM tm62item_nm_add_qv
)
select * from final