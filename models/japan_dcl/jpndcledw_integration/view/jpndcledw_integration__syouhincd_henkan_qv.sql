with item_z_h_hen_tbl
as
(
    select * from jpdcledw_integration.item_z_h_hen_tbl
),
final as
(
    SELECT item_z_h_hen_tbl.h_itemcode::VARCHAR(45) AS itemcode,
        item_z_h_hen_tbl.z_itemcode::VARCHAR(45) AS koseiocode
    FROM item_z_h_hen_tbl
    WHERE (
            (item_z_h_hen_tbl.marker >= 1)
            AND (item_z_h_hen_tbl.marker <= 4)
            )
)
select * from final