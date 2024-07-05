

WITH item_jizen_bunkai_w06 AS 
(
    SELECT *
    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
),

transformed AS 
(
    SELECT
        t.itemcode,
        sum(t.koseritsu) AS koseritsukei,
        1 - sum(t.koseritsu) AS sa
    FROM item_jizen_bunkai_w06 AS t
    WHERE t.koseritsu <> 0
    GROUP BY t.itemcode
    HAVING sum(t.koseritsu) <> 1
),

final AS 
(
    SELECT
        itemcode::VARCHAR(20) AS itemcode,
        koseritsukei::DECIMAL(16, 8) AS koseritsukei,
        sa::DECIMAL(16, 8) AS sa
    FROM transformed
)

SELECT *
FROM final
